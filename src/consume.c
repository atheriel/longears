#include <stdlib.h> /* for malloc */
#include <string.h> /* for memcpy */
#include <sys/time.h>
#include <time.h> /* for time() */

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "altrep.h"
#include "connection.h"
#include "utils.h"

static void R_finalize_consumer(SEXP ptr)
{
  consumer *con = (consumer *) R_ExternalPtrAddr(ptr);
  if (con) {
    /* Attempt to cancel the consumer and close the channel. */
    if (con->chan.is_open) {
      amqp_basic_cancel(con->conn->conn, con->chan.chan, con->tag);
      amqp_channel_close(con->conn->conn, con->chan.chan, AMQP_REPLY_SUCCESS);
    }
    /* Remove it from the global list of consumers. */
    if (con->next) {
      con->next->prev = con->prev;
    }
    if (con->prev) {
      con->prev->next = con->next;
    } else if (con->conn->consumers == con) {
      con->conn->consumers = con->next;
    }
    amqp_bytes_free(con->tag);
    R_ReleaseObject(con->fcall);
    R_ReleaseObject(con->rho);
    R_ClearExternalPtr(ptr);
    free(con);
    con = NULL;
  }
  R_ClearExternalPtr(ptr);
}

SEXP R_amqp_create_consumer(SEXP ptr, SEXP queue, SEXP tag, SEXP fun, SEXP rho,
                            SEXP no_ack, SEXP exclusive, SEXP prefetch_count_, SEXP args)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  consumer *con = malloc(sizeof(consumer));
  con->conn = conn;
  con->chan.chan = 0;
  con->chan.is_open = 0;
  con->tag = amqp_empty_bytes;
  con->rho = rho;
  con->prev = NULL;
  con->next = NULL;

  char errbuff[200];
  if (ensure_valid_channel(con->conn, &con->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  amqp_bytes_t queue_str = charsxp_to_amqp_bytes(Rf_asChar(queue));
  amqp_bytes_t tag_str = charsxp_to_amqp_bytes(Rf_asChar(tag));
  int has_no_ack = asLogical(no_ack);
  int is_exclusive = asLogical(exclusive);
  int prefetch_count = asInteger(prefetch_count_);
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  /* Note: QoS needs to happen on the channel *before* we start the consumer to
   * take effect.
   *
   * See: https://github.com/rabbitmq/rabbitmq-management/issues/311 and
   *      https://www.rabbitmq.com/consumer-prefetch.html
   */
  amqp_basic_qos_ok_t *qos_ok = amqp_basic_qos(conn->conn, con->chan.chan, 0,
                                               prefetch_count, 0);
  if (qos_ok == NULL) {
    free(con);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
    Rf_error("Failed to set quality of service. %s", errbuff);
  }

  amqp_basic_consume_ok_t *consume_ok;
  consume_ok = amqp_basic_consume(conn->conn, con->chan.chan, queue_str, tag_str,
                                  0, has_no_ack, is_exclusive, *arg_table);

  if (consume_ok == NULL) {
    free(con);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
    Rf_error("Failed to start a queue consumer. %s", errbuff);
  }

  con->tag = amqp_bytes_malloc_dup(consume_ok->consumer_tag);

  /* Set up the callback so we don't need to construct it later. */
  con->fcall = has_no_ack ? Rf_lang2(fun, R_NilValue) :
    Rf_lang3(fun, R_NilValue, R_NilValue);
  R_PreserveObject(con->fcall);

  if (!has_no_ack) {
    /* The channel is passed to the callback wrapper so that it can be used
     * for acknowledgements. TODO: Does this need a finalizer? */
    SEXP chan_ptr = PROTECT(R_MakeExternalPtr(&con->chan, R_NilValue, R_NilValue));
    SETCADDR(con->fcall, chan_ptr);
    UNPROTECT(1);
  }

  SEXP out = PROTECT(R_MakeExternalPtr(con, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(out, R_finalize_consumer, 1);
  setAttrib(out, R_ClassSymbol, mkString("amqp_consumer"));

  /* Add it to the global list of consumers. */
  if (!conn->consumers) {
    conn->consumers = con;
  } else {
    consumer *elt = conn->consumers;
    while (elt->next) {
      elt = elt->next;
    }
    elt->next = con;
    con->prev = elt;
  }

  R_PreserveObject(rho);

  UNPROTECT(1);
  return out;
}

struct msg_ctx {
  amqp_envelope_t *env;
  consumer *con;
  SEXP msg;
};

static SEXP eval_consumer_callback(void * data)
{
  struct msg_ctx *ctx = (struct msg_ctx *) data;
#ifdef ENABLE_ALTREP
  SEXP body = PROTECT(new_pooled_bytes_sexp(&ctx->env->message.body));
#else
  SEXP body = PROTECT(Rf_allocVector(RAWSXP, ctx->env->message.body.len));
  memcpy((void *) RAW(body), ctx->env->message.body.bytes,
         ctx->env->message.body.len);
#endif

  ctx->msg = PROTECT(R_message_object(body, ctx->env->delivery_tag,
                                      ctx->env->redelivered, ctx->env->exchange,
                                      ctx->env->routing_key, -1,
                                      ctx->env->consumer_tag,
                                      &ctx->env->message.properties));
  SETCADR(ctx->con->fcall, ctx->msg);
  Rf_eval(ctx->con->fcall, ctx->con->rho);
  SETCADR(ctx->con->fcall, R_NilValue);
  UNPROTECT(2);
  return R_NilValue;
}

static void eval_cleanup(void *data, Rboolean jump)
{
  struct msg_ctx *ctx = (struct msg_ctx *) data;

#ifdef ENABLE_ALTREP
  /* If the callback failed (as opposed to body/message creation failing), we
   * should try to release the memory. FIXME: What happens if this throws an
   * error? */
  if (ctx->msg != R_NilValue) {
    SEXP body = VECTOR_ELT(ctx->msg, 0);

    /* FIXME: Currently this condition will always be true, even on R 4.0 with
     * true reference counting.
     *
     * Using REFCNT() seems to indicate that ctx->msg is referenced after
     * evaluation *even when the callback does not create any references*; in
     * addition, wrapped callbacks (when no_ack = FALSE) seem to add an
     * additional increment.
     *
     * I'm not sure why this is the case -- perhaps the enclosing frame
     * environment or some related promises are storing a reference here we
     * can't eliminate. */
    if (MAYBE_REFERENCED(ctx->msg) || MAYBE_SHARED(body)) {
      materialize_pooled_bytes(body);
    }

    release_pooled_bytes(body);
  }
#endif

  amqp_destroy_envelope(ctx->env);
}

SEXP R_amqp_listen(SEXP ptr, SEXP timeout)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to consume messages. %s", errbuff);
    return R_NilValue;
  }

  if (!conn->consumers) {
    Rf_error("No consumers are declared on this connection.");
  }

  int current_wait = 0, max_wait = asInteger(timeout);
  max_wait = max_wait > 60 ? 60 : max_wait;
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;
  time_t start = time(NULL);

  SEXP message, body, R_fcall;

  amqp_rpc_reply_t reply;
  amqp_envelope_t env;
  consumer *elt;
  struct msg_ctx ctx;
  ctx.env = NULL;
  ctx.con = NULL;
  ctx.msg = R_NilValue;

  while (current_wait < max_wait) {

    amqp_maybe_release_buffers(conn->conn);
    reply = amqp_consume_message(conn->conn, &env, &tv, 0);

    /* Accumulate timeouts one second at a time until we hit the max. This is to
     * make the loop more responsive and give the user the ability to interrupt
     * the function early. */

    if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
      int status = reply.library_error;

      /* If we get into an unexpected state, try to decode a relevant method
         (e.g. connection.close). */
      if (status == AMQP_STATUS_UNEXPECTED_STATE) {
        amqp_frame_t frame;
        status = amqp_simple_wait_frame(conn->conn, &frame);
        /* If the server shuts down gracefully, this is how we will probably be
           notified. */
        if (status == AMQP_STATUS_OK && frame.frame_type == AMQP_FRAME_METHOD &&
            frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD) {
          status = AMQP_STATUS_CONNECTION_CLOSED;
        } else if (status == AMQP_STATUS_OK &&
                   frame.frame_type == AMQP_FRAME_METHOD &&
                   frame.payload.method.id == AMQP_BASIC_CANCEL_METHOD) {
          /* If we have consumer_cancel_notify enabled, this is how we are
             notified that e.g. deleted queues have cancelled a consumer. */
          amqp_basic_cancel_t *cancel;
          cancel = (amqp_basic_cancel_t *) frame.payload.method.decoded;
          char tag[128];
          strncpy(tag, (const char *) cancel->consumer_tag.bytes,
                  cancel->consumer_tag.len);
          tag[cancel->consumer_tag.len] = '\0';
          Rf_error("Consumer '%s' cancelled by the broker.", tag);
        } else if (status == AMQP_STATUS_OK) {
          status = AMQP_STATUS_UNEXPECTED_STATE;
        } else {
          /* Act on whatever status amqp_simple_wait_frame() gave us. */
        }
      }

      switch (status) {
      case AMQP_STATUS_TIMEOUT:
        /* OK. */
        break;
      case AMQP_STATUS_CONNECTION_CLOSED:
        /* fallthrough */
      case AMQP_STATUS_SOCKET_CLOSED:
        /* fallthrough */
      case AMQP_STATUS_SOCKET_ERROR:
        /* fallthrough */
        conn->is_connected = 0;
        Rf_error("Disconnected from server.");
        break;
      default:
        Rf_error("Encountered unexpected library error: %s\n",
                 amqp_error_string2(status));
        break;
      }
    }

    /* The envelope contains a message. */

    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      /* Find the right consumer. */
      elt = conn->consumers;
      while (elt && strncmp(elt->tag.bytes, env.consumer_tag.bytes,
                            elt->tag.len) != 0) {
        elt = elt->next;
      }
      if (!elt) {
        /* Quietly swallow messages sent to now-cancelled consumers. */
        amqp_destroy_envelope(&env);
        continue;
      }

      ctx.env = &env;
      ctx.con = elt;
      /* We need to execute the callback in an unwind context so that the
       * envelope's resources are always cleaned up after the fact. */
      R_UnwindProtect(eval_consumer_callback, &ctx, eval_cleanup, &ctx,
                      NULL);
    }

    current_wait = time(NULL) - start;
    R_CheckUserInterrupt(); // Escape hatch.
  }

  return R_NilValue;
}

SEXP R_amqp_destroy_consumer(SEXP ptr)
{
  consumer *con = (consumer *) R_ExternalPtrAddr(ptr);
  if (!con) {
    Rf_error("Invalid consumer object.");
  }
  R_finalize_consumer(ptr);

  return R_NilValue;
}
