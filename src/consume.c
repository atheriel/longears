#include <stdlib.h> /* for malloc */
#include <string.h> /* for memcpy */
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

static void R_finalize_consumer(SEXP ptr)
{
  struct consumer *con = (struct consumer *) R_ExternalPtrAddr(ptr);
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
    R_ReleaseObject(con->fun);
    R_ReleaseObject(con->rho);
    free(con);
    con = NULL;
  }
  R_ClearExternalPtr(ptr);
}

SEXP R_amqp_create_consumer(SEXP ptr, SEXP queue, SEXP tag, SEXP fun, SEXP rho,
                            SEXP no_ack, SEXP exclusive, SEXP args)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
  struct consumer *con = malloc(sizeof(struct consumer));
  con->conn = conn;
  con->chan.chan = 0;
  con->chan.is_open = 0;
  con->tag = amqp_empty_bytes;
  con->fun = fun;
  con->rho = rho;
  con->prev = NULL;
  con->next = NULL;

  char errbuff[200];
  if (ensure_valid_channel(con->conn, &con->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  const char *tag_str = CHAR(asChar(tag));
  int has_no_ack = asLogical(no_ack);
  int is_exclusive = asLogical(exclusive);
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  con->no_ack = has_no_ack;

  amqp_basic_consume_ok_t *consume_ok;
  consume_ok = amqp_basic_consume(conn->conn, con->chan.chan,
                                  amqp_cstring_bytes(queue_str),
                                  amqp_cstring_bytes(tag_str),
                                  0, has_no_ack, is_exclusive,
                                  *arg_table);

  if (consume_ok == NULL) {
    free(con);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
    Rf_error("Failed to start a queue consumer. %s", errbuff);
  }

  if (!has_no_ack) {
    amqp_basic_qos_ok_t *qos_ok = amqp_basic_qos(conn->conn, con->chan.chan,
                                                 0, DEFAULT_PREFETCH_COUNT, 0);
    if (qos_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
      render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
      Rf_error("Failed to set quality of service. %s", errbuff);
    }
  }

  con->tag = amqp_bytes_malloc_dup(consume_ok->consumer_tag);

  SEXP out = PROTECT(R_MakeExternalPtr(con, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(out, R_finalize_consumer, 1);
  setAttrib(out, R_ClassSymbol, mkString("amqp_consumer"));

  /* Add it to the global list of consumers. */
  if (!conn->consumers) {
    conn->consumers = con;
  } else {
    struct consumer *elt = conn->consumers;
    while (elt->next) {
      elt = elt->next;
    }
    elt->next = con;
    con->prev = elt;
  }

  R_PreserveObject(fun);
  R_PreserveObject(rho);

  UNPROTECT(1);
  return out;
}

SEXP R_amqp_listen(SEXP ptr, SEXP timeout)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
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

  SEXP message, body;
  SEXP R_fcall = PROTECT(allocList(2));
  SET_TYPEOF(R_fcall, LANGSXP);

  int ack;
  amqp_rpc_reply_t reply;
  amqp_envelope_t env;
  struct consumer *elt;

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
        current_wait++;
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

      /* Copy body. */
      size_t body_len = env.message.body.len;
      body = PROTECT(Rf_allocVector(RAWSXP, body_len));
      memcpy((void *) RAW(body), env.message.body.bytes, body_len);

      message = PROTECT(R_message_object(body, env.delivery_tag, env.redelivered,
                                         env.exchange, env.routing_key, -1,
                                         env.consumer_tag,
                                         &env.message.properties));
      amqp_destroy_envelope(&env);

      /* Acknowledge the message before we run the callback, just in case it
       * fails. */

      if (!elt->no_ack) {
        ack = amqp_basic_ack(conn->conn, elt->chan.chan, env.delivery_tag, 0);
        if (ack != AMQP_STATUS_OK) {
          Rf_warning("Failed to acknowledge message. %s", amqp_error_string2(ack));
        }
      }

      SETCAR(R_fcall, elt->fun);
      SETCADR(R_fcall, message);
      Rf_eval(R_fcall, elt->rho);

      UNPROTECT(2);
    }

    R_CheckUserInterrupt(); // Escape hatch.
  }

  UNPROTECT(1);
  return R_NilValue;
}

SEXP R_amqp_destroy_consumer(SEXP ptr)
{
  struct consumer *con = (struct consumer *) R_ExternalPtrAddr(ptr);
  if (!con) {
    Rf_error("Invalid consumer object.");
  }
  R_finalize_consumer(ptr);

  return R_NilValue;
}
