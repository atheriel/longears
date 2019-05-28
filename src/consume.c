#include <stdlib.h> /* for malloc */
#include <string.h> /* for memcpy */
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

typedef struct consumer_ {
  connection *conn;
  channel chan;
  amqp_bytes_t tag;
  struct consumer_ *prev;
  struct consumer_ *next;
} consumer;

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
    } else if (con->conn->consumers == (void *) con) {
      con->conn->consumers = con->next;
    }
    amqp_bytes_free(con->tag);
    free(con);
    con = NULL;
  }
  R_ClearExternalPtr(ptr);
}

SEXP R_amqp_create_consumer(SEXP ptr, SEXP queue, SEXP tag, SEXP no_ack,
                            SEXP exclusive)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  consumer *con = malloc(sizeof(consumer));
  con->conn = conn;
  con->chan.chan = 0;
  con->chan.is_open = 0;
  con->tag = amqp_empty_bytes;
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

  amqp_basic_consume_ok_t *consume_ok;
  consume_ok = amqp_basic_consume(conn->conn, con->chan.chan,
                                  amqp_cstring_bytes(queue_str),
                                  amqp_cstring_bytes(tag_str),
                                  0, has_no_ack, is_exclusive,
                                  amqp_empty_table);

  if (consume_ok == NULL) {
    free(con);
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
    Rf_error("Failed to start a queue consumer. %s", errbuff);
  }

  con->tag = amqp_bytes_malloc_dup(consume_ok->consumer_tag);

  SEXP out = PROTECT(R_MakeExternalPtr(con, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(out, R_finalize_consumer, 1);
  setAttrib(out, R_ClassSymbol, mkString("amqp_consumer"));

  /* Add it to the global list of consumers. */
  if (!conn->consumers) {
    conn->consumers = (void *) con;
  } else {
    consumer *elt = (consumer *) conn->consumers;
    while (elt->next) {
      elt = elt->next;
    }
    elt->next = con;
    con->prev = elt;
  }

  UNPROTECT(1);
  return out;
}

SEXP R_amqp_listen(SEXP ptr, SEXP fun, SEXP rho, SEXP timeout)
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

  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;

  SEXP message, body, res;
  Rboolean finished = FALSE;
  SEXP R_fcall = PROTECT(allocList(2));
  SET_TYPEOF(R_fcall, LANGSXP);
  SETCAR(R_fcall, fun);

  amqp_rpc_reply_t reply;
  amqp_envelope_t env;
  consumer *elt;

  while (!finished) {

    amqp_maybe_release_buffers(conn->conn);
    reply = amqp_consume_message(conn->conn, &env, &tv, 0);

    /* The envelope contains a message. */

    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      /* Find the right consumer. */
      elt = (consumer *) conn->consumers;
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

      SETCADR(R_fcall, message);
      res = PROTECT(Rf_eval(R_fcall, rho));
      if (!isLogical(res)) {
        Rf_error("'fun' must return TRUE or FALSE");
      }
      finished = asLogical(res);

      UNPROTECT(3);
    }

    R_CheckUserInterrupt(); // Escape hatch.
  }

  UNPROTECT(1);
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
