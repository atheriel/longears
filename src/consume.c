#include <stdlib.h> /* for malloc */
#include <string.h> /* for memcpy */
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

typedef struct {
  connection *conn;
  channel chan;
  amqp_bytes_t tag;
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

  while (!finished) {

    amqp_maybe_release_buffers(conn->conn);
    reply = amqp_consume_message(conn->conn, &env, &tv, 0);

    /* The envelope contains a message. */

    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
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
  if (!con->conn->is_connected || !con->chan.is_open) {
    Rf_error("Consumer's channel or connection has already been closed.");
  }

  char errbuff[200];
  amqp_rpc_reply_t reply;
  amqp_basic_cancel_ok_t *cancel_ok;

  cancel_ok = amqp_basic_cancel(con->conn->conn, con->chan.chan,
                                con->tag);

  if (cancel_ok == NULL) {
    reply = amqp_get_rpc_reply(con->conn->conn);
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
    Rf_error("Failed to cancel the consumer. %s", errbuff);
  }

  con->chan.is_open = 0;
  reply = amqp_channel_close(con->conn->conn, con->chan.chan,
                             AMQP_REPLY_SUCCESS);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 200);
    Rf_error("Failed to close the consumer's channel. %s", errbuff);
  }

  return R_NilValue;
}
