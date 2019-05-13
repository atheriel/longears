#include <stdlib.h> /* for malloc */
#include <string.h> /* for memcpy */
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

SEXP R_amqp_create_consumer(SEXP ptr, SEXP queue, SEXP tag, SEXP no_local,
                            SEXP no_ack, SEXP exclusive)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  const char *tag_str = CHAR(asChar(tag));
  int has_no_local = asLogical(no_local);
  int has_no_ack = asLogical(no_ack);
  int is_exclusive = asLogical(exclusive);

  /* TODO: Use a separate channel for the consumer. */

  amqp_basic_consume_ok_t *consume_ok;
  consume_ok = amqp_basic_consume(conn->conn, conn->chan.chan,
                                  amqp_cstring_bytes(queue_str),
                                  amqp_cstring_bytes(tag_str),
                                  has_no_local, has_no_ack, is_exclusive,
                                  amqp_empty_table);

  if (consume_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: consume response is NULL with a normal reply.");
    } else {
      render_amqp_error(reply, conn, errbuff, 200);
      Rf_error("Failed to start a queue consumer. %s", errbuff);
    }
    return R_NilValue;
  }

  return ScalarString(mkCharLen(consume_ok->consumer_tag.bytes,
                                consume_ok->consumer_tag.len));
}

SEXP R_amqp_listen(SEXP ptr, SEXP fun, SEXP rho, SEXP timeout)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  /* int timeout_ = asInteger(timeout); */
  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;

  SEXP message, body;
  SEXP R_fcall = PROTECT(allocList(2));
  SET_TYPEOF(R_fcall, LANGSXP);
  SETCAR(R_fcall, fun);

  amqp_rpc_reply_t reply;
  amqp_envelope_t env;

  for (;;) {

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
      Rf_eval(R_fcall, rho);

      UNPROTECT(2);
    }

    R_CheckUserInterrupt(); // Escape hatch.
  }

  return R_NilValue;
}

SEXP R_amqp_destroy_consumer(SEXP ptr, SEXP tag)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *tag_str = CHAR(asChar(tag));

  /* TODO: Use a separate channel for the consumer. */

  amqp_basic_cancel_ok_t *cancel_ok;
  cancel_ok = amqp_basic_cancel(conn->conn, conn->chan.chan,
                                amqp_cstring_bytes(tag_str));

  if (cancel_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: cancel response is NULL with a normal reply.");
    } else {
      render_amqp_error(reply, conn, errbuff, 200);
      Rf_error("Failed to cancel the consumer. %s", errbuff);
    }
    return R_NilValue;
  }

  return ScalarString(mkCharLen(cancel_ok->consumer_tag.bytes,
                                cancel_ok->consumer_tag.len));
}
