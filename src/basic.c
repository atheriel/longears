#include <stdlib.h> /* for calloc, free */
#include <string.h> /* for strncpy */

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

SEXP R_amqp_publish(SEXP ptr, SEXP body, SEXP exchange, SEXP routing_key,
                    SEXP mandatory, SEXP immediate, SEXP props)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *body_str = CHAR(asChar(body));
  const char *exchange_str = CHAR(asChar(exchange));
  const char *routing_key_str = CHAR(asChar(routing_key));
  int is_mandatory = asLogical(mandatory);
  int is_immediate = asLogical(immediate);
  amqp_basic_properties_t *props_ = NULL;
  if (TYPEOF(props) != 0) {
    props_ = R_ExternalPtrAddr(props);
  }

  /* Send message. */

  int result = amqp_basic_publish(conn->conn, conn->chan.chan,
                                  amqp_cstring_bytes(exchange_str),
                                  amqp_cstring_bytes(routing_key_str),
                                  is_mandatory, is_immediate, props_,
                                  amqp_cstring_bytes(body_str));

  if (result != AMQP_STATUS_OK) {
    Rf_error("Failed to publish message. Error: %s.",
             amqp_error_string2(result));
    return R_NilValue;
  }

  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, conn, errbuff, 200);
    Rf_error("Failed to publish message. %s", errbuff);
  }

  return R_NilValue;
}

SEXP R_amqp_get(SEXP ptr, SEXP queue, SEXP no_ack)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  int has_no_ack = asLogical(no_ack);

  /* Get message. */

  amqp_rpc_reply_t reply = amqp_basic_get(conn->conn, conn->chan.chan,
                                          amqp_cstring_bytes(queue_str),
                                          has_no_ack);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, conn, errbuff, 200);
    Rf_error("Failed to get message. %s", errbuff);
    return R_NilValue;
  } else if (reply.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
    return allocVector(STRSXP, 0); // Equivalent to character(0).
  }

  /* Read basic_get fields before they are reclaimed. */
  amqp_basic_get_ok_t *ok = (amqp_basic_get_ok_t *) reply.reply.decoded;
  int delivery_tag = ok->delivery_tag;
  int redelivered = ok->redelivered;
  amqp_bytes_t exchange = amqp_bytes_malloc_dup(ok->exchange);
  amqp_bytes_t routing_key = amqp_bytes_malloc_dup(ok->routing_key);
  int message_count = ok->message_count;

  amqp_message_t message;
  reply = amqp_read_message(conn->conn, conn->chan.chan, &message, 0);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, conn, errbuff, 200);
    amqp_destroy_message(&message);
    Rf_error("Failed to read message. %s", errbuff);
  }

  // It's possible the message body is not a valid string -- e.g. it's gzipped
  // or base64 encoded. So we return a raw vector.

  SEXP body = PROTECT(Rf_allocVector(RAWSXP, message.body.len));
  memcpy((void *) RAW(body), message.body.bytes, message.body.len);

  SEXP out = PROTECT(R_message_object(body, delivery_tag, redelivered, exchange,
                                      routing_key, message_count,
                                      amqp_empty_bytes, &message.properties));

  if (!has_no_ack) {
    int ack = amqp_basic_ack(conn->conn, conn->chan.chan, delivery_tag, 0);
    if (ack != AMQP_STATUS_OK) {
      Rf_warning("Failed to acknowledge message. %s", amqp_error_string2(ack));
    }
  }

  amqp_destroy_message(&message);
  amqp_bytes_free(exchange);
  amqp_bytes_free(routing_key);
  amqp_maybe_release_buffers_on_channel(conn->conn, conn->chan.chan);
  UNPROTECT(2);
  return out;
}

SEXP R_amqp_ack(SEXP ptr, SEXP delivery_tag, SEXP multiple)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  int delivery_tag_ = asInteger(delivery_tag);
  int multiple_ = asLogical(multiple);

  int result = amqp_basic_ack(conn->conn, conn->chan.chan, delivery_tag_,
                              multiple_);
  if (result != AMQP_STATUS_OK) {
    Rf_error("Failed to acknowledge message(s). %s", amqp_error_string2(result));
  }

  return R_NilValue;
}

SEXP R_amqp_nack(SEXP ptr, SEXP delivery_tag, SEXP multiple, SEXP requeue)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  int delivery_tag_ = asInteger(delivery_tag);
  int multiple_ = asLogical(multiple);
  int requeue_ = asLogical(requeue);

  int result = amqp_basic_nack(conn->conn, conn->chan.chan, delivery_tag_,
                               multiple_, requeue_);
  if (result != AMQP_STATUS_OK) {
    Rf_error("Failed to nack message(s). %s", amqp_error_string2(result));
  }

  return R_NilValue;
}
