#include <stdlib.h> /* for calloc, free */
#include <string.h> /* for strncpy */

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

SEXP R_amqp_publish(SEXP ptr, SEXP routing_key, SEXP body, SEXP exchange,
                    SEXP content_type, SEXP mandatory, SEXP immediate)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *routing_key_str = CHAR(asChar(routing_key));
  const char *body_str = CHAR(asChar(body));
  const char *exchange_str = CHAR(asChar(exchange));
  SEXP content_type_str = asChar(content_type);
  int is_mandatory = asLogical(mandatory);
  int is_immediate = asLogical(immediate);

  /* Send message. */

  amqp_basic_properties_t props;
  props._flags = AMQP_BASIC_DELIVERY_MODE_FLAG;
  props.delivery_mode = 1;

  if (content_type_str != NA_STRING) {
    props._flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
    props.content_type = amqp_cstring_bytes(CHAR(content_type_str));
  }

  int result = amqp_basic_publish(conn->conn, conn->chan.chan,
                                  amqp_cstring_bytes(exchange_str),
                                  amqp_cstring_bytes(routing_key_str),
                                  is_mandatory, is_immediate, &props,
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

  // TODO: Add the ability to acknowledge the message.
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

  amqp_frame_t frame;
  int result = amqp_simple_wait_frame(conn->conn, &frame);
  if (result != AMQP_STATUS_OK) {
    Rf_error("Failed to read frame. Error: %d.", result);
    return R_NilValue;
  }
  if (frame.frame_type != AMQP_FRAME_HEADER) {
    Rf_error("Failed to read frame. Unexpected header type: %d.",
             frame.frame_type);
    return R_NilValue;
  }

  size_t body_len, body_remaining;
  body_len = body_remaining = frame.payload.properties.body_size;
  char *body = calloc(1, body_remaining); // NOTE: Assuming this works here.
  while (body_remaining) {
    result = amqp_simple_wait_frame(conn->conn, &frame);
    if (result != AMQP_STATUS_OK) {
      Rf_error("Failed to wait for frame. Error: %d.", result);
      free(body);
      return R_NilValue;
    }
    strncpy(body, (const char *) frame.payload.body_fragment.bytes,
            frame.payload.body_fragment.len);
    body_remaining -= frame.payload.body_fragment.len;
  }

  // TODO: It's possible the message body is not a valid string -- e.g. it's
  // gzipped or base64 encoded. We could return a raw vector instead, and then
  // perhaps use the content-type to guess whether to convert it at the R level.

  SEXP out = PROTECT(ScalarString(mkCharLen(body, body_len)));
  free(body);

  // TODO: Decide if it makes more sense to return a list instead of using
  // attributes for properties.

  amqp_basic_properties_t *props =
    (amqp_basic_properties_t *) frame.payload.properties.decoded;

  if (!props) {
    Rf_warning("Message properties cannot be recovered.\n");
    UNPROTECT(1);
    return out;
  }

  // TODO: Decode other relevant properties.

  if (props->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
    SEXP content_type = PROTECT(mkCharLen(props->content_type.bytes,
                                          props->content_type.len));
    setAttrib(out, install("content_type"), ScalarString(content_type));
    UNPROTECT(1);
  }

  UNPROTECT(1);
  return out;
}
