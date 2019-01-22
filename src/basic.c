#include <stdlib.h> /* for calloc, free */
#include <string.h> /* for strncpy */

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"

SEXP R_amqp_publish(SEXP ptr, SEXP routing_key, SEXP body, SEXP exchange,
                    SEXP content_type, SEXP mandatory, SEXP immediate)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *routing_key_str = CHAR(asChar(routing_key));
  const char *body_str = CHAR(asChar(body));
  const char *exchange_str = CHAR(asChar(exchange));
  const char *content_type_str = CHAR(asChar(content_type));
  int is_mandatory = asLogical(mandatory);
  int is_immediate = asLogical(immediate);

  /* Send message. */

  amqp_basic_properties_t props;
  props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
    AMQP_BASIC_DELIVERY_MODE_FLAG;
  props.content_type = amqp_cstring_bytes(content_type_str);
  props.delivery_mode = 1;

  int result = amqp_basic_publish(conn, 1, amqp_cstring_bytes(exchange_str),
                                  amqp_cstring_bytes(routing_key_str),
                                  is_mandatory, is_immediate, &props,
                                  amqp_cstring_bytes(body_str));

  if (result != AMQP_STATUS_OK) {
    Rf_error("Failed to publish message. Error: %s.",
             amqp_error_string2(result));
    return R_NilValue;
  }

  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    // FIXME: Report better errors for failures.
    Rf_error("Failed to publish message.");
  }

  return R_NilValue;
}

SEXP R_amqp_get(SEXP ptr, SEXP queue, SEXP no_ack)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  int has_no_ack = asLogical(no_ack);

  /* Get message. */

  // TODO: Add the ability to acknowledge the message.
  amqp_rpc_reply_t reply = amqp_basic_get(conn, 1, amqp_cstring_bytes(queue_str),
                                          has_no_ack);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    // FIXME: Report better errors for failures.
    Rf_error("Failed to get message.");
    return R_NilValue;
  } else if (reply.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
    return allocVector(STRSXP, 0); // Equivalent to character(0).
  }

  amqp_frame_t frame;
  int result = amqp_simple_wait_frame(conn, &frame);
  if (result != AMQP_STATUS_OK) {
    Rf_error("Failed to read frame. Error: %d.", result);
    return R_NilValue;
  }
  if (frame.frame_type != AMQP_FRAME_HEADER) {
    Rf_error("Failed to read frame. Unexpected header type: %d.",
             frame.frame_type);
    return R_NilValue;
  }

  size_t body_remaining = frame.payload.properties.body_size;
  char *body = calloc(1, body_remaining); // NOTE: Assuming this works here.
  while (body_remaining) {
    result = amqp_simple_wait_frame(conn, &frame);
    if (result != AMQP_STATUS_OK) {
      Rf_error("Failed to wait for frame. Error: %d.", result);
      free(body);
      return R_NilValue;
    }
    strncpy(body, (const char *) frame.payload.body_fragment.bytes,
            frame.payload.body_fragment.len);
    body_remaining -= frame.payload.body_fragment.len;
  }
  return ScalarString(mkChar(body));
}
