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
