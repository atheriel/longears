#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "utils.h"

SEXP R_amqp_bind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  const char *exchange_str = CHAR(asChar(exchange));
  const char *routing_key_str = CHAR(asChar(routing_key));

  amqp_queue_bind_ok_t *bind_ok;
  bind_ok = amqp_queue_bind(conn, 1, amqp_cstring_bytes(queue_str),
                            amqp_cstring_bytes(exchange_str),
                            amqp_cstring_bytes(routing_key_str),
                            amqp_empty_table);

  if (bind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: queue bind response is NULL with a normal reply.");
    } else {
      handle_amqp_error("Failed to bind queue.", reply);
    }
  }

  return R_NilValue;
}

SEXP R_amqp_unbind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  const char *exchange_str = CHAR(asChar(exchange));
  const char *routing_key_str = CHAR(asChar(routing_key));

  amqp_queue_unbind_ok_t *unbind_ok;
  unbind_ok = amqp_queue_unbind(conn, 1, amqp_cstring_bytes(queue_str),
                                amqp_cstring_bytes(exchange_str),
                                amqp_cstring_bytes(routing_key_str),
                                amqp_empty_table);

  if (unbind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: queue unbind response is NULL with a normal reply.");
    } else {
      handle_amqp_error("Failed to unbind queue.", reply);
    }
  }

  return R_NilValue;
}
