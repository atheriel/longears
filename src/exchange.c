#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "utils.h"

SEXP R_amqp_declare_exchange(SEXP ptr, SEXP exchange, SEXP type, SEXP passive,
                             SEXP durable, SEXP auto_delete, SEXP internal)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *exchange_str = CHAR(asChar(exchange));
  const char *type_str = CHAR(asChar(type));
  int is_passive = asLogical(passive);
  int is_durable = asLogical(durable);
  int is_auto_delete = asLogical(auto_delete);
  int is_internal = asLogical(internal);

  amqp_exchange_declare_ok_t *exch_ok;
  exch_ok = amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange_str),
                                  amqp_cstring_bytes(type_str), is_passive,
                                  is_durable, is_auto_delete, is_internal,
                                  amqp_empty_table);

  if (exch_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: exchange declare response is NULL with a normal reply.");
    } else {
      handle_amqp_error("Failed to declare exchange.", reply);
    }
  }

  return R_NilValue;
}

SEXP R_amqp_delete_exchange(SEXP ptr, SEXP exchange, SEXP if_unused)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *exchange_str = CHAR(asChar(exchange));
  int unused = asLogical(if_unused);

  amqp_exchange_delete_ok_t *delete_ok;
  delete_ok = amqp_exchange_delete(conn, 1, amqp_cstring_bytes(exchange_str),
                                   unused);

  if (delete_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: exchange delete response is NULL with a normal reply.");
    } else {
      handle_amqp_error("Failed to delete exchange.", reply);
    }
  }

  return R_NilValue;
}
