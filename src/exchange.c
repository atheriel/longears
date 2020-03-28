#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

SEXP R_amqp_declare_exchange(SEXP ptr, SEXP exchange, SEXP type, SEXP passive,
                             SEXP durable, SEXP auto_delete, SEXP internal,
                             SEXP args)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *exchange_str = CHAR(asChar(exchange));
  const char *type_str = CHAR(asChar(type));
  int is_passive = asLogical(passive);
  int is_durable = asLogical(durable);
  int is_auto_delete = asLogical(auto_delete);
  int is_internal = asLogical(internal);
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  amqp_exchange_declare_ok_t *exch_ok;
  exch_ok = amqp_exchange_declare(conn->conn, conn->chan.chan,
                                  amqp_cstring_bytes(exchange_str),
                                  amqp_cstring_bytes(type_str), is_passive,
                                  is_durable, is_auto_delete, is_internal,
                                  *arg_table);

  if (exch_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to declare exchange. %s", errbuff);
  }

  return R_NilValue;
}

SEXP R_amqp_delete_exchange(SEXP ptr, SEXP exchange, SEXP if_unused)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *exchange_str = CHAR(asChar(exchange));
  int unused = asLogical(if_unused);

  amqp_exchange_delete_ok_t *delete_ok;
  delete_ok = amqp_exchange_delete(conn->conn, conn->chan.chan,
                                   amqp_cstring_bytes(exchange_str), unused);

  if (delete_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to delete exchange. %s", errbuff);
  }

  return R_NilValue;
}
