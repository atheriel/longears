#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

SEXP R_amqp_bind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key,
                       SEXP args)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  amqp_bytes_t queue_str = charsxp_to_amqp_bytes(Rf_asChar(queue));
  amqp_bytes_t exchange_str = charsxp_to_amqp_bytes(Rf_asChar(exchange));
  amqp_bytes_t routing_key_str = charsxp_to_amqp_bytes(Rf_asChar(routing_key));
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  amqp_queue_bind_ok_t *bind_ok;
  bind_ok = amqp_queue_bind(conn->conn, conn->chan.chan, queue_str, exchange_str,
                            routing_key_str, *arg_table);

  if (bind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to bind queue. %s", errbuff);
  }

  return R_NilValue;
}

SEXP R_amqp_unbind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key,
                         SEXP args)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  amqp_bytes_t queue_str = charsxp_to_amqp_bytes(Rf_asChar(queue));
  amqp_bytes_t exchange_str = charsxp_to_amqp_bytes(Rf_asChar(exchange));
  amqp_bytes_t routing_key_str = charsxp_to_amqp_bytes(Rf_asChar(routing_key));
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  amqp_queue_unbind_ok_t *unbind_ok;
  unbind_ok = amqp_queue_unbind(conn->conn, conn->chan.chan, queue_str,
                                exchange_str, routing_key_str, *arg_table);

  if (unbind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to unbind queue. %s", errbuff);
  }

  return R_NilValue;
}

SEXP R_amqp_bind_exchange(SEXP ptr, SEXP dest, SEXP source, SEXP routing_key,
                          SEXP args)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  amqp_bytes_t dest_str = charsxp_to_amqp_bytes(Rf_asChar(dest));
  amqp_bytes_t source_str = charsxp_to_amqp_bytes(Rf_asChar(source));
  amqp_bytes_t routing_key_str = charsxp_to_amqp_bytes(Rf_asChar(routing_key));
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  amqp_exchange_bind_ok_t *bind_ok;
  bind_ok = amqp_exchange_bind(conn->conn, conn->chan.chan, dest_str, source_str,
                               routing_key_str, *arg_table);

  if (bind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to bind exchange. %s", errbuff);
  }

  return R_NilValue;
}

SEXP R_amqp_unbind_exchange(SEXP ptr, SEXP dest, SEXP source, SEXP routing_key,
                         SEXP args)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  amqp_bytes_t dest_str = charsxp_to_amqp_bytes(Rf_asChar(dest));
  amqp_bytes_t source_str = charsxp_to_amqp_bytes(Rf_asChar(source));
  amqp_bytes_t routing_key_str = charsxp_to_amqp_bytes(Rf_asChar(routing_key));
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  amqp_exchange_unbind_ok_t *unbind_ok;
  unbind_ok = amqp_exchange_unbind(conn->conn, conn->chan.chan, dest_str,
                                   source_str, routing_key_str, *arg_table);

  if (unbind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to unbind exchange. %s", errbuff);
  }

  return R_NilValue;
}
