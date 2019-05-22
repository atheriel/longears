#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

SEXP R_amqp_bind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  const char *exchange_str = CHAR(asChar(exchange));
  const char *routing_key_str = CHAR(asChar(routing_key));

  amqp_queue_bind_ok_t *bind_ok;
  bind_ok = amqp_queue_bind(conn->conn, conn->chan.chan,
                            amqp_cstring_bytes(queue_str),
                            amqp_cstring_bytes(exchange_str),
                            amqp_cstring_bytes(routing_key_str),
                            amqp_empty_table);

  if (bind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to bind queue. %s", errbuff);
  }

  return R_NilValue;
}

SEXP R_amqp_unbind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  char errbuff[200];
  if (ensure_valid_channel(conn, &conn->chan, errbuff, 200) < 0) {
    Rf_error("Failed to find an open channel. %s", errbuff);
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  const char *exchange_str = CHAR(asChar(exchange));
  const char *routing_key_str = CHAR(asChar(routing_key));

  amqp_queue_unbind_ok_t *unbind_ok;
  unbind_ok = amqp_queue_unbind(conn->conn, conn->chan.chan,
                                amqp_cstring_bytes(queue_str),
                                amqp_cstring_bytes(exchange_str),
                                amqp_cstring_bytes(routing_key_str),
                                amqp_empty_table);

  if (unbind_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
    render_amqp_error(reply, conn, &conn->chan, errbuff, 200);
    Rf_error("Failed to unbind queue. %s", errbuff);
  }

  return R_NilValue;
}
