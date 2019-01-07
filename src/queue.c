#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"

SEXP R_amqp_declare_queue(SEXP ptr, SEXP queue, SEXP passive, SEXP durable,
                          SEXP exclusive, SEXP auto_delete)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  int is_passive = asLogical(passive);
  int is_exclusive = asLogical(exclusive);
  int is_durable = asLogical(durable);
  int is_auto_delete = asLogical(auto_delete);

  amqp_queue_declare_ok_t *queue_ok;
  queue_ok = amqp_queue_declare(conn, 1, amqp_cstring_bytes(queue_str),
                                is_passive, is_exclusive, is_durable,
                                is_auto_delete, amqp_empty_table);

  if (queue_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: queue declare response is NULL with a normal reply.");
      return R_NilValue;
    } else {
      // FIXME: Report better errors for failures.
      Rf_error("Failed to declare queue.");
      return R_NilValue;
    }
  }

  SEXP out = PROTECT(allocVector(VECSXP, 3));
  SEXP names = PROTECT(allocVector(STRSXP, 3));
  SEXP qname = PROTECT(mkCharLen(queue_ok->queue.bytes, queue_ok->queue.len));

  SET_VECTOR_ELT(out, 0, ScalarString(qname));
  SET_VECTOR_ELT(out, 1, ScalarInteger(queue_ok->message_count));
  SET_VECTOR_ELT(out, 2, ScalarInteger(queue_ok->consumer_count));

  SET_STRING_ELT(names, 0, mkChar("queue"));
  SET_STRING_ELT(names, 1, mkChar("message_count"));
  SET_STRING_ELT(names, 2, mkChar("consumer_count"));
  setAttrib(out, R_NamesSymbol, names);
  setAttrib(out, R_ClassSymbol, ScalarString(mkChar("amqp_queue")));

  UNPROTECT(3);
  return out;
}

SEXP R_amqp_delete_queue(SEXP ptr, SEXP queue, SEXP if_unused, SEXP if_empty)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  const char *queue_str = CHAR(asChar(queue));
  int unused = asLogical(if_unused);
  int empty = asLogical(if_empty);

  amqp_queue_delete_ok_t *delete_ok;
  delete_ok = amqp_queue_delete(conn, 1, amqp_cstring_bytes(queue_str), unused,
                                empty);

  if (delete_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      // This should never happen.
      Rf_error("Unexpected error: queue delete response is NULL with a normal reply.");
      return R_NilValue;
    } else {
      // FIXME: Report better errors for failures.
      Rf_error("Failed to delete queue.");
      return R_NilValue;
    }
  }

  return ScalarInteger(delete_ok->message_count);
}
