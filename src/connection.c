#include <stdlib.h>
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"

static void R_finalize_amqp_connection(SEXP ptr)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (conn) {
    // Attempt to close the connection.
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
  }
  R_ClearExternalPtr(ptr);
}

SEXP R_amqp_connect(SEXP host, SEXP port, SEXP vhost, SEXP username,
                    SEXP password, SEXP timeout)
{
  const char *host_str = CHAR(asChar(host));
  int port_num = asInteger(port);
  const char *vhost_str = CHAR(asChar(vhost));
  const char *username_str = CHAR(asChar(username));
  const char *password_str = CHAR(asChar(password));
  int seconds = asInteger(timeout);

  amqp_connection_state_t conn = amqp_new_connection();

  if (!conn) {
    Rf_error("Failed to create an amqp connection.");
    return R_NilValue;
  }

  amqp_socket_t *socket = amqp_tcp_socket_new(conn);

  if (!socket) {
    amqp_destroy_connection(conn);
    Rf_error("Failed to create an amqp socket.");
    return R_NilValue;
  }

  struct timeval tv;
  tv.tv_sec = seconds;
  tv.tv_usec = 0;
  int sockfd = amqp_socket_open_noblock(socket, host_str, port_num, &tv);
  if (sockfd < 0) {
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    // This has an unhelpful error message in this case.
    if (sockfd == AMQP_STATUS_SOCKET_ERROR) {
      Rf_error("Failed to open amqp socket. Is the server running?");
    } else {
      Rf_error("Failed to open amqp socket. Error: %d.",
               amqp_error_string2(sockfd));
    }
    return R_NilValue;
  }

  /* Log in. */

  amqp_rpc_reply_t login_reply = amqp_login(conn, vhost_str,
                                            AMQP_DEFAULT_MAX_CHANNELS,
                                            AMQP_DEFAULT_FRAME_SIZE, 0,
                                            AMQP_SASL_METHOD_PLAIN,
                                            username_str, password_str);

  if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    // FIXME: Report better errors for login failures.
    Rf_error("Failed to log in.");
    return R_NilValue;
  }

  amqp_channel_open(conn, 1);
  amqp_rpc_reply_t open_reply = amqp_get_rpc_reply(conn);
  if (open_reply.reply_type != AMQP_RESPONSE_NORMAL) {
    amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    Rf_error("Failed to open channel.");
    return R_NilValue;
  }

  SEXP ptr = PROTECT(R_MakeExternalPtr(conn, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptr, R_finalize_amqp_connection, 1);
  UNPROTECT(1);
  return ptr;
}

SEXP R_amqp_is_connected(SEXP ptr)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  return (conn) ? ScalarLogical(1) : ScalarLogical(0);
}

SEXP R_amqp_disconnect(SEXP ptr)
{
  amqp_connection_state_t conn = (amqp_connection_state_t) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  amqp_rpc_reply_t reply = amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    // FIXME: Report better errors for failures.
    Rf_error("Failed to disconnect.");
    return R_NilValue;
  }
  amqp_destroy_connection(conn);
  R_ClearExternalPtr(ptr);
  return R_NilValue;
}
