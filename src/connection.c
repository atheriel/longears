#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

static int connect(connection *conn, char *buffer, size_t len);

static void R_finalize_amqp_connection(SEXP ptr)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  if (conn) {
    // Attempt to close the connection.
    amqp_connection_close(conn->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn->conn);
    if (conn->bg_conn) {
      destroy_bg_conn(conn->bg_conn);
    }
    free(conn);
    conn = NULL;
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

  connection *conn = malloc(sizeof(connection)); // NOTE: Assuming this works.
  conn->host = host_str;
  conn->port = port_num;
  conn->vhost = vhost_str;
  conn->username = username_str;
  conn->password = password_str;
  conn->timeout = seconds;
  conn->chan.chan = 0;
  conn->chan.is_open = 0;
  conn->next_chan = 1;
  conn->consumers = NULL;
  conn->bg_conn = NULL;
  conn->is_connected = 0;
  conn->conn = amqp_new_connection();

  if (!conn->conn) {
    free(conn);
    Rf_error("Failed to create an amqp connection.");
    return R_NilValue;
  }

  char msg[120];
  /* Although this should not really be necessary, it seems that under some
   * connection error conditions the stack-allocated array above will trigger
   * errors due to uninitialized memory. */
  memset(msg, 0, 120);
  if (connect(conn, msg, 120) < 0) {
    amqp_destroy_connection(conn->conn);
    free(conn);
    Rf_error("Failed to connect to server. %s", msg);
    return R_NilValue;
  }

  SEXP ptr = PROTECT(R_MakeExternalPtr(conn, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptr, R_finalize_amqp_connection, 1);
  UNPROTECT(1);
  return ptr;
}

SEXP R_amqp_is_connected(SEXP ptr)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  return ScalarLogical(conn->is_connected);
}

SEXP R_amqp_reconnect(SEXP ptr)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }

  if (conn->is_connected) {
    Rprintf("Connection is already open.\n");
  } else {
    char msg[120];
    if (connect(conn, msg, 120) < 0) {
      Rf_error("Failed to reconnect to server. %s", msg);
      return R_NilValue;
    }
  }

  return R_NilValue;
}

SEXP R_amqp_disconnect(SEXP ptr)
{
  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  if (!conn->is_connected) {
    Rprintf("Connection is already closed.\n");
    return R_NilValue;
  }

  amqp_rpc_reply_t reply = amqp_connection_close(conn->conn, AMQP_REPLY_SUCCESS);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    char msg[120];
    render_amqp_error(reply, conn, &conn->chan, msg, 120);
    Rf_error("Failed to disconnect. %s", msg);
    return R_NilValue;
  }
  // Depending on the nature of the possible error above, the connection might
  // be closed without this value having been set.
  conn->is_connected = 0;

  // NOTE: amqp_connection_close() does not seem to close the actual file
  // descriptor of the socket, and we do not seem to be able to re-use them for
  // e.g. reconnection, unfortunately.
  return R_NilValue;
}

static int connect(connection *conn, char *buffer, size_t len)
{
  // Assume conn->conn is valid.
  if (conn->is_connected) return 0;

  // If a connection is closed, clearly the channel(s) are as well.
  conn->chan.is_open = 0;

  amqp_rpc_reply_t reply;
  amqp_socket_t *socket = amqp_tcp_socket_new(conn->conn);

  if (!socket) {
    snprintf(buffer, len, "Failed to create an amqp socket.");
    return -1;
  }

  struct timeval tv;
  tv.tv_sec = conn->timeout;
  tv.tv_usec = 0;
  int sockfd = amqp_socket_open_noblock(socket, conn->host, conn->port, &tv);

  if (sockfd < 0) {
    // This has an unhelpful error message in this case.
    if (sockfd == AMQP_STATUS_SOCKET_ERROR) {
      snprintf(buffer, len, "Is the server running?");
    } else {
      snprintf(buffer, len, "%s", amqp_error_string2(sockfd));
    }
    return -1;
  }

  reply = amqp_login(conn->conn, conn->vhost, AMQP_DEFAULT_MAX_CHANNELS,
                     AMQP_DEFAULT_FRAME_SIZE, 0, AMQP_SASL_METHOD_PLAIN,
                     conn->username, conn->password);

  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, conn, &conn->chan, buffer, len);
    // The connection is likely closed already, but try anyway.
    amqp_connection_close(conn->conn, AMQP_REPLY_SUCCESS);
    return -1;
  }

  conn->is_connected = 1;

  return 0;
}

int ensure_valid_channel(connection *conn, channel *chan, char *buffer, size_t len)
{
  if (!conn) {
    snprintf(buffer, len, "Invalid connection object.");
    return -1;
  }

  // Automatically reconnect.
  if (!conn->is_connected) {
    char msg[120];
    chan->is_open = 0;
    int ret = connect(conn, msg, 120);
    if (ret < 0) {
      snprintf(buffer, len, "Failed to reconnect to server. %s", msg);
      return ret;
    }
  }
  if (chan->is_open) return 0;

  // The connection seems to hang indefinitely if we reuse channel IDs, so make
  // sure to get a new one.
  chan->chan = conn->next_chan;
  conn->next_chan += 1;

  amqp_channel_open(conn->conn, chan->chan);
  amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn->conn);
  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, conn, &conn->chan, buffer, len);
    return -1;
  }

  chan->is_open = 1;
  return 0;
}
