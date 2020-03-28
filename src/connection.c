#include <stdlib.h>
#include <string.h>
#include <sys/time.h>

#include <amqp.h>
#include <amqp_tcp_socket.h>
#include <amqp_framing.h>

#include "longears.h"
#include "connection.h"
#include "utils.h"

static void mark_consumers_closed(struct connection *conn);

static void R_finalize_amqp_connection(SEXP ptr)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
  if (conn) {
    // Attempt to close the connection, if it appears to be open.
    if (conn->conn) {
      amqp_connection_close(conn->conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn->conn);
    }
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

  struct connection *conn = malloc(sizeof(struct connection)); // NOTE: Assuming this works.
  conn->thread = 0;
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
  pthread_mutex_init(&conn->mutex, NULL);

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
  if (lconnect(conn, msg, 120) < 0) {
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
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }
  return ScalarLogical(conn->is_connected);
}

SEXP R_amqp_reconnect(SEXP ptr)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
  if (!conn) {
    Rf_error("The amqp connection no longer exists.");
    return R_NilValue;
  }

  if (conn->is_connected) {
    Rprintf("Connection is already open.\n");
  } else {
    char msg[120];
    if (lconnect(conn, msg, 120) < 0) {
      Rf_error("Failed to reconnect to server. %s", msg);
      return R_NilValue;
    }
  }

  return R_NilValue;
}

SEXP R_amqp_disconnect(SEXP ptr)
{
  struct connection *conn = (struct connection *) R_ExternalPtrAddr(ptr);
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
  amqp_destroy_connection(conn->conn);
  conn->conn = NULL;

  mark_consumers_closed(conn);

  // NOTE: amqp_connection_close() does not seem to close the actual file
  // descriptor of the socket, and we do not seem to be able to re-use them for
  // e.g. reconnection, unfortunately.
  return R_NilValue;
}

int lconnect(struct connection *conn, char *buffer, size_t len)
{
  // Assume conn->conn is valid.
  if (conn->is_connected) return 0;

  if (conn->conn) {
    amqp_destroy_connection(conn->conn);
  }
  conn->conn = amqp_new_connection();

  // If a connection is closed, clearly the channel(s) are as well.
  conn->chan.is_open = 0;

  amqp_rpc_reply_t reply;
  amqp_socket_t *socket = amqp_tcp_socket_new(conn->conn);

  if (!socket) {
    snprintf(buffer, len, "Failed to create an amqp socket.");
    amqp_destroy_connection(conn->conn);
    conn->conn = NULL;
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
    amqp_destroy_connection(conn->conn);
    conn->conn = NULL;
    return -1;
  }

  amqp_table_t props, capabilities;
  amqp_table_entry_t pentry[6], centry;

  props.num_entries = 6;
  props.entries = pentry;
  capabilities.num_entries = 1;
  capabilities.entries = &centry;

  pentry[0].key = amqp_cstring_bytes("version");
  pentry[0].value.kind = AMQP_FIELD_KIND_UTF8;
  pentry[0].value.value.bytes = amqp_cstring_bytes(LONGEARS_FULL_VERSION);

  pentry[1].key = amqp_cstring_bytes("product");
  pentry[1].value.kind = AMQP_FIELD_KIND_UTF8;
  pentry[1].value.value.bytes = amqp_cstring_bytes("longears");

  pentry[2].key = amqp_cstring_bytes("copyright");
  pentry[2].value.kind = AMQP_FIELD_KIND_UTF8;
  pentry[2].value.value.bytes = amqp_cstring_bytes("Copyright (c) Crescendo Technology Ltd.");

  pentry[3].key = amqp_cstring_bytes("information");
  pentry[3].value.kind = AMQP_FIELD_KIND_UTF8;
  pentry[3].value.value.bytes = amqp_cstring_bytes("Licensed under the GPL (>=2). See https://github.com/atheriel/longears/");

  pentry[4].key = amqp_cstring_bytes("connection_name");
  pentry[4].value.kind = AMQP_FIELD_KIND_UTF8;
  pentry[4].value.value.bytes = amqp_cstring_bytes("longears");

  pentry[5].key = amqp_cstring_bytes("capabilities");
  pentry[5].value.kind = AMQP_FIELD_KIND_TABLE;
  pentry[5].value.value.table = capabilities;

  /* Tell the server that we can handle consumer cancel notifications. */
  centry.key = amqp_cstring_bytes("consumer_cancel_notify");
  centry.value.kind = AMQP_FIELD_KIND_BOOLEAN;
  centry.value.value.boolean = 1;

  reply = amqp_login_with_properties(conn->conn, conn->vhost,
                                     AMQP_DEFAULT_MAX_CHANNELS,
                                     AMQP_DEFAULT_FRAME_SIZE, 60, &props,
                                     AMQP_SASL_METHOD_PLAIN, conn->username,
                                     conn->password);

  if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
    render_amqp_error(reply, conn, &conn->chan, buffer, len);
    // The connection is likely closed already, but try anyway.
    amqp_connection_close(conn->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn->conn);
    conn->conn = NULL;
    return -1;
  }

  conn->is_connected = 1;

  /* Clean up any leftover consumers. */
  if (conn->consumers) {
    Rf_warning("Existing consumers have been lost and must be recreated.");
    mark_consumers_closed(conn);
  }

  return 0;
}

int ensure_valid_channel(struct connection *conn, struct channel *chan,
                         char *buffer, size_t len)
{
  if (!conn) {
    snprintf(buffer, len, "Invalid connection object.");
    return -1;
  }

  if (!conn->is_connected) {
    chan->is_open = 0;
    snprintf(buffer, len, "Not connected to a server.");
    return -1;
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

static void mark_consumers_closed(struct connection *conn)
{
  struct consumer *elt = conn->consumers;
  while (elt) {
    elt->chan.is_open = 0;
    elt = elt->next;
  }
}
