#ifndef __LONGEARS_CONNECTION_H__
#define __LONGEARS_CONNECTION_H__

#include <amqp.h> /* for amqp_channel_t, amqp_connection_state_t */

#ifdef __cplusplus
extern "C" {
#endif

/* Forward declaration. */
struct consumer;

typedef struct channel {
  amqp_channel_t chan;
  int is_open;
} channel;

typedef struct connection {
  amqp_connection_state_t conn;
  int is_connected;
  const char *host;
  int port;
  const char *vhost;
  const char *username;
  const char *password;
  int timeout;
  channel chan;
  int next_chan;
  struct consumer *consumers;
} connection;

typedef struct consumer {
  connection *conn;
  channel chan;
  amqp_bytes_t tag;
  SEXP fun;
  SEXP rho;
  int no_ack;
  struct consumer *prev;
  struct consumer *next;
} consumer;

int ensure_valid_channel(connection *, channel *, char *, size_t);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_CONNECTION_H__
