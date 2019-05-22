#ifndef __LONGEARS_CONSUME_H__
#define __LONGEARS_CONSUME_H__

#include <amqp.h>       /* for amqp_rpc_reply_t */
#include "connection.h" /* for connection, channel */

#ifdef __cplusplus
extern "C" {
#endif

typedef struct {
  connection *conn;
  channel chan;
  amqp_bytes_t tag;
} consumer;

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_CONSUME_H__
