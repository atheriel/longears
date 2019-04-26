#ifndef __LONGEARS_UTILS_H__
#define __LONGEARS_UTILS_H__

#include <Rinternals.h>
#include <amqp.h>       /* for amqp_rpc_reply_t */
#include "connection.h" /* for connection */

#ifdef __cplusplus
extern "C" {
#endif

void handle_amqp_error(const char *ctxt, amqp_rpc_reply_t reply);
void render_amqp_error(const amqp_rpc_reply_t reply, connection *conn, char *buffer, size_t len);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_UTILS_H__
