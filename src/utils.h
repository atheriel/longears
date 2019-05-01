#ifndef __LONGEARS_UTILS_H__
#define __LONGEARS_UTILS_H__

#include <amqp.h>       /* for amqp_rpc_reply_t */
#include "connection.h" /* for connection */

#ifdef __cplusplus
extern "C" {
#endif

void render_amqp_error(const amqp_rpc_reply_t, connection *, char *, size_t);
SEXP decode_properties(amqp_basic_properties_t *props);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_UTILS_H__
