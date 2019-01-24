#ifndef __LONGEARS_UTILS_H__
#define __LONGEARS_UTILS_H__

#include <Rinternals.h>
#include <amqp.h>       /* for amqp_rpc_reply_t */

#ifdef __cplusplus
extern "C" {
#endif

void handle_amqp_error(const char *ctxt, amqp_rpc_reply_t reply);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_UTILS_H__
