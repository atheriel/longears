#ifndef __LONGEARS_UTILS_H__
#define __LONGEARS_UTILS_H__

#include <amqp.h>       /* for amqp_rpc_reply_t */
#include "connection.h" /* for connection, channel */

#ifdef __cplusplus
extern "C" {
#endif

void render_amqp_library_error(int err, connection *conn, channel *chan,
                               char *buffer, size_t len);
void render_amqp_error(const amqp_rpc_reply_t reply, connection *conn,
                       channel *chan, char *err_buffer, size_t buffer_len);
SEXP R_properties_object(amqp_basic_properties_t *props);
SEXP R_message_object(SEXP body, int delivery_tag, int redelivered,
                      amqp_bytes_t exchange, amqp_bytes_t routing_key,
                      int message_count, amqp_bytes_t consumer_tag,
                      amqp_basic_properties_t *props);
SEXP amqp_bytes_to_string(const amqp_bytes_t *in);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_UTILS_H__
