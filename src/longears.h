#ifndef __LONGEARS_H__
#define __LONGEARS_H__

#include <Rinternals.h> /* for SEXP */

#ifdef __cplusplus
extern "C" {
#endif

SEXP R_amqp_connect(SEXP host, SEXP port, SEXP vhost, SEXP username, SEXP password, SEXP timeout);
SEXP R_amqp_is_connected(SEXP ptr);
SEXP R_amqp_disconnect(SEXP ptr);

SEXP R_amqp_declare_exchange(SEXP ptr, SEXP exchange, SEXP type, SEXP passive, SEXP durable, SEXP auto_delete, SEXP internal);
SEXP R_amqp_delete_exchange(SEXP ptr, SEXP exchange, SEXP if_unused);

SEXP R_amqp_declare_queue(SEXP ptr, SEXP queue, SEXP passive, SEXP exclusive, SEXP durable, SEXP auto_delete);
SEXP R_amqp_delete_queue(SEXP ptr, SEXP queue, SEXP if_unused, SEXP if_empty);

SEXP R_amqp_bind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key);
SEXP R_amqp_unbind_queue(SEXP ptr, SEXP queue, SEXP exchange, SEXP routing_key);

SEXP R_amqp_publish(SEXP ptr, SEXP routing_key, SEXP body, SEXP exchange, SEXP context_type, SEXP mandatory, SEXP immediate);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_H__
