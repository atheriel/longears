#ifndef __LONGEARS_H__
#define __LONGEARS_H__

#include <Rinternals.h> /* for SEXP */

#ifdef __cplusplus
extern "C" {
#endif

SEXP R_amqp_connect(SEXP host, SEXP port, SEXP vhost, SEXP username, SEXP password, SEXP timeout);
SEXP R_amqp_is_connected(SEXP ptr);
SEXP R_amqp_disconnect(SEXP ptr);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_H__
