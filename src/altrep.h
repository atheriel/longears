#ifndef __LONGEARS_ALTREP_H__
#define __LONGEARS_ALTREP_H__

#define STRICT_R_HEADERS
#include <Rinternals.h> /* for SEXP */
#include <R_ext/Rdynload.h> /* for DllInfo */
#include <Rversion.h>
#include <amqp.h> /* for amqp_bytes_t */

#ifdef __cplusplus
extern "C" {
#endif

#if !defined(ENABLE_ALTREP) && defined(R_VERSION) && R_VERSION >= R_Version(3, 5, 0)
#define ENABLE_ALTREP 1

SEXP new_pooled_bytes_sexp(amqp_bytes_t *);
void release_pooled_bytes(SEXP);
#endif

void maybe_init_altrep(DllInfo *);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_ALTREP_H__
