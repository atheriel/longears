#include "altrep.h"

#ifndef ENABLE_ALTREP
#error unexpected
void maybe_init_altrep(DllInfo *dll) {}
#else /* ENABLE_ALTREP */
#include <string.h>
#include <R_ext/Altrep.h>
#include <amqp.h>

static R_altrep_class_t pooled_bytes_class;

SEXP new_pooled_bytes_sexp(amqp_bytes_t *data)
{
  /* We do not need a finalizer because the pooled memory is managed by
   * librabbitmq. */
  SEXP ptr = PROTECT(R_MakeExternalPtr(data, R_NilValue, R_NilValue));
  SEXP out = R_new_altrep(pooled_bytes_class, ptr, R_NilValue);
  UNPROTECT(1);
  return out;
}

void release_pooled_bytes(SEXP x)
{
  SEXP data1 = R_altrep_data1(x);
  if (data1 != NULL && R_ExternalPtrAddr(data1) != NULL) {
    R_ClearExternalPtr(data1);
  }
}

static R_INLINE int is_released(SEXP x)
{
  SEXP data1 = R_altrep_data1(x);
  return data1 == NULL || R_ExternalPtrAddr(data1) == NULL;
}

static R_xlen_t Length_impl(SEXP x) {
  if (is_released(x)) {
    return 0; /* TODO: What is a reasonable fallback? Rf_error()? */
  }

  /* Return the pooled bytes's length, if they still exist. */
  amqp_bytes_t *data = R_ExternalPtrAddr(R_altrep_data1(x));
  return data->len;
}

static Rboolean Inspect_impl(SEXP x, int pre, int deep, int pvec,
                             void (*inspect_subtree)(SEXP, int, int, int))
{
  Rprintf("amqp_pooled_bytes (len=%d, released=%s)\n", Length_impl(x),
          is_released(x) ? "TRUE" : "FALSE");
  return TRUE;
}

static void* Dataptr_impl(SEXP vec, Rboolean writeable) {
  if (is_released(vec)) {
    /* This should never happen. */
    Rf_error("Can't get a DATAPTR to released memory.");
  }

  /* Return the pooled bytes, if they still exist. */
  amqp_bytes_t *data = R_ExternalPtrAddr(R_altrep_data1(vec));
  return data->bytes;
}

static const void* Dataptr_or_null_impl(SEXP vec) {
  if (is_released(vec)) {
    /* TODO: Should we error instead? */
    return NULL;
  }

  /* Return the pooled bytes, if they still exist. */
  amqp_bytes_t *data = R_ExternalPtrAddr(R_altrep_data1(vec));
  return data->bytes;
}

static Rbyte Elt_impl(SEXP vec, R_xlen_t i) {
  if (is_released(vec)) {
    /* TODO: This should never happen -- should we error instead? */
    return 0;
  }

  /* Return the pooled bytes, if they still exist. */
  amqp_bytes_t *data = R_ExternalPtrAddr(R_altrep_data1(vec));
  return ((Rbyte *) data->bytes)[i];
}

void maybe_init_altrep(DllInfo *dll)
{
  pooled_bytes_class = R_make_altraw_class("amqp_pooled_bytes", "longears", dll);

  /* ALTREP methods */
  R_set_altrep_Length_method(pooled_bytes_class, Length_impl);
  R_set_altrep_Inspect_method(pooled_bytes_class, Inspect_impl);

  /* ALTVEC methods */
  R_set_altvec_Dataptr_method(pooled_bytes_class, Dataptr_impl);
  R_set_altvec_Dataptr_or_null_method(pooled_bytes_class, Dataptr_or_null_impl);

  /* ALTRAW methods */
  R_set_altraw_Elt_method(pooled_bytes_class, Elt_impl);
}
#endif /* ENABLE_ALTREP */
