#ifndef __LONGEARS_TRACING_H__
#define __LONGEARS_TRACING_H__

#include "connection.h"

#ifdef __cplusplus
extern "C" {
#endif

void extract_span_ctx(amqp_basic_properties_t *props);
void inject_span_ctx(amqp_basic_properties_t *props);

void * start_publish_span(const char *, const char *, connection *);
void * start_consume_span(const char *, const char *, connection *);
void finish_span(void *, int);

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_TRACING_H__
