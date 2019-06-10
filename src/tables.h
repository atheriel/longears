#ifndef __LONGEARS_TABLES_H__
#define __LONGEARS_TABLES_H__

#include <Rinternals.h>
#include <amqp.h>

#ifdef __cplusplus
extern "C" {
#endif

void encode_table(SEXP list, amqp_table_t *table, int alloc);
void encode_value(const SEXP in, amqp_field_value_t *out);
SEXP decode_table(amqp_table_t *table);


#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_TABLES_H__
