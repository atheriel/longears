#ifndef __LONGEARS_CONSTANTS_H__
#define __LONGEARS_CONSTANTS_H__

#define STRICT_R_HEADERS
#include <Rinternals.h> /* for SEXP */

#ifdef __cplusplus
extern "C" {
#endif

extern SEXP message_class;
extern SEXP message_names_consume;
extern SEXP message_names_get;
extern SEXP properties_class;
extern SEXP table_class;
extern SEXP ptr_object_names;

extern SEXP headers_charsxp;
extern SEXP content_type_charsxp;
extern SEXP content_encoding_charsxp;
extern SEXP delivery_mode_charsxp;
extern SEXP priority_charsxp;
extern SEXP correlation_id_charsxp;
extern SEXP reply_to_charsxp;
extern SEXP expiration_charsxp;
extern SEXP message_id_charsxp;
extern SEXP timestamp_charsxp;
extern SEXP type_charsxp;
extern SEXP user_id_charsxp;
extern SEXP app_id_charsxp;
extern SEXP cluster_id_charsxp;

extern SEXP empty_named_list;
extern SEXP empty_table_object;
extern SEXP empty_properties_object;

void init_static_sexps();

#ifdef __cplusplus
}
#endif

#endif // __LONGEARS_CONSTANTS_H__
