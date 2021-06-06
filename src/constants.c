#include <amqp.h>

#include "constants.h"

SEXP message_class = NULL;
SEXP message_names_consume = NULL;
SEXP message_names_get = NULL;
SEXP properties_class = NULL;
SEXP table_class = NULL;
SEXP ptr_object_names = NULL;

SEXP headers_charsxp = NULL;
SEXP content_type_charsxp = NULL;
SEXP content_encoding_charsxp = NULL;
SEXP delivery_mode_charsxp = NULL;
SEXP priority_charsxp = NULL;
SEXP correlation_id_charsxp = NULL;
SEXP reply_to_charsxp = NULL;
SEXP expiration_charsxp = NULL;
SEXP message_id_charsxp = NULL;
SEXP timestamp_charsxp = NULL;
SEXP type_charsxp = NULL;
SEXP user_id_charsxp = NULL;
SEXP app_id_charsxp = NULL;
SEXP cluster_id_charsxp = NULL;

SEXP empty_named_list = NULL;
SEXP empty_table_object = NULL;
SEXP empty_properties_object = NULL;

static SEXP charsxp_container = NULL;
static amqp_table_t empty_table = {};
static amqp_basic_properties_t empty_props = {};

/* This is the method used by the rlang package. */
static SEXP new_shared_vector(SEXPTYPE type, R_len_t n) {
  SEXP out = Rf_allocVector(type, n);
  R_PreserveObject(out);
  MARK_NOT_MUTABLE(out);
  return out;
}

void init_static_sexps()
{
  message_class = new_shared_vector(STRSXP, 1);
  SET_STRING_ELT(message_class, 0, Rf_mkCharLen("amqp_message", 12));

  /* amqp_get and amqp_consume will have different entries. */

  message_names_consume = new_shared_vector(STRSXP, 7);
  SET_STRING_ELT(message_names_consume, 0, Rf_mkCharLen("body", 4));
  SET_STRING_ELT(message_names_consume, 1, Rf_mkCharLen("delivery_tag", 12));
  SET_STRING_ELT(message_names_consume, 2, Rf_mkCharLen("redelivered", 11));
  SET_STRING_ELT(message_names_consume, 3, Rf_mkCharLen("exchange", 8));
  SET_STRING_ELT(message_names_consume, 4, Rf_mkCharLen("routing_key", 11));
  SET_STRING_ELT(message_names_consume, 5, Rf_mkCharLen("consumer_tag", 12));
  SET_STRING_ELT(message_names_consume, 6, Rf_mkCharLen("properties", 10));

  message_names_get = new_shared_vector(STRSXP, 7);
  SET_STRING_ELT(message_names_get, 0, Rf_mkCharLen("body", 4));
  SET_STRING_ELT(message_names_get, 1, Rf_mkCharLen("delivery_tag", 12));
  SET_STRING_ELT(message_names_get, 2, Rf_mkCharLen("redelivered", 11));
  SET_STRING_ELT(message_names_get, 3, Rf_mkCharLen("exchange", 8));
  SET_STRING_ELT(message_names_get, 4, Rf_mkCharLen("routing_key", 11));
  SET_STRING_ELT(message_names_get, 5, Rf_mkCharLen("message_count", 13));
  SET_STRING_ELT(message_names_get, 6, Rf_mkCharLen("properties", 10));

  properties_class = new_shared_vector(STRSXP, 1);
  SET_STRING_ELT(properties_class, 0, Rf_mkCharLen("amqp_properties", 15));

  table_class = new_shared_vector(STRSXP, 1);
  SET_STRING_ELT(table_class, 0, Rf_mkCharLen("amqp_table", 10));

  ptr_object_names = new_shared_vector(STRSXP, 1);
  SET_STRING_ELT(ptr_object_names, 0, Rf_mkCharLen("ptr", 3));

  /* CHARSXP constants. Note that these need to be stuffed in a character vector
   * so they aren't GC'd. */

  charsxp_container = new_shared_vector(STRSXP, 14);

  headers_charsxp = Rf_mkCharLen("headers", 7);
  SET_STRING_ELT(charsxp_container, 0, headers_charsxp);

  content_type_charsxp = Rf_mkCharLen("content_type", 12);
  SET_STRING_ELT(charsxp_container, 1, content_type_charsxp);

  content_encoding_charsxp = Rf_mkCharLen("content_encoding", 16);
  SET_STRING_ELT(charsxp_container, 2, content_encoding_charsxp);

  delivery_mode_charsxp = Rf_mkCharLen("delivery_mode", 13);
  SET_STRING_ELT(charsxp_container, 3, delivery_mode_charsxp);

  priority_charsxp = Rf_mkCharLen("priority", 8);
  SET_STRING_ELT(charsxp_container, 4, priority_charsxp);

  correlation_id_charsxp = Rf_mkCharLen("correlation_id", 14);
  SET_STRING_ELT(charsxp_container, 5, correlation_id_charsxp);

  reply_to_charsxp = Rf_mkCharLen("reply_to", 8);
  SET_STRING_ELT(charsxp_container, 6, reply_to_charsxp);

  expiration_charsxp = Rf_mkCharLen("expiration", 10);
  SET_STRING_ELT(charsxp_container, 7, expiration_charsxp);

  message_id_charsxp = Rf_mkCharLen("message_id", 10);
  SET_STRING_ELT(charsxp_container, 8, message_id_charsxp);

  timestamp_charsxp = Rf_mkCharLen("timestamp", 9);
  SET_STRING_ELT(charsxp_container, 9, timestamp_charsxp);

  type_charsxp = Rf_mkCharLen("type", 4);
  SET_STRING_ELT(charsxp_container, 10, type_charsxp);

  user_id_charsxp = Rf_mkCharLen("user_id", 7);
  SET_STRING_ELT(charsxp_container, 11, user_id_charsxp);

  app_id_charsxp = Rf_mkCharLen("app_id", 6);
  SET_STRING_ELT(charsxp_container, 12, app_id_charsxp);

  cluster_id_charsxp = Rf_mkCharLen("cluster_id", 10);
  SET_STRING_ELT(charsxp_container, 13, cluster_id_charsxp);

  empty_named_list = new_shared_vector(VECSXP, 0);
  Rf_setAttrib(empty_named_list, R_NamesSymbol, Rf_allocVector(STRSXP, 0));

  empty_table_object = new_shared_vector(VECSXP, 1);
  SET_VECTOR_ELT(empty_table_object, 0,
                 R_MakeExternalPtr(&empty_table, R_NilValue, R_NilValue));
  Rf_setAttrib(empty_table_object, R_NamesSymbol, ptr_object_names);
  Rf_setAttrib(empty_table_object, R_ClassSymbol, table_class);

  empty_properties_object = new_shared_vector(VECSXP, 1);
  SET_VECTOR_ELT(empty_properties_object, 0,
                 R_MakeExternalPtr(&empty_props, R_NilValue, R_NilValue));
  Rf_setAttrib(empty_properties_object, R_NamesSymbol, ptr_object_names);
  Rf_setAttrib(empty_properties_object, R_ClassSymbol, properties_class);
}
