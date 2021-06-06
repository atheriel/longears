#include <stdio.h> /* for snprintf */
#include <stdlib.h> /* for malloc, free */
#include <string.h> /* for strcmp */
#include <Rinternals.h>

#include "constants.h"
#include "connection.h"
#include "tables.h"
#include "utils.h"

void render_amqp_library_error(int err, connection *conn, channel *chan,
                               char *buffer, size_t len) {
  /* Some errors affect the connection state. */
  switch (err) {
  case AMQP_STATUS_WRONG_METHOD:
    chan->is_open = 0;
    conn->is_connected = 0;
    snprintf(buffer, len, "Unexpected method received. Disconnected.");
    break;
  case AMQP_STATUS_UNEXPECTED_STATE:
    chan->is_open = 0;
    conn->is_connected = 0;
    snprintf(buffer, len, "Unexpected state. Disconnected.");
    break;
  case AMQP_STATUS_CONNECTION_CLOSED:
    /* fallthrough */
  case AMQP_STATUS_SOCKET_ERROR:
    /* fallthrough */
  case AMQP_STATUS_SOCKET_CLOSED:
    chan->is_open = 0;
    conn->is_connected = 0;
    snprintf(buffer, len, "Disconnected from server.");
    break;
  default:
    snprintf(buffer, len, "Library error: %s", amqp_error_string2(err));
    break;
  }
}

void render_amqp_error(const amqp_rpc_reply_t reply, connection *conn,
                       channel *chan, char *buffer, size_t len)
{
  // This is mostly ported from rabbitmq-c/examples/utils.c.
  switch (reply.reply_type) {
  case AMQP_RESPONSE_NONE:
    snprintf(buffer, len, "RPC reply is missing.");
    break;

  case AMQP_RESPONSE_NORMAL:
    // This should never happen.
    snprintf(buffer, len, "Response is NULL with a normal reply.");
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    render_amqp_library_error(reply.library_error, conn, chan, buffer, len);
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (reply.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *method = (amqp_connection_close_t *) reply.reply.decoded;
      snprintf(buffer, len, "%s", (char *) method->reply_text.bytes);
      // These errors will close the connection.
      conn->is_connected = 0;
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *method = (amqp_channel_close_t *) reply.reply.decoded;
      snprintf(buffer, len, "%s", (char *) method->reply_text.bytes);
      // These errors close the channel and require us to open a new one.
      chan->is_open = 0;
      break;
    }
    default:
      snprintf(buffer, len, "Unexpected server error: %s.",
               amqp_method_name(reply.reply.id));
      break;
    }
    break;

  default:
    // This should never happen.
    snprintf(buffer, len, "Unknown reply type: %d.", reply.reply_type);
    break;
  }
}

void encode_properties(const SEXP list, amqp_basic_properties_t *props)
{
  props->_flags = 0;
  props->headers.num_entries = 0;
  int max_len = Rf_length(list);
  if (max_len == 0) {
    return;
  }

  /* We accumulate the indices of header arguments on the first pass, then
   * actually fill the table on the second. */
  SEXP names = PROTECT(Rf_getAttrib(list, R_NamesSymbol));
  int headers[max_len];

  int i;
  SEXP elt, name;
  for (i = 0; i < max_len; i++) {
    elt = VECTOR_ELT(list, i);
    name = STRING_ELT(names, i);

    if (strcmp(CHAR(name), "content_type") == 0) {
      if (!isString(elt)) Rf_error("'content_type' must be a string.");
      props->_flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
      props->content_type = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "content_encoding") == 0) {
      if (!isString(elt)) Rf_error("'content_encoding' must be a string.");
      props->_flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
      props->content_encoding = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "delivery_mode") == 0) {
      if (!isNumeric(elt)) Rf_error("'delivery_mode' must be an integer.");
      props->_flags |= AMQP_BASIC_DELIVERY_MODE_FLAG;
      props->delivery_mode = asInteger(elt);
    } else if (strcmp(CHAR(name), "priority") == 0) {
      if (!isNumeric(elt)) Rf_error("'priority' must be an integer.");
      props->_flags |= AMQP_BASIC_PRIORITY_FLAG;
      props->priority = asInteger(elt);
    } else if (strcmp(CHAR(name), "correlation_id") == 0) {
      if (!isString(elt)) Rf_error("'correlation_id' must be a string.");
      props->_flags |= AMQP_BASIC_CORRELATION_ID_FLAG;
      props->correlation_id = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "reply_to") == 0) {
      if (!isString(elt)) Rf_error("'reply_to' must be a string.");
      props->_flags |= AMQP_BASIC_REPLY_TO_FLAG;
      props->reply_to = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "expiration") == 0) {
      if (!isString(elt)) Rf_error("'expiration' must be a string.");
      props->_flags |= AMQP_BASIC_EXPIRATION_FLAG;
      props->expiration = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "message_id") == 0) {
      if (!isString(elt)) Rf_error("'message_id' must be a string.");
      props->_flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
      props->message_id = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "timestamp") == 0) {
      /* TODO: Support the timestamp. */
      Rf_warning("The timestamp property is not yet supported, and will be ignored.");
    } else if (strcmp(CHAR(name), "type") == 0) {
      if (!isString(elt)) Rf_error("'type' must be a string.");
      props->_flags |= AMQP_BASIC_TYPE_FLAG;
      props->type = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "user_id") == 0) {
      if (!isString(elt)) Rf_error("'user_id' must be a string.");
      props->_flags |= AMQP_BASIC_USER_ID_FLAG;
      props->user_id = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "app_id") == 0) {
      if (!isString(elt)) Rf_error("'app_id' must be a string.");
      props->_flags |= AMQP_BASIC_APP_ID_FLAG;
      props->app_id = strsxp_to_amqp_bytes(elt);
    } else if (strcmp(CHAR(name), "cluster_id") == 0) {
      if (!isString(elt)) Rf_error("'cluster_id' must be a string.");
      props->_flags |= AMQP_BASIC_CLUSTER_ID_FLAG;
      props->cluster_id = strsxp_to_amqp_bytes(elt);
    } else {
      headers[props->headers.num_entries] = i;
      props->headers.num_entries++;
    }
  }

  if (props->headers.num_entries > 0) {
    props->_flags |= AMQP_BASIC_HEADERS_FLAG;
    props->headers.entries = calloc(props->headers.num_entries,
                                    sizeof(amqp_table_entry_t));
    for (i = 0; i < props->headers.num_entries; i++) {
      elt = VECTOR_ELT(list, headers[i]);
      name = STRING_ELT(names, headers[i]);

      props->headers.entries[i].key = charsxp_to_amqp_bytes(name);
      encode_value(elt, &props->headers.entries[i].value);
    }
  }

  UNPROTECT(1);
  return;
}

SEXP decode_properties(amqp_basic_properties_t *props)
{
  if (!props || props->_flags == 0) {
    return empty_named_list;
  }

  /* Determine the total number of flags so we can allocate the right size. */
  int flag_count = 0, flags = props->_flags, index = 0;
  while (flags) {
    flag_count += flags & 1;
    flags >>= 1;
  }
  SEXP out = PROTECT(Rf_allocVector(VECSXP, flag_count));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, flag_count));

  if (props->_flags & AMQP_BASIC_HEADERS_FLAG) {
    SET_VECTOR_ELT(out, index, decode_table(&props->headers));
    SET_STRING_ELT(names, index, headers_charsxp);
    index++;
  }

  if (props->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
    SEXP content_type = PROTECT(mkCharLen(props->content_type.bytes,
                                          props->content_type.len));
    SET_VECTOR_ELT(out, index, ScalarString(content_type));
    SET_STRING_ELT(names, index, content_type_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) {
    SEXP content_encoding = PROTECT(mkCharLen(props->content_encoding.bytes,
                                              props->content_encoding.len));
    SET_VECTOR_ELT(out, index, ScalarString(content_encoding));
    SET_STRING_ELT(names, index, content_encoding_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_DELIVERY_MODE_FLAG) {
    SET_VECTOR_ELT(out, index, ScalarInteger(props->delivery_mode));
    SET_STRING_ELT(names, index, delivery_mode_charsxp);
    index++;
  }

  if (props->_flags & AMQP_BASIC_PRIORITY_FLAG) {
    SET_VECTOR_ELT(out, index, ScalarInteger(props->priority));
    SET_STRING_ELT(names, index, priority_charsxp);
    index++;
  }

  if (props->_flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
    SEXP correlation_id = PROTECT(mkCharLen(props->correlation_id.bytes,
                                            props->correlation_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(correlation_id));
    SET_STRING_ELT(names, index, correlation_id_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_REPLY_TO_FLAG) {
    SEXP reply_to = PROTECT(mkCharLen(props->reply_to.bytes,
                                      props->reply_to.len));
    SET_VECTOR_ELT(out, index, ScalarString(reply_to));
    SET_STRING_ELT(names, index, reply_to_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_EXPIRATION_FLAG) {
    SEXP expiration = PROTECT(mkCharLen(props->expiration.bytes,
                                        props->expiration.len));
    SET_VECTOR_ELT(out, index, ScalarString(expiration));
    SET_STRING_ELT(names, index, expiration_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
    SEXP message_id = PROTECT(mkCharLen(props->message_id.bytes,
                                        props->message_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(message_id));
    SET_STRING_ELT(names, index, message_id_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_TIMESTAMP_FLAG) {
    /* TODO: This could actually be converted to a time. */
    double timestamp = (double) props->timestamp;
    SET_VECTOR_ELT(out, index, ScalarReal(timestamp));
    SET_STRING_ELT(names, index, timestamp_charsxp);
    index++;
  }

  if (props->_flags & AMQP_BASIC_TYPE_FLAG) {
    SEXP type = PROTECT(mkCharLen(props->type.bytes, props->type.len));
    SET_VECTOR_ELT(out, index, ScalarString(type));
    SET_STRING_ELT(names, index, type_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_USER_ID_FLAG) {
    SEXP user_id = PROTECT(mkCharLen(props->user_id.bytes, props->user_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(user_id));
    SET_STRING_ELT(names, index, user_id_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_APP_ID_FLAG) {
    SEXP app_id = PROTECT(mkCharLen(props->app_id.bytes, props->app_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(app_id));
    SET_STRING_ELT(names, index, app_id_charsxp);
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_CLUSTER_ID_FLAG) {
    SEXP cluster_id = PROTECT(mkCharLen(props->cluster_id.bytes,
                                        props->cluster_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(cluster_id));
    SET_STRING_ELT(names, index, cluster_id_charsxp);
    index++;
    UNPROTECT(1);
  }

  Rf_setAttrib(out, R_NamesSymbol, names);
  UNPROTECT(2);
  return out;
}

static void R_finalize_amqp_properties(SEXP ptr)
{
  amqp_basic_properties_t *props = (amqp_basic_properties_t *) R_ExternalPtrAddr(ptr);
  if (props) {
    if (props->headers.num_entries > 0) {
      free(props->headers.entries);
    }
    free(props);
  }
  R_ClearExternalPtr(ptr);
}

SEXP R_properties_object(amqp_basic_properties_t *props)
{
  SEXP ptr = PROTECT(R_MakeExternalPtr(props, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ptr, R_finalize_amqp_properties, 1);

  /* Create the "amqp_properties" object. */
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 1));
  SET_VECTOR_ELT(out, 0, ptr);
  Rf_setAttrib(out, R_NamesSymbol, ptr_object_names);
  Rf_setAttrib(out, R_ClassSymbol, properties_class);

  UNPROTECT(2);
  return out;
}

SEXP R_amqp_encode_properties(SEXP list)
{
  if (Rf_xlength(list) == 0) {
    return empty_properties_object;
  }
  amqp_basic_properties_t *props = malloc(sizeof(amqp_basic_properties_t));
  encode_properties(list, props);
  return R_properties_object(props);
}

SEXP R_amqp_decode_properties(SEXP ptr)
{
  amqp_basic_properties_t *props = (amqp_basic_properties_t *) R_ExternalPtrAddr(ptr);
  if (!props)
    Rf_error("Properties object is no longer valid.");
  return decode_properties(props);
}

SEXP R_message_object(SEXP body, int delivery_tag, int redelivered,
                      amqp_bytes_t exchange, amqp_bytes_t routing_key,
                      int message_count, amqp_bytes_t consumer_tag,
                      amqp_basic_properties_t *props)
{
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 7));
  SET_VECTOR_ELT(out, 0, body);
  SET_VECTOR_ELT(out, 1, ScalarInteger(delivery_tag));
  SET_VECTOR_ELT(out, 2, ScalarLogical(redelivered));
  SET_VECTOR_ELT(out, 3, amqp_bytes_to_string(&exchange));
  SET_VECTOR_ELT(out, 4, amqp_bytes_to_string(&routing_key));
  SET_VECTOR_ELT(out, 6, decode_properties(props));

  /* amqp_get and amqp_consume will have different entries. */
  if (message_count < 0) {
    SET_VECTOR_ELT(out, 5, amqp_bytes_to_string(&consumer_tag));
    Rf_setAttrib(out, R_NamesSymbol, message_names_consume);
  } else {
    SET_VECTOR_ELT(out, 5, ScalarInteger(message_count));
    Rf_setAttrib(out, R_NamesSymbol, message_names_get);
  }

  Rf_setAttrib(out, R_ClassSymbol, message_class);

  UNPROTECT(1);
  return out;
}

SEXP amqp_bytes_to_string(const amqp_bytes_t *in)
{
  return ScalarString(mkCharLen(in->bytes, in->len));
}

SEXP amqp_bytes_to_char(const amqp_bytes_t *in)
{
  return mkCharLen(in->bytes, in->len);
}

amqp_bytes_t charsxp_to_amqp_bytes(const SEXP in)
{
  /* Assume we have a CHARSXP */
  amqp_bytes_t result;
  result.len = XLENGTH(in);
  result.bytes = (void *) CHAR(in);
  return result;
}

amqp_bytes_t strsxp_to_amqp_bytes(const SEXP in)
{
  /* Assume we have a STRSXP of length 1. */
  SEXP content = STRING_ELT(in, 0);
  amqp_bytes_t result;
  result.len = XLENGTH(content);
  result.bytes = (void *) CHAR(content);
  return result;
}
