#include <stdio.h> /* for snprintf */
#include <stdlib.h> /* for malloc, free */
#include <string.h> /* for strcmp */
#include <time.h> /* for clock_gettime, gettimeoday */
#include <Rinternals.h>

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
  SEXP names = PROTECT(Rf_getAttrib(list, R_NamesSymbol));
  props->_flags = 0;
  props->headers.num_entries = 0;

  /* We accumulate the indices of header arguments on the first pass, then
   * actually fill the table on the second. */
  int max_len = Rf_length(list);
  int headers[max_len];

  int i;
  SEXP elt, name;
  for (i = 0; i < max_len; i++) {
    elt = VECTOR_ELT(list, i);
    name = STRING_ELT(names, i);

    if (strcmp(CHAR(name), "content_type") == 0) {
      if (!isString(elt)) Rf_error("'content_type' must be a string.");
      props->_flags |= AMQP_BASIC_CONTENT_TYPE_FLAG;
      props->content_type = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "content_encoding") == 0) {
      if (!isString(elt)) Rf_error("'content_encoding' must be a string.");
      props->_flags |= AMQP_BASIC_CONTENT_ENCODING_FLAG;
      props->content_encoding = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
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
      props->correlation_id = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "reply_to") == 0) {
      if (!isString(elt)) Rf_error("'reply_to' must be a string.");
      props->_flags |= AMQP_BASIC_REPLY_TO_FLAG;
      props->reply_to = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "expiration") == 0) {
      if (!isString(elt)) Rf_error("'expiration' must be a string.");
      props->_flags |= AMQP_BASIC_EXPIRATION_FLAG;
      props->expiration = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "message_id") == 0) {
      if (!isString(elt)) Rf_error("'message_id' must be a string.");
      props->_flags |= AMQP_BASIC_MESSAGE_ID_FLAG;
      props->message_id = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "timestamp") == 0) {
      /* TODO: Support the timestamp. */
      Rf_warning("The timestamp property is not yet supported, and will be ignored.");
    } else if (strcmp(CHAR(name), "type") == 0) {
      if (!isString(elt)) Rf_error("'type' must be a string.");
      props->_flags |= AMQP_BASIC_TYPE_FLAG;
      props->type = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "user_id") == 0) {
      if (!isString(elt)) Rf_error("'user_id' must be a string.");
      props->_flags |= AMQP_BASIC_USER_ID_FLAG;
      props->user_id = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "app_id") == 0) {
      if (!isString(elt)) Rf_error("'app_id' must be a string.");
      props->_flags |= AMQP_BASIC_APP_ID_FLAG;
      props->app_id = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
    } else if (strcmp(CHAR(name), "cluster_id") == 0) {
      if (!isString(elt)) Rf_error("'cluster_id' must be a string.");
      props->_flags |= AMQP_BASIC_CLUSTER_ID_FLAG;
      props->cluster_id = amqp_cstring_bytes(CHAR(STRING_ELT(elt, 0)));
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

      props->headers.entries[i].key = amqp_cstring_bytes(CHAR(name));
      encode_value(elt, &props->headers.entries[i].value);
    }
  }

  UNPROTECT(1);
  return;
}

SEXP decode_properties(amqp_basic_properties_t *props)
{
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
    SET_STRING_ELT(names, index, mkCharLen("headers", 7));
    index++;
  }

  if (props->_flags & AMQP_BASIC_CONTENT_TYPE_FLAG) {
    SEXP content_type = PROTECT(mkCharLen(props->content_type.bytes,
                                          props->content_type.len));
    SET_VECTOR_ELT(out, index, ScalarString(content_type));
    SET_STRING_ELT(names, index, mkCharLen("content_type", 12));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_CONTENT_ENCODING_FLAG) {
    SEXP content_encoding = PROTECT(mkCharLen(props->content_encoding.bytes,
                                              props->content_encoding.len));
    SET_VECTOR_ELT(out, index, ScalarString(content_encoding));
    SET_STRING_ELT(names, index, mkCharLen("content_encoding", 16));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_DELIVERY_MODE_FLAG) {
    SET_VECTOR_ELT(out, index, ScalarInteger(props->delivery_mode));
    SET_STRING_ELT(names, index, mkCharLen("delivery_mode", 13));
    index++;
  }

  if (props->_flags & AMQP_BASIC_PRIORITY_FLAG) {
    SET_VECTOR_ELT(out, index, ScalarInteger(props->priority));
    SET_STRING_ELT(names, index, mkCharLen("priority", 8));
    index++;
  }

  if (props->_flags & AMQP_BASIC_CORRELATION_ID_FLAG) {
    SEXP correlation_id = PROTECT(mkCharLen(props->correlation_id.bytes,
                                            props->correlation_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(correlation_id));
    SET_STRING_ELT(names, index, mkCharLen("correlation_id", 14));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_REPLY_TO_FLAG) {
    SEXP reply_to = PROTECT(mkCharLen(props->reply_to.bytes,
                                      props->reply_to.len));
    SET_VECTOR_ELT(out, index, ScalarString(reply_to));
    SET_STRING_ELT(names, index, mkCharLen("reply_to", 8));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_EXPIRATION_FLAG) {
    SEXP expiration = PROTECT(mkCharLen(props->expiration.bytes,
                                        props->expiration.len));
    SET_VECTOR_ELT(out, index, ScalarString(expiration));
    SET_STRING_ELT(names, index, mkCharLen("expiration", 10));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_MESSAGE_ID_FLAG) {
    SEXP message_id = PROTECT(mkCharLen(props->message_id.bytes,
                                        props->message_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(message_id));
    SET_STRING_ELT(names, index, mkCharLen("message_id", 10));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_TIMESTAMP_FLAG) {
    /* TODO: This could actually be converted to a time. */
    double timestamp = (double) props->timestamp;
    SET_VECTOR_ELT(out, index, ScalarReal(timestamp));
    SET_STRING_ELT(names, index, mkCharLen("timestamp", 9));
    index++;
  }

  if (props->_flags & AMQP_BASIC_TYPE_FLAG) {
    SEXP type = PROTECT(mkCharLen(props->type.bytes, props->type.len));
    SET_VECTOR_ELT(out, index, ScalarString(type));
    SET_STRING_ELT(names, index, mkCharLen("type", 4));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_USER_ID_FLAG) {
    SEXP user_id = PROTECT(mkCharLen(props->user_id.bytes, props->user_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(user_id));
    SET_STRING_ELT(names, index, mkCharLen("user_id", 7));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_APP_ID_FLAG) {
    SEXP app_id = PROTECT(mkCharLen(props->app_id.bytes, props->app_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(app_id));
    SET_STRING_ELT(names, index, mkCharLen("app_id", 6));
    index++;
    UNPROTECT(1);
  }

  if (props->_flags & AMQP_BASIC_CLUSTER_ID_FLAG) {
    SEXP cluster_id = PROTECT(mkCharLen(props->cluster_id.bytes,
                                        props->cluster_id.len));
    SET_VECTOR_ELT(out, index, ScalarString(cluster_id));
    SET_STRING_ELT(names, index, mkCharLen("cluster_id", 10));
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
  SEXP names = PROTECT(Rf_allocVector(STRSXP, 1));
  SEXP class = PROTECT(Rf_allocVector(STRSXP, 1));
  SET_VECTOR_ELT(out, 0, ptr);
  SET_STRING_ELT(names, 0, mkCharLen("ptr", 3));
  SET_STRING_ELT(class, 0, mkCharLen("amqp_properties", 15));
  Rf_setAttrib(out, R_NamesSymbol, names);
  Rf_setAttrib(out, R_ClassSymbol, class);

  UNPROTECT(4);
  return out;
}

SEXP R_amqp_encode_properties(SEXP list)
{
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
  SEXP out = PROTECT(Rf_allocVector(VECSXP, 8));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, 8));
  SET_STRING_ELT(names, 0, mkCharLen("body", 4));
  SET_STRING_ELT(names, 1, mkCharLen("delivery_tag", 12));
  SET_STRING_ELT(names, 2, mkCharLen("redelivered", 11));
  SET_STRING_ELT(names, 3, mkCharLen("exchange", 8));
  SET_STRING_ELT(names, 4, mkCharLen("routing_key", 11));
  SET_STRING_ELT(names, 6, mkCharLen("properties", 10));
  SET_STRING_ELT(names, 7, mkCharLen("received", 8));

  SET_VECTOR_ELT(out, 0, body);
  SET_VECTOR_ELT(out, 1, ScalarInteger(delivery_tag));
  SET_VECTOR_ELT(out, 2, ScalarLogical(redelivered));
  SET_VECTOR_ELT(out, 3, amqp_bytes_to_string(&exchange));
  SET_VECTOR_ELT(out, 4, amqp_bytes_to_string(&routing_key));
  SET_VECTOR_ELT(out, 7, amqp_timestamp());

  /* amqp_get and amqp_consume will have different entries. */
  if (message_count < 0) {
    SET_STRING_ELT(names, 5, mkCharLen("consumer_tag", 12));
    SET_VECTOR_ELT(out, 5, amqp_bytes_to_string(&consumer_tag));
  } else {
    SET_STRING_ELT(names, 5, mkCharLen("message_count", 13));
    SET_VECTOR_ELT(out, 5, ScalarInteger(message_count));
  }

  if (!props) {
    Rf_warning("Out properties cannot be recovered.\n");
    SET_VECTOR_ELT(out, 6, R_NilValue);
  } else {
    SET_VECTOR_ELT(out, 6, decode_properties(props));
  }

  Rf_setAttrib(out, R_NamesSymbol, names);
  Rf_setAttrib(out, R_ClassSymbol, mkString("amqp_message"));

  UNPROTECT(2);
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

/* Equivalent to Sys.time(). */
SEXP amqp_timestamp()
{
  double epoch = NA_REAL;

 /* R's timestamps have fractional timestamps, so we want sub-second
  * precision. */

#if defined(CLOCK_REALTIME)
  struct timespec tp;
  if (clock_gettime(CLOCK_REALTIME, &tp) == 0) {
    epoch = (double) tp.tv_sec + 1e-9 * (double) tp.tv_nsec;
  }
#else
  struct timeval tv;
  if (gettimeofday(&tv, NULL) == 0) {
    epoch = (double) tv.tv_sec + 1e-6 * (double) tv.tv_usec;
  }
#endif

  SEXP out = PROTECT(Rf_ScalarReal(epoch));
  SEXP class = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(class, 0, Rf_mkCharLen("POSIXct", 7));
  SET_STRING_ELT(class, 1, Rf_mkCharLen("POSIXt", 6));
  Rf_setAttrib(out, R_ClassSymbol, class);

  UNPROTECT(2);
  return out;
}
