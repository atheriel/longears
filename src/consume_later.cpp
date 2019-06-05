#include <amqp.h>
#include <amqp_framing.h>

#include <pthread.h>
#include <later_api.h>

#include "utils.h"

typedef struct {
  connection *conn;
  channel chan;
  amqp_bytes_t tag;
  int no_ack;
  pthread_t thread;
  SEXP fun;
  SEXP rho;
} bg_consumer;

typedef struct {
  bg_consumer *con;
  amqp_envelope_t *env;
} callback_data;

static void R_finalize_bg_consumer(SEXP ptr)
{
  bg_consumer *con = (bg_consumer *) R_ExternalPtrAddr(ptr);
  if (con) {
    if (pthread_cancel(con->thread) == 0) {
      /* No point in checking the result. */
      pthread_join(con->thread, NULL);
    }

    R_ReleaseObject(con->fun);
    R_ReleaseObject(con->rho);

    /* Attempt to cancel the consumer and close the channel. */
    if (con->chan.is_open) {
      amqp_basic_cancel(con->conn->conn, con->chan.chan, con->tag);
      amqp_channel_close(con->conn->conn, con->chan.chan, AMQP_REPLY_SUCCESS);
    }
    /* Attempt to close the connection. */
    if (con->conn->is_connected) {
      amqp_connection_close(con->conn->conn, AMQP_REPLY_SUCCESS);
    }

    amqp_bytes_free(con->tag);
    amqp_destroy_connection(con->conn->conn);
    free(con->conn);
    free(con);
    con = NULL;
  }
  R_ClearExternalPtr(ptr);
}

static void later_callback(void *data)
{
  callback_data *cdata = (callback_data *) data;

  /* Create R-level message object. */
  size_t body_len = cdata->env->message.body.len;
  SEXP body = PROTECT(Rf_allocVector(RAWSXP, body_len));
  memcpy((void *) RAW(body), cdata->env->message.body.bytes, body_len);

  SEXP message = PROTECT(R_message_object(body, cdata->env->delivery_tag,
                                          cdata->env->redelivered,
                                          cdata->env->exchange,
                                          cdata->env->routing_key, -1,
                                          cdata->env->consumer_tag,
                                          &cdata->env->message.properties));

  SEXP R_fcall = PROTECT(Rf_allocList(2));
  SET_TYPEOF(R_fcall, LANGSXP);
  SETCAR(R_fcall, cdata->con->fun);
  SETCADR(R_fcall, message);
  Rf_eval(R_fcall, cdata->con->rho);

  UNPROTECT(3);
  amqp_destroy_envelope(cdata->env);
  free(cdata->env);
  free(cdata);
  return;
}

static void * consume_run(void *data)
{
  bg_consumer *con = (bg_consumer *) data;

  struct timeval tv;
  tv.tv_sec = 1;
  tv.tv_usec = 0;

  amqp_rpc_reply_t reply;
  amqp_envelope_t *env;
  callback_data *ptr;

  for (;;) {
    /* Supress thread cancellation during allocation, etc. */
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);

    /* TODO: Is this still safe? Does the callback rely on memory that is released here? */
    amqp_maybe_release_buffers(con->conn->conn);
    env = (amqp_envelope_t *) malloc(sizeof(amqp_envelope_t));
    reply = amqp_consume_message(con->conn->conn, env, &tv, 0);

    /* If the envelope contains a message, schedule a callback. Note that the callback
     * is responsible for releasing the memory of both (1) ptr; and (2) env. */
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      ptr = (callback_data *) malloc(sizeof(callback_data));
      ptr->con = con;
      ptr->env = env;
      later::later(later_callback, ptr, 0);
    } else {
      amqp_destroy_envelope(env);
      free(env);
    }

    /* Allow the thread to be cancelled here. */
    pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
    pthread_testcancel();
  }

  return NULL;
}

connection *clone_connection(const connection *old)
{
  connection *conn = (connection *) malloc(sizeof(connection));
  conn->host = old->host;
  conn->port = old->port;
  conn->vhost = old->vhost;
  conn->username = old->username;
  conn->password = old->password;
  conn->timeout = old->timeout;
  conn->chan.chan = 0;
  conn->chan.is_open = 0;
  conn->next_chan = 1;
  conn->is_connected = 0;
  conn->conn = amqp_new_connection();

  return (connection *) conn;
}

extern "C" SEXP R_amqp_consume_later(SEXP ptr, SEXP queue, SEXP fun, SEXP rho,
                                     SEXP consumer, SEXP no_ack, SEXP exclusive)
{

  const char *queue_str = CHAR(Rf_asChar(queue));
  const char *consumer_str = CHAR(Rf_asChar(consumer));
  int has_no_ack = Rf_asLogical(no_ack);
  int is_exclusive = Rf_asLogical(exclusive);

  bg_consumer *con = (bg_consumer *) malloc(sizeof(bg_consumer));
  con->conn = clone_connection((connection *) R_ExternalPtrAddr(ptr));
  con->chan.chan = 0;
  con->chan.is_open = 0;
  con->tag = amqp_empty_bytes;
  char errbuff[1000];
  if (ensure_valid_channel(con->conn, &con->chan, errbuff, 1000) < 0) {
    Rf_error("Failed to clone connection. %s", errbuff);
    return R_NilValue;
  }

  con->no_ack = has_no_ack;
  con->fun = fun;
  con->rho = rho;

  amqp_basic_consume_ok_t *consume_ok;
  consume_ok = amqp_basic_consume(con->conn->conn, con->chan.chan,
                                  amqp_cstring_bytes(queue_str),
                                  amqp_cstring_bytes(consumer_str),
                                  0, has_no_ack, is_exclusive,
                                  amqp_empty_table);

  if (consume_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(con->conn->conn);
    render_amqp_error(reply, con->conn, &con->chan, errbuff, 1000);

    /* Clean up. */
    if (con->conn->is_connected) {
      amqp_connection_close(con->conn->conn, AMQP_REPLY_SUCCESS);
    }
    amqp_destroy_connection(con->conn->conn);
    free(con->conn);
    free(con);

    Rf_error("Failed to start a queue consumer. %s", errbuff);
  }

  int res = pthread_create(&con->thread, NULL, consume_run, con);
  if (res != 0) {
    /* Clean up. */
    amqp_connection_close(con->conn->conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(con->conn->conn);
    free(con->conn);
    free(con);

    Rf_error("Failed to create thread. Error: %d.", res);
  }

  con->tag = amqp_bytes_malloc_dup(consume_ok->consumer_tag);

  /* Inhibit GC for the function and environment. This is because R does not
   * have a way of knowing we are storing them in a C struct. */
  R_PreserveObject(fun);
  R_PreserveObject(rho);

  SEXP ext = PROTECT(R_MakeExternalPtr(con, R_NilValue, R_NilValue));
  R_RegisterCFinalizerEx(ext, R_finalize_bg_consumer, TRUE);

  SEXP out = PROTECT(Rf_allocVector(VECSXP, 2));
  SEXP names = PROTECT(Rf_allocVector(STRSXP, 2));
  SET_STRING_ELT(names, 0, Rf_mkCharLen("ptr", 3));
  SET_STRING_ELT(names, 1, Rf_mkCharLen("tag", 3));

  SET_VECTOR_ELT(out, 0, ext);
  SET_VECTOR_ELT(out, 1, amqp_bytes_to_string(&con->tag));

  Rf_setAttrib(out, R_NamesSymbol, names);
  Rf_setAttrib(out, R_ClassSymbol, Rf_mkString("amqp_bg_consumer"));

  UNPROTECT(3);
  return out;
}

extern "C" SEXP R_amqp_destroy_bg_consumer(SEXP ptr)
{
  bg_consumer *con = (bg_consumer *) R_ExternalPtrAddr(ptr);
  if (!con) {
    Rf_error("The consumer has already been destroyed.");
  }
  R_finalize_bg_consumer(ptr);

  return R_NilValue;
}
