#include <time.h> /* for nanosleep */

#include <amqp.h>
#include <amqp_framing.h>

#include <pthread.h>
#include <later_api.h>

#include "utils.h"

typedef struct bg_consumer {
  bg_conn *conn;
  channel chan;
  amqp_bytes_t tag;
  int no_ack;
  SEXP fun;
  SEXP rho;
  struct bg_consumer *next;
  struct bg_consumer *prev;
} bg_consumer;

typedef struct {
  bg_conn *conn;
  amqp_envelope_t *env;
} callback_data;

static void R_finalize_bg_consumer(SEXP ptr)
{
  bg_consumer *con = (bg_consumer *) R_ExternalPtrAddr(ptr);
  if (con && con->conn) {
    pthread_mutex_lock(&con->conn->mutex);

    /* Attempt to cancel the consumer and close the channel. We need to ensure
       that we have exclusive access to the connection before we do this. It's
       also important to keep track of channel/connection errors, since they can
       affect other consumers. */
    if (con->conn->conn->is_connected && con->chan.is_open) {
      amqp_rpc_reply_t reply = amqp_channel_close(con->conn->conn->conn,
                                                  con->chan.chan,
                                                  AMQP_REPLY_SUCCESS);
      if (reply.reply_type != AMQP_RESPONSE_NORMAL) {
        char errbuff[200];
        render_amqp_error(reply, con->conn->conn, &con->chan, errbuff, 200);
      } else {
        /* TODO: This may not be necessary. */
        amqp_maybe_release_buffers_on_channel(con->conn->conn->conn,
                                              con->chan.chan);
      }
    }

    /* Remove it from the global list of consumers. */
    if (con->next) {
      con->next->prev = con->prev;
    }
    if (con->prev) {
      con->prev->next = con->next;
    } else if (con->conn->consumers == con) {
      con->conn->consumers = con->next;
    }

    pthread_mutex_unlock(&con->conn->mutex);
  }

  if (con) {
    amqp_bytes_free(con->tag);
    R_ReleaseObject(con->fun);
    R_ReleaseObject(con->rho);
    free(con);
    con = NULL;
  }
  R_ClearExternalPtr(ptr);
}

static void later_callback(void *data)
{
  callback_data *cdata = (callback_data *) data;

  /* Find the consumer for the envelope. */

  bg_consumer *elt = (bg_consumer *) cdata->conn->consumers;
  while (elt && strncmp((const char *) elt->tag.bytes,
                        (const char *) cdata->env->consumer_tag.bytes,
                        elt->tag.len) != 0) {
    elt = elt->next;
  }
  if (!elt) {
    /* Quietly swallow messages sent to now-cancelled consumers. */
    amqp_destroy_envelope(cdata->env);
    free(cdata->env);
    free(cdata);
    return;
  }

  /* Acknowledge message receipt. TODO: Can we n'ack messages for cancelled
     consumers swallowed above? */
  if (!elt->no_ack) {
    pthread_mutex_lock(&cdata->conn->mutex);
    int ack = amqp_basic_ack(cdata->conn->conn->conn, elt->chan.chan,
                             cdata->env->delivery_tag, 0);
    if (ack != AMQP_STATUS_OK) {
      Rf_warning("Failed to acknowledge message. %s", amqp_error_string2(ack));
    }
    pthread_mutex_unlock(&cdata->conn->mutex);
  }

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
  SETCAR(R_fcall, elt->fun);
  SETCADR(R_fcall, message);
  Rf_eval(R_fcall, elt->rho);

  UNPROTECT(3);
  amqp_destroy_envelope(cdata->env);
  free(cdata->env);
  free(cdata);
  return;
}

enum bg_consumer_err {
  BG_ERR_DISCONNECTED,
  BG_ERR_UNEXPECTED_STATUS,
  BG_ERR_CONSUMER_CANCEL
};

struct bg_consumer_err_data {
  enum bg_consumer_err kind;
  union {
    int status;
    amqp_bytes_t tag;
  } payload;
};

static void later_warn_callback(void *data)
{
  struct bg_consumer_err_data *err = (struct bg_consumer_err_data *) data;
  switch(err->kind) {
  case BG_ERR_DISCONNECTED:
    free(err);
    Rf_warning("Disconnected from server. Existing background consumers have been lost and must be recreated.");
    break;
  case BG_ERR_UNEXPECTED_STATUS:
    {
      int status = err->payload.status;
      free(err);
      Rf_warning("Unexpected AMQP status during consume: %d.", status);
    }
    break;
  case BG_ERR_CONSUMER_CANCEL:
    {
      char tag[128];
      strncpy(tag, (const char *) err->payload.tag.bytes, err->payload.tag.len);
      tag[err->payload.tag.len] = '\0';
      amqp_bytes_free(err->payload.tag);
      free(err);
      Rf_warning("Consumer '%s' cancelled by the broker.", tag);
    }
    break;
  }
  return;
}

static void * consume_run(void *data)
{
  bg_conn *con = (bg_conn *) data;

  struct timeval tv;
  tv.tv_sec = 0;
  tv.tv_usec = 0;

  /* Sleeping between checks for messages allows us to reliably acquire the
     mutex on the main thread. However, it also effectively sets the maximum
     thoroughput for consumers.

     I estimate R code will not handle more than 100 messages/second, so use
     that to infer a sleep time of 0.01 seconds. */
  struct timespec sleeptime;
  sleeptime.tv_sec = 0;
  sleeptime.tv_nsec = 10000000;

  amqp_rpc_reply_t reply;
  amqp_envelope_t *env;
  callback_data *ptr;
  struct bg_consumer_err_data *cdata;

  for (;;) {
    nanosleep(&sleeptime, NULL);

    /* Supress thread cancellation during allocation, etc. */
    pthread_setcancelstate(PTHREAD_CANCEL_DISABLE, NULL);
    pthread_mutex_lock(&con->mutex);

    /* Sleep until this thread will actually be useful. */
    if (!con->conn->is_connected || !con->consumers) {
      pthread_mutex_unlock(&con->mutex);
      pthread_setcancelstate(PTHREAD_CANCEL_ENABLE, NULL);
      pthread_testcancel();
      continue;
    }

    /* TODO: Is this still safe? Does the callback rely on memory that is released here? */
    amqp_maybe_release_buffers(con->conn->conn);
    env = (amqp_envelope_t *) malloc(sizeof(amqp_envelope_t));
    reply = amqp_consume_message(con->conn->conn, env, &tv, 0);

    /* If the envelope contains a message, schedule a callback. Note that the callback
     * is responsible for releasing the memory of both (1) ptr; and (2) env. */
    if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
      ptr = (callback_data *) malloc(sizeof(callback_data));
      ptr->conn = con;
      ptr->env = env;
      later::later(later_callback, ptr, 0);
    } else if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
      int status = reply.library_error;

      /* If we get into an unexpected state, try to decode a relevant method
         (e.g. connection.close). */
      if (status == AMQP_STATUS_UNEXPECTED_STATE) {
        amqp_frame_t frame;
        status = amqp_simple_wait_frame(con->conn->conn, &frame);
        /* If the server shuts down gracefully, this is how we will probably be
           notified. */
        if (status == AMQP_STATUS_OK && frame.frame_type == AMQP_FRAME_METHOD &&
            frame.payload.method.id == AMQP_CONNECTION_CLOSE_METHOD) {
          status = AMQP_STATUS_CONNECTION_CLOSED;
        } else if (status == AMQP_STATUS_OK &&
                   frame.frame_type == AMQP_FRAME_METHOD &&
                   frame.payload.method.id == AMQP_BASIC_CANCEL_METHOD) {
          /* If we have consumer_cancel_notify enabled, this is how we are
             notified that e.g. deleted queues have cancelled a consumer. */
          amqp_basic_cancel_t *cancel;
          cancel = (amqp_basic_cancel_t *) frame.payload.method.decoded;
          bg_consumer *elt = con->consumers;
          while (elt && strncmp((const char *) elt->tag.bytes,
                                (const char *) cancel->consumer_tag.bytes,
                                elt->tag.len) != 0) {
            elt = elt->next;
          }
          if (!elt) {
            /* Ignore consumers we dont recognize, for now. */
            status = AMQP_STATUS_OK;
          } else {
            cdata = (struct bg_consumer_err_data *) malloc(sizeof(struct bg_consumer_err_data));
            cdata->kind = BG_ERR_CONSUMER_CANCEL;
            cdata->payload.tag = amqp_bytes_malloc_dup(cancel->consumer_tag);
            later::later(later_warn_callback, (void *) cdata, 0);

            /* Close the corresponding channel now so we don't try to during the
               finalizer. */
            reply = amqp_channel_close(con->conn->conn, elt->chan.chan,
                                       AMQP_REPLY_SUCCESS);
            if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
              amqp_maybe_release_buffers_on_channel(con->conn->conn,
                                                    elt->chan.chan);
              status = AMQP_STATUS_OK;
            } else if (reply.reply_type == AMQP_RESPONSE_LIBRARY_EXCEPTION) {
              status = reply.library_error;
            } else {
              /* Probably the server closed the connection. */
              status = AMQP_STATUS_UNEXPECTED_STATE;
            }
          }
        } else if (status == AMQP_STATUS_OK) {
          status = AMQP_STATUS_UNEXPECTED_STATE;
        } else {
          /* Act on whatever status amqp_simple_wait_frame() gave us. */
        }
      }

      /* Terminate the thread on connection errors and schedule a warning to be
         surfaced to the user at some point in the future. */
      switch (status) {
      case AMQP_STATUS_WRONG_METHOD:
        /* fallthrough */
      case AMQP_STATUS_UNEXPECTED_STATE:
        /* fallthrough */
      case AMQP_STATUS_CONNECTION_CLOSED:
        /* fallthrough */
      case AMQP_STATUS_SOCKET_CLOSED:
        /* fallthrough */
      case AMQP_STATUS_SOCKET_ERROR:
        con->conn->is_connected = 0;
        amqp_destroy_envelope(env);
        free(env);
        pthread_mutex_unlock(&con->mutex);
        cdata = (struct bg_consumer_err_data *) malloc(sizeof(struct bg_consumer_err_data));
        cdata->kind = BG_ERR_DISCONNECTED;
        later::later(later_warn_callback, (void *) cdata, 0);
        return NULL;
      case AMQP_STATUS_OK:
        /* fallthrough */
      case AMQP_STATUS_TIMEOUT:
        /* Nothing to consume right now. */
        amqp_destroy_envelope(env);
        free(env);
        break;
      default:
        /* Warn on other errors. */
        cdata = (struct bg_consumer_err_data *) malloc(sizeof(struct bg_consumer_err_data));
        cdata->kind = BG_ERR_UNEXPECTED_STATUS;
        cdata->payload.status = status;
        later::later(later_warn_callback, (void *) cdata, 0);
        amqp_destroy_envelope(env);
        free(env);
        break;
      }
    } else {
      /* FIXME: Can this ever happen? What should we do if it does? */
      amqp_destroy_envelope(env);
      free(env);
    }

    /* Allow the thread to be cancelled here. */
    pthread_mutex_unlock(&con->mutex);
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
  conn->consumers = NULL;
  conn->bg_conn = NULL;
  conn->is_connected = 0;
  conn->conn = NULL;

  return (connection *) conn;
}

extern "C" int init_bg_conn(connection *conn)
{
  bg_conn *con = conn->bg_conn;
  if (con) {
    /* If the connection has been marked as closed by the background thread, we
       need to clean up and start again. TODO: Do we need to lock the mutex here
       to safely check the connection state? */
    if (!con->conn->is_connected) {
      destroy_bg_conn(conn->bg_conn);
    } else {
      return 0;
    }
  }

  bg_conn *out = (bg_conn *) malloc(sizeof(bg_conn));

  /* Need to do this before pthread_create() to avoid a data race on
     fields in out. */
  out->conn = clone_connection(conn);
  out->mutex = PTHREAD_MUTEX_INITIALIZER;
  out->consumers = NULL;

  int res = pthread_create(&out->thread, NULL, consume_run, out);
  if (res != 0) {
    amqp_destroy_connection(out->conn->conn);
    free(out->conn);
    free(out);
    return res;
  }

  conn->bg_conn = out;
  return 0;
}

extern "C" void destroy_bg_conn(bg_conn *conn)
{
  if (!conn) return;

  if (pthread_cancel(conn->thread) == 0) {
    /* No point in checking the result. */
    pthread_join(conn->thread, NULL);
  }
  pthread_mutex_destroy(&conn->mutex);

  /* Ensure all consumers attached to this thread know that the connection is
     dead. Instead of cleaning up consumers, rely on their finalizers to run. */

  bg_consumer *next, *elt = conn->consumers;
  while (elt) {
    next = elt->next;
    elt->conn = NULL;
    elt->chan.is_open = 0;
    elt->prev = NULL;
    elt->next = NULL;
    elt = next;
  }

  /* Attempt to close the connection. */
  if (conn->conn->is_connected) {
    amqp_connection_close(conn->conn->conn, AMQP_REPLY_SUCCESS);
  }

  amqp_destroy_connection(conn->conn->conn);
  free(conn->conn);
  free(conn);
  conn = NULL;

  return;
}

extern "C" SEXP R_amqp_consume_later(SEXP ptr, SEXP queue, SEXP fun, SEXP rho,
                                     SEXP consumer, SEXP no_ack, SEXP exclusive,
                                     SEXP args)
{

  const char *queue_str = CHAR(Rf_asChar(queue));
  const char *consumer_str = CHAR(Rf_asChar(consumer));
  int has_no_ack = Rf_asLogical(no_ack);
  int is_exclusive = Rf_asLogical(exclusive);
  amqp_table_t *arg_table = (amqp_table_t *) R_ExternalPtrAddr(args);

  connection *conn = (connection *) R_ExternalPtrAddr(ptr);
  int res = init_bg_conn(conn);
  if (res != 0) {
    Rf_error("Failed to create background thread. Error: %d.", res);
  }
  bg_conn *bg_conn = conn->bg_conn;

  pthread_mutex_lock(&bg_conn->mutex);

  bg_consumer *con = (bg_consumer *) malloc(sizeof(bg_consumer));
  con->conn = bg_conn;
  con->chan.chan = 0;
  con->chan.is_open = 0;
  con->tag = amqp_empty_bytes;
  char errbuff[1000];
  if (lconnect(bg_conn->conn, errbuff, 1000) < 0 ||
      ensure_valid_channel(bg_conn->conn, &con->chan, errbuff, 1000) < 0) {
    Rf_error("Failed to clone connection. %s", errbuff);
    return R_NilValue;
  }

  con->no_ack = has_no_ack;
  con->fun = fun;
  con->rho = rho;
  con->prev = NULL;
  con->next = NULL;

  amqp_basic_consume_ok_t *consume_ok;
  consume_ok = amqp_basic_consume(bg_conn->conn->conn, con->chan.chan,
                                  amqp_cstring_bytes(queue_str),
                                  amqp_cstring_bytes(consumer_str),
                                  0, has_no_ack, is_exclusive,
                                  *arg_table);

  if (consume_ok == NULL) {
    amqp_rpc_reply_t reply = amqp_get_rpc_reply(bg_conn->conn->conn);
    render_amqp_error(reply, bg_conn->conn, &con->chan, errbuff, 1000);

    /* Clean up. */
    if (con->chan.is_open) {
      amqp_channel_close(bg_conn->conn->conn, con->chan.chan,
                         AMQP_REPLY_SUCCESS);
    }
    free(con);

    Rf_error("Failed to start a queue consumer. %s", errbuff);
  }

  if (!has_no_ack) {
    amqp_basic_qos_ok_t *qos_ok = amqp_basic_qos(bg_conn->conn->conn,
                                                 con->chan.chan, 0,
                                                 DEFAULT_PREFETCH_COUNT, 0);
    if (qos_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(bg_conn->conn->conn);
      render_amqp_error(reply, bg_conn->conn, &con->chan, errbuff, 1000);

      /* Clean up. */
      if (con->chan.is_open) {
        amqp_channel_close(bg_conn->conn->conn, con->chan.chan,
                           AMQP_REPLY_SUCCESS);
      }
      free(con);

      Rf_error("Failed to set quality of service. %s", errbuff);
    }
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

  /* Add it to the global list of consumers. */
  if (!bg_conn->consumers) {
    bg_conn->consumers = con;
  } else {
    bg_consumer *elt = bg_conn->consumers;
    while (elt->next) {
      elt = elt->next;
    }
    elt->next = con;
    con->prev = elt;
  }

  pthread_mutex_unlock(&bg_conn->mutex);
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
