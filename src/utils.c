#include "utils.h"

void handle_amqp_error(const char *ctxt, amqp_rpc_reply_t reply)
{
  // This is mostly ported from rabbitmq-c/examples/utils.c.
  switch (reply.reply_type) {
  case AMQP_RESPONSE_NONE:
    Rf_error("%s RPC reply is missing.", ctxt);
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    Rf_error("%s Library error: %s", ctxt,
             amqp_error_string2(reply.library_error));
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (reply.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *method;
      method = (amqp_connection_close_t *) reply.reply.decoded;
      Rf_error("%s Server connection error: %s. Disconnected.", ctxt,
               (char *) method->reply_text.bytes);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *method;
      method = (amqp_channel_close_t *) reply.reply.decoded;
      Rf_error("%s Server channel error: %s. Open a new channel.", ctxt,
               (char *) method->reply_text.bytes);
      break;
    }
    default:
      Rf_error("%s Unexpected server error: %s.", ctxt,
               amqp_method_name(reply.reply.id));
      break;
    }
    break;

  default:
    // This should never happen.
    Rf_error("%s Unknown reply type: %d.", ctxt, reply.reply_type);
    break;
  }
}
