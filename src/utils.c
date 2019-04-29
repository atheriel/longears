#include <stdio.h> /* for snprintf */

#include "connection.h"
#include "utils.h"

void render_amqp_error(const amqp_rpc_reply_t reply, connection *conn,
                       char *buffer, size_t len)
{
  // This is mostly ported from rabbitmq-c/examples/utils.c.
  switch (reply.reply_type) {
  case AMQP_RESPONSE_NONE:
    snprintf(buffer, len, "RPC reply is missing.");
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    snprintf(buffer, len, "Library error: %s",
             amqp_error_string2(reply.library_error));
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
      conn->chan.is_open = 0;
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
