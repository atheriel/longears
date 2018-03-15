#include "longears_types.h"

// Error handling.
void stop_on_rpc_error(const char *context, amqp_rpc_reply_t reply);

class AmqpConnection {

private:

  amqp_connection_state_t conn; // librabbitmq connection object.

public:

  void connect(std::string host = "localhost", int port = 5672) {
    conn = amqp_new_connection();

    if (!conn) {
      Rcpp::stop("Failed create an amqp connection.");
    }

    amqp_socket_t *socket = amqp_tcp_socket_new(conn);

    if (!socket) {
      amqp_destroy_connection(conn);
      Rcpp::stop("Failed create an amqp socket. Error: %d", socket);
    }

    // TODO: Should we use amqp_default_connection_info instead?
    int sockfd = amqp_socket_open(socket, host.c_str(), port);
    if (sockfd < 0) {
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      Rcpp::stop("Failed open amqp socket. Error: %d", sockfd);
    }

    /* Log in. */

    // TODO: Should we use amqp_default_connection_info instead?
    amqp_rpc_reply_t login_reply = amqp_login(conn, "/", 0, AMQP_DEFAULT_FRAME_SIZE, 0,
                                              AMQP_SASL_METHOD_PLAIN, "guest",
                                              "guest");

    if (login_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      stop_on_rpc_error("Failed to log in.", login_reply);
    }

    /* Open a Channel. */

    amqp_channel_open(conn, 1);
    amqp_rpc_reply_t open_reply = amqp_get_rpc_reply(conn);
    if (open_reply.reply_type != AMQP_RESPONSE_NORMAL) {
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      stop_on_rpc_error("Failed to open channel.", open_reply);
    }
  }

  void declare_queue(std::string queue, bool quietly = false) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Declare queue. */

    amqp_queue_declare_ok_t *queue_ok = amqp_queue_declare(
      conn, 1, amqp_cstring_bytes(queue.c_str()), 0, 0, 0, 0, amqp_empty_table
    );

    if (queue_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: queue declare response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to declare queue.", reply);
      }
    }

    if (!quietly) {
      Rcpp::Rcout << "Queue messages: " << (uint32_t) queue_ok->message_count <<
        " consumers: " << (uint32_t) queue_ok->consumer_count << std::endl;
    }
  }

  void send_message(std::string queue, std::string msg, std::string content_type = "text/plain") {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Send message. */

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
      AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes(content_type.c_str());
    props.delivery_mode = 1;

    int result = amqp_basic_publish(
      conn, 1, amqp_empty_bytes, amqp_cstring_bytes(queue.c_str()),
      0, 0, &props, amqp_cstring_bytes(msg.c_str())
    );

    if (result != AMQP_STATUS_OK) {
      Rcpp::stop("Failed to publish message. Error: %d.", result);
    }

    // TODO: This is not enough to ensure that we have sent the message.
    stop_on_rpc_error("Failed to publish message.", amqp_get_rpc_reply(conn));
  }

  Rcpp::StringVector get(std::string queue) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Get message. */

    // TODO: Add the ability to acknowledge the message.
    amqp_rpc_reply_t reply = amqp_basic_get(conn, 1, amqp_cstring_bytes(queue.c_str()), 1);
    stop_on_rpc_error("Failed to get message.", reply);

    if (reply.reply.id == AMQP_BASIC_GET_EMPTY_METHOD) {
      // Rcpp::Rcout << "No messages to get." << std::endl;
      return Rcpp::StringVector(0);
    }

    amqp_frame_t frame;
    int result = amqp_simple_wait_frame(conn, &frame);
    if (result != AMQP_STATUS_OK) {
      Rcpp::stop("Failed to read frame. Error: %d.", result);
    }
    if (frame.frame_type != AMQP_FRAME_HEADER) {
      Rcpp::stop("Failed to read frame. Unexpected header type: %d.", frame.frame_type);
    }

    size_t body_remaining = frame.payload.properties.body_size;
    std::string body; // = std::string();
    while (body_remaining) {
      result = amqp_simple_wait_frame(conn, &frame);
      if (result != AMQP_STATUS_OK) {
        Rcpp::stop("Failed to wait for frame. Error: %d.", result);
      }
      body.append((char *) frame.payload.body_fragment.bytes, frame.payload.body_fragment.len);
      body_remaining -= frame.payload.body_fragment.len;
    }
    Rcpp::StringVector out(1);
    out[0] = body;
    return out;
  }

  AmqpConnection(std::string host = "localhost", int port = 5672) {
    connect(host, port);
  }

  ~AmqpConnection() {
    // TODO: Does it matter if we close the connection?
    // amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    conn = NULL;
  }
};

// [[Rcpp::export]]
Rcpp::XPtr<AmqpConnection> amqp_connect_(std::string host = "localhost", int port = 5672) {
  AmqpConnection *conn = new AmqpConnection(host, port);
  return Rcpp::XPtr<AmqpConnection>(conn, true);
}

// [[Rcpp::export]]
void amqp_declare_queue_(Rcpp::XPtr<AmqpConnection> conn, std::string queue, bool quietly = false) {
  conn->declare_queue(queue, quietly);
}

// [[Rcpp::export]]
void amqp_send_message_(Rcpp::XPtr<AmqpConnection> conn, std::string queue, std::string msg,
                        std::string content_type = "text/plain") {
  conn->send_message(queue, msg, content_type);
}

// [[Rcpp::export]]
Rcpp::StringVector amqp_get_(Rcpp::XPtr<AmqpConnection> conn, std::string queue) {
  return conn->get(queue);
}

void stop_on_rpc_error(const char *context, amqp_rpc_reply_t reply) {
  // This is mostly ported from rabbitmq-c/examples/utils.c.
  switch (reply.reply_type) {
  case AMQP_RESPONSE_NORMAL:
    return;

  case AMQP_RESPONSE_NONE:
    Rcpp::stop("%s RPC reply is missing.", context);
    break;

  case AMQP_RESPONSE_LIBRARY_EXCEPTION:
    Rcpp::stop("%s Library error: %s", context, amqp_error_string2(reply.library_error));
    break;

  case AMQP_RESPONSE_SERVER_EXCEPTION:
    switch (reply.reply.id) {
    case AMQP_CONNECTION_CLOSE_METHOD: {
      amqp_connection_close_t *m = (amqp_connection_close_t *) reply.reply.decoded;
      Rcpp::stop("%s Server connection error: %s.", context, (char *) m->reply_text.bytes);
      break;
    }
    case AMQP_CHANNEL_CLOSE_METHOD: {
      amqp_channel_close_t *m = (amqp_channel_close_t *) reply.reply.decoded;
      Rcpp::stop("%s Server channel error: %s.", context, (char *) m->reply_text.bytes);
      break;
    }
    default:
      Rcpp::stop("%s Unexpected server error: %s.", context, amqp_method_name(reply.reply.id));
      break;
    }
    break;

  default:
    // This should never happen.
    Rcpp::stop("%s Unknown reply type: %d.", context, reply.reply_type);
    break;
  }
}
