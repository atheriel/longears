#include "longears_types.h"

// Error handling.
void stop_on_rpc_error(const char *context, amqp_rpc_reply_t reply);

class AmqpConnection {

private:

  amqp_connection_state_t conn; // librabbitmq connection object.

public:

  void connect(std::string host = "localhost", int port = 5672, std::string vhost = "/",
               std::string username = "guest", std::string password = "guest", long timeout = 10) {
    conn = amqp_new_connection();

    if (!conn) {
      Rcpp::stop("Failed to create an amqp connection.");
    }

    amqp_socket_t *socket = amqp_tcp_socket_new(conn);

    if (!socket) {
      amqp_destroy_connection(conn);
      Rcpp::stop("Failed to create an amqp socket.");
    }

    // TODO: It should be possible to set an infinite timeout.
    timeval *tv = new timeval;
    tv->tv_sec = timeout;
    tv->tv_usec = 0;
    int sockfd = amqp_socket_open_noblock(socket, host.c_str(), port, tv);
    if (sockfd < 0) {
      amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
      amqp_destroy_connection(conn);
      if (sockfd == AMQP_STATUS_SOCKET_ERROR) { // This has an unhelpful error message in this case.
        Rcpp::stop("Failed to open amqp socket. Is the server running?");
      } else {
        Rcpp::stop("Failed to open amqp socket. Error: %d.", amqp_error_string2(sockfd));
      }
    }

    /* Log in. */

    amqp_rpc_reply_t login_reply = amqp_login(conn, vhost.c_str(), AMQP_DEFAULT_MAX_CHANNELS, AMQP_DEFAULT_FRAME_SIZE,
                                              0, AMQP_SASL_METHOD_PLAIN, username.c_str(), password.c_str());

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

  void disconnect() {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }
    stop_on_rpc_error("Failed to disconnect.", amqp_connection_close(conn, AMQP_REPLY_SUCCESS));
    amqp_destroy_connection(conn);
    conn = NULL;
  }

  bool is_connected() {
    return (conn != NULL);
  }

  void declare_exchange(std::string exchange, std::string type, bool passive = false, bool durable = false,
                        bool auto_delete = false, bool internal = false) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Declare exchange. */

    amqp_exchange_declare_ok_t *exch_ok = amqp_exchange_declare(conn, 1, amqp_cstring_bytes(exchange.c_str()),
                                                                amqp_cstring_bytes(type.c_str()), passive, durable,
                                                                auto_delete, internal, amqp_empty_table);

    if (exch_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: exchange declare response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to declare exchange.", reply);
      }
    }
  }

  void delete_exchange(std::string exchange, bool if_unused = false) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Delete exchange. */

    amqp_exchange_delete_ok_t *delete_ok = amqp_exchange_delete(conn, 1, amqp_cstring_bytes(exchange.c_str()),
                                                                if_unused);

    if (delete_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: exchange delete response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to delete exchange.", reply);
      }
    }
  }

  Rcpp::List declare_queue(std::string queue, bool passive = false, bool durable = false, bool exclusive = false,
                           bool auto_delete = false) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Declare queue. */

    amqp_bytes_t qname = amqp_empty_bytes;
    if (!queue.empty()) {
      qname = amqp_cstring_bytes(queue.c_str());
    }

    amqp_queue_declare_ok_t *queue_ok = amqp_queue_declare(conn, 1, qname, passive, durable, exclusive, auto_delete,
                                                           amqp_empty_table);

    if (queue_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: queue declare response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to declare queue.", reply);
      }
    }

    // TODO: Is this copy costly? Can it be avoided?
    std::string name;
    if (!queue.empty()) {
      name = queue;
    } else {
      name = std::string((char *) queue_ok->queue.bytes, queue_ok->queue.len);
    }

    int message_count = queue_ok->message_count;
    int consumer_count = queue_ok->consumer_count;

    Rcpp::List rval = Rcpp::List::create(Rcpp::Named("queue") = name, Rcpp::Named("message_count") = message_count,
                                         Rcpp::Named("consumer_count") = consumer_count);
    rval.attr("class") = "amqp_queue";
    return rval;
  }

  int delete_queue(std::string queue, bool if_unused = false, bool if_empty = false) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Delete queue. */

    amqp_bytes_t qname = amqp_cstring_bytes(queue.c_str());
    amqp_queue_delete_ok_t *delete_ok = amqp_queue_delete(conn, 1, qname, if_unused, if_empty);

    if (delete_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: queue delete response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to delete queue.", reply);
      }
    }

    int message_count = delete_ok->message_count;
    return message_count;
  }

  void bind_queue(std::string queue, std::string exchange, std::string routing_key = "") {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Bind queue to exchange. */

    amqp_queue_bind_ok_t *bind_ok = amqp_queue_bind(conn, 1, amqp_cstring_bytes(queue.c_str()),
                                                    amqp_cstring_bytes(exchange.c_str()),
                                                    amqp_cstring_bytes(routing_key.c_str()),
                                                    amqp_empty_table);

    if (bind_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: queue bind response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to bind queue.", reply);
      }
    }
  }

  void unbind_queue(std::string queue, std::string exchange, std::string routing_key = "") {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Unbind queue from exchange. */

    amqp_queue_unbind_ok_t *unbind_ok = amqp_queue_unbind(conn, 1, amqp_cstring_bytes(queue.c_str()),
                                                          amqp_cstring_bytes(exchange.c_str()),
                                                          amqp_cstring_bytes(routing_key.c_str()),
                                                          amqp_empty_table);

    if (unbind_ok == NULL) {
      amqp_rpc_reply_t reply = amqp_get_rpc_reply(conn);
      if (reply.reply_type == AMQP_RESPONSE_NORMAL) {
        // This should never happen.
        Rcpp::stop("Unexpected error: queue unbind response is NULL with a normal reply.");
      } else {
        stop_on_rpc_error("Failed to unbind queue.", reply);
      }
    }
  }

  void publish(std::string routing_key, std::string body, std::string exchange = "",
               std::string content_type = "text/plain", bool mandatory = false, bool immediate = false) {
    if (!conn) {
      Rcpp::stop("The amqp connection no longer exists.");
    }

    /* Send message. */

    amqp_basic_properties_t props;
    props._flags = AMQP_BASIC_CONTENT_TYPE_FLAG |
      AMQP_BASIC_DELIVERY_MODE_FLAG;
    props.content_type = amqp_cstring_bytes(content_type.c_str());
    props.delivery_mode = 1;
    amqp_bytes_t exch = amqp_empty_bytes;
    if (!exchange.empty()) {
      exch = amqp_cstring_bytes(exchange.c_str());
    }

    int result = amqp_basic_publish(conn, 1, exch, amqp_cstring_bytes(routing_key.c_str()), mandatory, immediate,
                                    &props, amqp_cstring_bytes(body.c_str()));

    if (result != AMQP_STATUS_OK) {
      Rcpp::stop("Failed to publish message. Error: %s.", amqp_error_string2(result));
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

  AmqpConnection(std::string host = "localhost", int port = 5672, std::string vhost = "/",
                 std::string username = "guest", std::string password = "guest", long timeout = 10) {
    connect(host, port, vhost, username, password, timeout);
  }

  ~AmqpConnection() {
    // TODO: Does it matter if we close the connection?
    // amqp_connection_close(conn, AMQP_REPLY_SUCCESS);
    amqp_destroy_connection(conn);
    conn = NULL;
  }
};

// [[Rcpp::export]]
Rcpp::XPtr<AmqpConnection> amqp_connect_(std::string host = "localhost", int port = 5672, std::string vhost = "/",
                                         std::string username = "guest", std::string password = "guest",
                                         long timeout = 10) {
  AmqpConnection *conn = new AmqpConnection(host, port, vhost, username, password, timeout);
  return Rcpp::XPtr<AmqpConnection>(conn, true);
}

// [[Rcpp::export]]
void amqp_disconnect_(Rcpp::XPtr<AmqpConnection> conn) {
  conn->disconnect();
}

// [[Rcpp::export]]
bool is_connected(Rcpp::XPtr<AmqpConnection> conn) {
  return conn->is_connected();
}

// [[Rcpp::export]]
void amqp_declare_exchange_(Rcpp::XPtr<AmqpConnection> conn, std::string exchange, std::string type,
                            bool passive = false, bool durable = false, bool auto_delete = false,
                            bool internal = false) {
  return conn->declare_exchange(exchange, type, passive, durable, auto_delete, internal);
}

// [[Rcpp::export]]
void amqp_delete_exchange_(Rcpp::XPtr<AmqpConnection> conn, std::string exchange, bool if_unused = false) {
  return conn->delete_exchange(exchange, if_unused);
}

// [[Rcpp::export]]
Rcpp::List amqp_declare_queue_(Rcpp::XPtr<AmqpConnection> conn, std::string queue, bool passive = false,
                         bool durable = false, bool exclusive = false, bool auto_delete = false) {
  return conn->declare_queue(queue, passive, durable, exclusive, auto_delete);
}

// [[Rcpp::export]]
int amqp_delete_queue_(Rcpp::XPtr<AmqpConnection> conn, std::string queue, bool if_unused = false,
                       bool if_empty = false) {
  return conn->delete_queue(queue, if_unused, if_empty);
}

// [[Rcpp::export]]
void amqp_bind_queue_(Rcpp::XPtr<AmqpConnection> conn, std::string queue, std::string exchange,
                      std::string routing_key = "") {
  return conn->bind_queue(queue, exchange, routing_key);
}

// [[Rcpp::export]]
void amqp_unbind_queue_(Rcpp::XPtr<AmqpConnection> conn, std::string queue, std::string exchange,
                        std::string routing_key = "") {
  return conn->unbind_queue(queue, exchange, routing_key);
}

// [[Rcpp::export]]
void amqp_publish_(Rcpp::XPtr<AmqpConnection> conn, std::string routing_key, std::string body,
                   std::string exchange = "", std::string content_type = "text/plain", bool mandatory = false,
                   bool immediate = false) {
  conn->publish(routing_key, body, exchange, content_type, mandatory, immediate);
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
