AmqpConnection <- R6::R6Class(
  "amqp_connection",
  cloneable = TRUE,
  public = list(
    initialize = function(host = "localhost", port = 5672L, vhost = "/",
                          username = "guest", password = "guest",
                          timeout = 10L, name = "longears") {
      # We keep track of these for printing.
      private$host <- host
      private$port <- port
      private$vhost <- vhost
      private$ptr <- .Call(
        R_amqp_connect, host, port, vhost, username, password, timeout, name,
        PACKAGE = "longears"
      )

      # Cache the client/server props.
      private$cprops <- .Call(
        R_amqp_client_properties, private$ptr, PACKAGE = "longears"
      )
      # For some reason these are in the reverse order we'd expect.
      private$sprops <- rev(
        .Call(R_amqp_server_properties, private$ptr, PACKAGE = "longears")
      )
    },

    print = function(full = FALSE, ...) {
      header <- list(
        status = ifelse(private$is_connected(), "connected", "disconnected"),
        address = sprintf("%s:%s", private$host, private$port),
        vhost = sprintf("'%s'", private$vhost)
      )

      if (!full) {
        cat(sep = "", "AMQP Connection:\n", format_fields(header, "  "), "\n")
        return()
      }

      cprops <- private$cprops
      ccapabilities <- cprops$capabilities
      cprops$capabilities <- NULL

      sprops <- private$sprops
      scapabilities <- sprops$capabilities
      sprops$capabilities <- NULL

      cat(
        sep = "", "AMQP Connection:\n", format_fields(header, "  "), "\n",
        "  client properties:\n", format_fields(cprops, "    "), "\n",
        "  client capabilities:\n", format_fields(ccapabilities, "    "), "\n",
        "  server properties:\n", format_fields(sprops, "    "), "\n",
        "  server capabilities:\n", format_fields(scapabilities, "    "), "\n"
      )
    },

    reconnect = function() {
      .Call(R_amqp_reconnect, private$ptr, PACKAGE = "longears")
    },

    disconnect = function() {
      .Call(R_amqp_disconnect, private$ptr, PACKAGE = "longears")
    },

    declare_exchange = function(exchange, type = "direct", passive = FALSE,
                                durable = FALSE, auto_delete = FALSE,
                                internal = FALSE, ...) {
      args <- amqp_table(...)
      out <-.Call(
        R_amqp_declare_exchange, private$ptr, exchange, type, passive, durable,
        auto_delete, internal, args$ptr, PACKAGE = "longears"
      )
      invisible(out)
    },

    delete_exchange = function(exchange, if_unused = FALSE) {
      out <- .Call(
        R_amqp_delete_exchange, private$ptr, exchange, if_unused,
        PACKAGE = "longears"
      )
      invisible(out)
    },

    declare_queue = function(queue = "", passive = FALSE, durable = FALSE,
                             exclusive = FALSE, auto_delete = FALSE, ...) {
      args <- amqp_table(...)
      .Call(
        R_amqp_declare_queue, private$ptr, queue, passive, durable, exclusive,
        auto_delete, args$ptr, PACKAGE = "longears"
      )
    },

    delete_queue = function(queue, if_unused = FALSE, if_empty = FALSE) {
      out <- .Call(
        R_amqp_delete_queue, private$ptr, queue, if_unused, if_empty,
        PACKAGE = "longears"
      )
      invisible(out)
    },

    bind_queue = function(conn, queue, exchange, routing_key = "", ...) {
      args <- amqp_table(...)
      invisible(.Call(
        R_amqp_bind_queue, private$ptr, queue, exchange, routing_key, args$ptr,
        PACKAGE = "longears"
      ))
    },

    unbind_queue = function(queue, exchange, routing_key = "", ...) {
      args <- amqp_table(...)
      invisible(.Call(
        R_amqp_unbind_queue, private$ptr, queue, exchange, routing_key, args$ptr,
        PACKAGE = "longears"
      ))
    },

    bind_exchange = function(dest, exchange, routing_key = "", ...) {
      args <- amqp_table(...)
      invisible(.Call(
        R_amqp_bind_exchange, private$ptr, dest, exchange, routing_key, args$ptr,
        PACKAGE = "longears"
      ))
    },

    unbind_exchange = function(dest, exchange, routing_key = "", ...) {
      args <- amqp_table(...)
      invisible(.Call(
        R_amqp_unbind_exchange, private$ptr, dest, exchange, routing_key,
        args$ptr, PACKAGE = "longears"
      ))
    },

    publish = function(body, exchange = "", routing_key = "", mandatory = FALSE,
                       immediate = FALSE, properties = NULL) {
      if (is.character(body)) {
        body <- charToRaw(body)
      }
      props <- if (inherits(properties, "amqp_properties")) {
        properties$ptr
      } else {
        NULL
      }
      invisible(.Call(
        R_amqp_publish, private$ptr, body, exchange, routing_key, mandatory,
        immediate, props, PACKAGE = "longears"
      ))
    },

    get = function(queue, no_ack = FALSE) {
      .Call(R_amqp_get, private$ptr, queue, no_ack, PACKAGE = "longears")
    },

    consume = function(queue, fun, tag = "", no_ack = FALSE, exclusive = FALSE,
                       ...) {
      stopifnot(is.function(fun))
      args <- amqp_table(...)
      .Call(
        R_amqp_create_consumer, private$ptr, queue, tag, fun, new.env(), no_ack,
        exclusive, args$ptr, PACKAGE = "longears"
      )
    },

    cancel_consumer = function(consumer) {
      if (inherits(consumer, "amqp_consumer")) {
        out <- .Call(R_amqp_destroy_consumer, consumer, PACKAGE = "longears")
      } else if (inherits(consumer, "amqp_bg_consumer")) {
        out <- .Call(
          R_amqp_destroy_bg_consumer, consumer$ptr, PACKAGE = "longears"
        )
      } else {
        stop("`consumer` is not an amqp_consumer object")
      }
      invisible(out)
    },

    listen = function(timeout = 10L) {
      invisible(.Call(R_amqp_listen, private$ptr, timeout, PACKAGE = "longears"))
    },

    consume_later = function(queue, fun, tag = "", no_ack = FALSE,
                             exclusive = FALSE, ...) {
      args <- amqp_table(...)
      .Call(
        R_amqp_consume_later, private$ptr, queue, fun, new.env(), tag, no_ack,
        exclusive, args$ptr, PACKAGE = "longears"
      )
    }
  ),
  private = list(
    ptr = NULL, host = NULL, port = NULL, vhost = NULL, cprops = list(),
    sprops = list(),

    #' @description Check if the underlying connection is open, to our knowledge.
    #' @noRd
    is_connected = function() {
      .Call(R_amqp_is_connected, private$ptr, PACKAGE = "longears")
    }
  )
)
