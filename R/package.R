#' @useDynLib longears, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' Open a Connection to a RabbitMQ Server
#'
#' @param host The server host.
#' @param port The server port.
#'
#' @return An \code{amqp_connection} object.
#'
#' @export
amqp_connect <- function(host = "localhost", port = 5672L, vhost = "/",
                         username = "guest", password = "guest",
                         timeout = 10L) {
  conn <- amqp_connect_(host, port, vhost, username, password, timeout)
  structure(list(ptr = conn, host = host, port = port, vhost = vhost),
            class = "amqp_connection")
}

#' @export
print.amqp_connection <- function(x, ...) {
  cat(sep = "", "AMQP Connection:\n",
      "  status:  ", ifelse(is_connected(x$ptr), "connected\n",
                           "disconnected\n"),
      "  address: ", x$host, ":", x$port, "\n",
      "  vhost:   '", x$vhost, "'\n")
}

#' Close a Connection to a RabbitMQ Server
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#'
#' @export
amqp_disconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  amqp_disconnect_(conn$ptr)
  invisible(conn)
}

#' Declare a Queue
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of a queue. If this is empty (the default), the server
#'   will generate a random name for the queue itself.
#' @param passive
#' @param durable
#' @param exclusive
#' @param auto_delete
#'
#' @return An object of class \code{amqp_queue}.
#'
#' @export
amqp_declare_queue <- function(conn, queue = "", passive = FALSE,
                               durable = FALSE, exclusive = FALSE,
                               auto_delete = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  amqp_declare_queue_(conn$ptr, queue, passive, durable, exclusive, auto_delete)
}

#' Declare a Temporary Queue
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param passive
#' @param exclusive
#'
#' @return The name of the temporary queue.
#'
#' @export
amqp_declare_tmp_queue <- function(conn, passive = FALSE, exclusive = TRUE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  queue <- amqp_declare_queue_(conn$ptr, queue = "", passive = passive,
                               durable = FALSE, exclusive = exclusive,
                               auto_delete = TRUE)
  queue$queue
}

#' @export
print.amqp_queue <- function(x, ...) {
  cat(sep = "", "AMQP queue '", x$queue, "'\n",
      "  messages:  ", x$message_count, "\n",
      "  consumers: ", x$consumer_count, "\n")
}

#' Publish a Message to a Queue
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param routing_key The routing key for the message. For the default
#'   exchange, this is the name of a queue.
#' @param body The message to send.
#' @param exchange The exchange to route the message through.
#' @param content_type The message content type.
#' @param mandatory When \code{TRUE}, demand that the message is placed in a
#'   queue.
#' @param immediate When \code{TRUE}, demand that the message is delivered
#'   immediately.
#'
#' @export
amqp_publish <- function(conn, routing_key, body, exchange = "",
                         content_type = "text/plain", mandatory = FALSE,
                         immediate = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  amqp_publish_(conn$ptr, routing_key, body, exchange, content_type, mandatory,
                immediate)
}

#' Get a Message from a Queue
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of a queue.
#'
#' @return A string containing the message, or a zero-length character vector
#'   if there is no message in the queue.
#'
#' @export
amqp_get <- function(conn, queue) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  amqp_get_(conn$ptr, queue)
}
