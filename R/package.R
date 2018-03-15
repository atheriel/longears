#' @useDynLib longears, .registration = TRUE
#' @importFrom Rcpp sourceCpp
NULL

#' Open a Connection to a RabbitMQ Server
#'
#' @param host The server host.
#' @param port The server port.
#'
#' @return An external pointer to a connection.
#'
#' @export
amqp_connect <- function(host = "localhost", port = 5672L, vhost = "/",
                         username = "guest", password = "guest",
                         timeout = 10L) {
  amqp_connect_(host, port, vhost, username, password, timeout)
}

#' Close a Connection to a RabbitMQ Server
#'
#' @param conn An external pointer to a connection.
#'
#' @export
amqp_disconnect <- function(conn) {
  amqp_disconnect_(conn)
}

#' Declare a Queue
#'
#' @param conn An external pointer to a connection.
#' @param queue The name of a queue.
#' @param quietly Suppress queue information echo.
#'
#' @export
amqp_declare_queue <- function(conn, queue, quietly = FALSE) {
  amqp_declare_queue_(conn, queue, quietly)
}

#' Publish a Message to a Queue
#'
#' @param conn An external pointer to a connection.
#' @param queue The name of a queue.
#' @param msg The message to send.
#' @param content_type The message content type.
#'
#' @export
amqp_publish <- function(conn, queue, msg, content_type = "text/plain")  {
  amqp_publish_(conn, queue, msg, content_type)
}

#' Get a Message from a Queue
#'
#' @param conn An external pointer to a connection.
#' @param queue The name of a queue.
#'
#' @return A string containing the message, or a zero-length character vector
#'   if there is no message in the queue.
#'
#' @export
amqp_get <- function(conn, queue) {
  amqp_get_(conn, queue)
}
