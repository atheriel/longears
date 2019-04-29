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
  invisible(.Call(
    R_amqp_publish, conn$ptr, routing_key, body, exchange, content_type,
    mandatory, immediate
  ))
}

#' Get a Message from a Queue
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of a queue.
#'
#' @return A string containing the message, or a zero-length character vector if
#'   there is no message in the queue. Messages may have additional properties
#'   (such as the content type) attached to them as attributes.
#'
#' @export
amqp_get <- function(conn, queue, no_ack = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  .Call(R_amqp_get, conn$ptr, queue, no_ack)
}
