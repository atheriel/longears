#' Bind Queues to Exchanges, or Exchanges to Other Exchanges
#'
#' In order for messages to be routed to a queue, the queue must be bound to
#' the exchange, although the nature of this binding depends on the type of
#' exchange. Exchange-to-exchange bindings can be used to construct complex
#' routing topologies.
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of the queue.
#' @param exchange The exchange to source messages from.
#' @param routing_key The routing key to use (if applicable).
#' @param ... Binding arguments, if any.
#'
#' @aliases amqp_bindings
#' @name amqp_bindings
#' @export
amqp_bind_queue <- function(conn, queue, exchange, routing_key = "", ...) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  args <- amqp_table(...)
  invisible(.Call(
    R_amqp_bind_queue, conn$ptr, queue, exchange, routing_key, args$ptr
  ))
}

#' @rdname amqp_bindings
#' @export
amqp_unbind_queue <- function(conn, queue, exchange, routing_key = "", ...) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  args <- amqp_table(...)
  invisible(.Call(
    R_amqp_unbind_queue, conn$ptr, queue, exchange, routing_key, args$ptr
  ))
}

#' @param dest The exchange to send messages to.
#'
#' @rdname amqp_bindings
#' @export
amqp_bind_exchange <- function(conn, dest, exchange, routing_key = "", ...) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  args <- amqp_table(...)
  invisible(.Call(
    R_amqp_bind_exchange, conn$ptr, dest, exchange, routing_key, args$ptr
  ))
}

#' @rdname amqp_bindings
#' @export
amqp_unbind_exchange <- function(conn, dest, exchange, routing_key = "", ...) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  args <- amqp_table(...)
  invisible(.Call(
    R_amqp_unbind_exchange, conn$ptr, dest, exchange, routing_key, args$ptr
  ))
}
