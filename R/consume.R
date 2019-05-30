#' @export
amqp_consume <- function(conn, queue, fun, consumer = "", no_ack = FALSE,
                         exclusive = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  stopifnot(is.function(fun))
  .Call(
    R_amqp_create_consumer, conn$ptr, queue, consumer, fun, new.env(), no_ack,
    exclusive
  )
}

#' @export
amqp_cancel_consumer <- function(consumer) {
  if (!inherits(consumer, "amqp_consumer")) {
    stop("`consumer` is not an amqp_consumer object")
  }
  invisible(.Call(R_amqp_destroy_consumer, consumer))
}

#' @param timeout Maximum number of seconds to wait for messages. Capped at 60.
#'
#' @export
amqp_listen <- function(conn, timeout = 10L) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_listen, conn$ptr, timeout))
}
