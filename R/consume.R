#' @export
amqp_create_consumer <- function(conn, queue, consumer = "", no_ack = FALSE,
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
amqp_destroy_consumer <- function(consumer) {
  if (!inherits(consumer, "amqp_consumer")) {
    stop("`consumer` is not an amqp_consumer object")
  }
  invisible(.Call(R_amqp_destroy_consumer, consumer))
}

#' @export
amqp_listen <- function(conn, fun, timeout = 10) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_listen, conn$ptr, timeout))
}
