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
  .Call(
    R_amqp_declare_queue, conn$ptr, queue, passive, durable, exclusive,
    auto_delete
  )
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
  queue <- amqp_declare_queue(conn, queue = "", passive = passive,
                              durable = FALSE, exclusive = exclusive,
                              auto_delete = TRUE)
  queue$queue
}

#' Delete a Queue
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of a queue.
#' @param if_unused Delete the queue only if it is unused.
#' @param if_empty Delete the queue only if it is empty.
#'
#' @return The number of messages in the queue when it was deleted, invisibly.
#'
#' @export
amqp_delete_queue <- function(conn, queue, if_unused = FALSE, if_empty = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_delete_queue, conn$ptr, queue, if_unused, if_empty))
}

#' @export
print.amqp_queue <- function(x, ...) {
  cat(sep = "", "AMQP queue '", x$queue, "'\n",
      "  messages:  ", x$message_count, "\n",
      "  consumers: ", x$consumer_count, "\n")
}
