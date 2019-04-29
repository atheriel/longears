#' Declare or Delete Queues
#'
#' @description
#'
#' AMQP queues store messages that can be consumed by clients. They must be
#' declared before use and \link[=amqp_bindings]{bound} to an
#' \link[=amqp_exchanges]{exchange} so that messages can be routed to them.
#'
#' The \code{amqp_declare_tmp_queue()} function is a shortcut for declaring a
#' non-durable (also called "transient") queue with a server-generated name.
#'
#' Both \code{amqp_declare_queue()} and \code{amqp_delete_queue()} will raise
#' errors if there is a problem declaring/deleting the queue.
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of a queue. If this is empty (the default), the server
#'   will generate a random name for the queue itself.
#' @param passive When \code{TRUE}, raise an error if the queue does not already
#'   exist.
#' @param durable When \code{TRUE}, the queue will persist between server
#'   restarts.
#' @param exclusive When \code{TRUE}, the queue will only be accessible to the
#'   current connection, and will be deleted when that connection closes.
#' @param auto_delete When \code{TRUE}, the queue is automatically deleted when
#'   all consumers have finished with it (i.e. their connections have closed).
#'   This does not come into effect until the queue has at least one consumer.
#'
#' @return \code{amqp_declare_tmp_queue()} will return the name of the new,
#'   temporary queue, while \code{amqp_declare_queue()} will return an object
#'   containing some additional information.
#'
#' @examples
#' \dontrun{
#' conn <- amqp_connect()
#' amqp_declare_queue(conn, "test.queue", auto_delete = TRUE)
#' amqp_delete_queue(conn, "test.queue")
#' amqp_disconnect(conn)
#' }
#'
#' @name amqp_queues
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

#' @rdname amqp_queues
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

#' @param if_unused Delete the queue only if it is unused.
#' @param if_empty Delete the queue only if it is empty.
#'
#' @return \code{amqp_delete_queue()} will return the number of messages in the
#'   queue when it was deleted, invisibly.
#'
#' @name amqp_queues
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
