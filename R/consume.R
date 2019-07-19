#' Consume Messages from a Queue
#'
#' @description
#'
#' Start or cancel a \strong{Consumer} for a given queue. Consumers attach a
#' callback function to the queue that is run once for each message received
#' (until it is cancelled). Any number of consumers can be started on a given
#' connection.
#'
#' Because R is single-threaded, you must call \code{amqp_listen()} to actually
#' receive and process messages. As an alternative, you can consume messages on
#' a background thread by using \code{\link{amqp_consume_later}}.
#'
#' @inheritParams amqp_get
#' @param fun A function taking a single parameter, the message received. This
#'   function is executed by \code{amqp_listen()} whenever messages are
#'   received on the queue.
#' @param tag An optional "tag" to identify the consumer. When empty, the
#'   server will generate one automatically.
#' @param exclusive When \code{TRUE}, request that this consumer has exclusive
#'   access to the queue.
#' @param ... Additional arguments, used to declare broker-specific AMQP
#'   extensions. See \strong{Details}.
#'
#' @details
#'
#' Additional arguments can be used to declare broker-specific extensions. An
#' incomplete list is as follows:
#'
#' \describe{
#'
#'   \item{\code{"x-priority"}}{Specify a consumer
#'     \href{https://www.rabbitmq.com/consumer-priority.html}{priority}.}
#'
#' }
#'
#' @return
#'
#' \code{amqp_consume} returns an \code{"amqp_consumer"} object, which can
#' later be used to cancel the consumer. Keep in mind that if you do not assign
#' the result of this function to a variable, you will have no way of
#' cancelling the consumer directly -- instead, you will be relying on
#' \code{\link[base]{gc}} to take care of this at some indeterminant point in
#' the future.
#'
#' @details
#'
#' Unless \code{no_ack} is \code{TRUE}, messages are acknowledged automatically
#' after the callback executes.
#'
#' @examples
#' \dontrun{
#' # Create a consumer.
#' conn <- amqp_connect()
#' queue <- amqp_declare_tmp_queue(conn)
#' consumer <- amqp_consume(conn, queue, function(msg) {
#'   print(msg)
#' })
#'
#' # Publish and then listen for a message.
#' amqp_publish(conn, "Hello, world.", routing_key = queue)
#' amqp_listen(conn, timeout = 1)
#'
#' # Clean up.
#' amqp_cancel_consumer(consumer)
#' amqp_disconnect(conn)
#' }
#'
#' @seealso
#'
#' \code{\link{amqp_get}} to get messages individually or
#' \code{\link{amqp_consume_later}} to consume messages in a background thread.
#'
#' @export
amqp_consume <- function(conn, queue, fun, tag = "", no_ack = FALSE,
                         exclusive = FALSE, ...) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  stopifnot(is.function(fun))
  args <- amqp_table(...)
  .Call(
    R_amqp_create_consumer, conn$ptr, queue, tag, fun, new.env(), no_ack,
    exclusive, args$ptr
  )
}

#' @param consumer An object created by \code{\link{amqp_consume}}.
#'
#' @rdname amqp_consume
#' @export
amqp_cancel_consumer <- function(consumer) {
  if (inherits(consumer, "amqp_consumer")) {
    invisible(.Call(R_amqp_destroy_consumer, consumer))
  } else if (inherits(consumer, "amqp_bg_consumer")) {
    invisible(.Call(R_amqp_destroy_bg_consumer, consumer$ptr))
  } else {
    stop("`consumer` is not an amqp_consumer object")
  }
}

#' @param timeout Maximum number of seconds to wait for messages. Capped at 60.
#'
#' @rdname amqp_consume
#' @export
amqp_listen <- function(conn, timeout = 10L) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_listen, conn$ptr, timeout))
}

#' Consume Messages from a Queue, Later
#'
#' @description
#'
#' Consume messages "asynchronously" by using the machinery of the
#' \strong{\link[later]{later}} package. This function is primarily for use
#' inside applications (particularly Shiny applications) that already make use
#' of \strong{later} to manage events.
#'
#' This interface is experimental and should be used with caution, since any
#' bugs in the implementation have the potential to cause serious memory
#' corruption issues that will terminate the R process.
#'
#' @inheritParams amqp_consume
#' @param conn An object returned by \code{\link{amqp_connect}}, but see
#'   \strong{Details}.
#' @param fun A function taking a single parameter, the message received. This
#'   function is executed by \code{\link[later]{later}} whenever messages are
#'   received on the queue.
#'
#' @details
#'
#' An \code{amqp_connection} object will start a "background thread" for these
#' consumers if any are declared. Because underlying components of the
#' \code{amqp_connection} object are not thread-safe, this background thread
#' creates a "clone" of the original connection using the same properties. This
#' may lead to some surprising results, including the fact that consumers
#' created with this interface will not stop running if the original connection
#' is closed with \code{amqp_disconnect} or due to connection-level user errors.
#' This may change in future versions.
#'
#' At present, consumers can only be cancelled by using
#' \code{\link{amqp_cancel_consumer}} or by garbage collection when the original
#' connection object expires.
#'
#' @seealso \code{\link{amqp_consume}} to consume messages in the main thread.
#' @export
#' @import later
amqp_consume_later <- function(conn, queue, fun, tag = "", no_ack = FALSE,
                               exclusive = FALSE, ...) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  args <- amqp_table(...)
  .Call(
    R_amqp_consume_later, conn$ptr, queue, fun, new.env(), tag, no_ack,
    exclusive, args$ptr
  )
}
