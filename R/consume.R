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
#' receive and process messages.
#'
#' @inheritParams amqp_get
#' @param fun A function taking a single parameter, the message received. This
#'   function is executed by \code{amqp_listen()} whenever messages are
#'   received on the queue.
#' @param tag An optional "tag" to identify the consumer. When empty, the
#'   server will generate one automatically.
#' @param exclusive When \code{TRUE}, request that this consumer has exclusive
#'   access to the queue.
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
#' @seealso \code{\link{amqp_get}} to get messages individually.
#' @export
amqp_consume <- function(conn, queue, fun, tag = "", no_ack = FALSE,
                         exclusive = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  stopifnot(is.function(fun))
  .Call(
    R_amqp_create_consumer, conn$ptr, queue, tag, fun, new.env(), no_ack,
    exclusive
  )
}

#' @param consumer An object created by \code{\link{amqp_consume}}.
#'
#' @rdname amqp_consume
#' @export
amqp_cancel_consumer <- function(consumer) {
  if (!inherits(consumer, "amqp_consumer")) {
    stop("`consumer` is not an amqp_consumer object")
  }
  invisible(.Call(R_amqp_destroy_consumer, consumer))
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

#' @import later
NULL
