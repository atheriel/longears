#' Publish a Message to an Exchange
#'
#' Publishes a message to an exchange with a given routing key.
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param body The message to send.
#' @param exchange The exchange to route the message through.
#' @param routing_key The routing key for the message. For the default exchange,
#'   this is the name of a queue.
#' @param mandatory When \code{TRUE}, demand that the message is placed in a
#'   queue.
#' @param immediate When \code{TRUE}, demand that the message is delivered
#'   immediately.
#' @param properties Message properties created with
#'   \code{\link{amqp_properties}}, or \code{NULL} to attach no properties to
#'   the message.
#'
#' @export
amqp_publish <- function(conn, body, exchange = "", routing_key = "",
                         mandatory = FALSE, immediate = FALSE,
                         properties = NULL) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  props <- if (inherits(properties, "amqp_properties")) {
    properties$ptr
  } else {
    NULL
  }
  invisible(.Call(
    R_amqp_publish, conn$ptr, body, exchange, routing_key, mandatory,
    immediate, props
  ))
}

#' Get a Message from a Queue
#'
#' Get a message from a given queue.
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param queue The name of a queue.
#' @param no_ack When \code{TRUE}, do not acknowldge receipt of the message.
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

#' @export
print.amqp_message <- function(x, ...) {
  # Turn all properties into HTTP-style "headers".
  headers <- x[setdiff(names(x), c("body", "properties", "class"))]
  if (!is.null(x$properties)) {
    headers <- c(headers, as.list(x$properties))
  }
  names(headers) <- gsub("_", " ", names(headers), fixed = TRUE)

  # Determine \t spacing.
  len <- (nchar(names(headers)) + 1) %/% 8
  len <- (max(len) - len) + 1
  buffer <- strrep("\t", times = len)

  header <- paste(
    tools::toTitleCase(names(headers)), ":", buffer, headers,
    sep = "", collapse = "\n"
  )
  cat(header, paste0(x$body, collapse = " "), sep = "\n")
}

#' @export
as.data.frame.amqp_message <- function(x, row.names = NULL, optional = FALSE,
                                       ...) {
  out <- c(x[setdiff(names(x), c("body", "properties"))], as.list(x$properties))
  # Put the raw body vector is in a list column.
  out$body <- list(x$body)
  # Note: We construct the object directly here for performance.
  structure(out, class = c("tbl_df", "tbl", "data.frame"), row.names = 1L)
}

#' Acknowledge or Reject Incoming Messages
#'
#' Notify the server that a message (or a series of messages) have been received
#' by acknowledging (ack-ing) them. Or, reject incoming messages that you cannot
#' handle correctly by "nack"-ing them.
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param delivery_tag The message's numeric identifier.
#' @param multiple When \code{TRUE}, (n)ack messages up-to-and-including this
#'   \code{delivery_tag}. By default, we only (n)ack a single message.
#'
#' @noRd
amqp_ack <- function(conn, delivery_tag, multiple = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_ack, conn$ptr, delivery_tag, multiple))
}

#' @param requeue When \code{TRUE}, ask the server to requeue the message.
#'   Otherwise, messages are discarded or dead-lettered.
#'
#' @noRd
amqp_nack <- function(conn, delivery_tag, multiple = FALSE, requeue = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_ack, conn$ptr, delivery_tag, multiple))
}
