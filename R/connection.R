#' Connections to a RabbitMQ Server
#'
#' @description
#'
#' Connect, disconnect, and reconnect to RabbitMQ servers. When possible, we
#' automatically recover from connection errors, so manual reconnection is not
#' usually necessary.
#'
#' For those familiar with the AMQP protocol: we manage channels internally, and
#' automatically recover from channel-level errors.
#'
#' @param host The server host.
#' @param port The server port.
#' @param vhost The desired virtual host.
#' @param username User credentials.
#' @param password User credentials.
#' @param timeout A timeout, in seconds, for operations that support it.
#' @param name A name for the connection that may appear in supported interfaces.
#'
#' @return An \code{amqp_connection} object.
#'
#' @examples
#' \dontrun{
#' conn <- amqp_connect(password = "wrong")
#' conn <- amqp_connect()
#' amqp_disconnect(conn)
#' }
#'
#' @name amqp_connections
#' @export
amqp_connect <- function(host = "localhost", port = 5672L, vhost = "/",
                         username = "guest", password = "guest",
                         timeout = 10L, name = "longears") {
  AmqpConnection$new(
    host = host, port = port, vhost = vhost, username = username,
    password = password, timeout = timeout, name = name
  )
}

format_fields <- function(fields, prefix = "") {
  len <- nchar(names(fields))
  longest <- max(len)
  buffer <- strrep(" ", times = longest - len + 1)

  paste(prefix, names(fields), ":", buffer, fields, sep = "", collapse = "\n")
}

#' @param x An object returned by \code{\link{amqp_connect}}.
#' @param full When \code{TRUE}, print all server and client properties instead
#'   of a brief summary.
#' @param ... Ignored.
#'
#' @rdname amqp_connections
#' @export
print.amqp_connection <- function(x, full = FALSE, ...) {
  x$print(full = full, ...)
}

#' @param conn An object returned by \code{\link{amqp_connect}}.
#'
#' @rdname amqp_connections
#' @export
amqp_reconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  conn$reconnect()
  invisible(conn)
}

#' @rdname amqp_connections
#' @export
amqp_disconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  conn$disconnect()
  invisible(conn)
}
