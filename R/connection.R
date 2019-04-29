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
                         timeout = 10L) {
  conn <- .Call(R_amqp_connect, host, port, vhost, username, password, timeout)
  structure(list(ptr = conn, host = host, port = port, vhost = vhost),
            class = "amqp_connection")
}

is_connected <- function(conn) {
  .Call(R_amqp_is_connected, conn$ptr)
}

#' @export
print.amqp_connection <- function(x, ...) {
  cat(sep = "", "AMQP Connection:\n",
      "  status:  ", ifelse(is_connected(x), "connected\n", "disconnected\n"),
      "  address: ", x$host, ":", x$port, "\n",
      "  vhost:   '", x$vhost, "'\n")
}

#' @param conn An object returned by \code{\link{amqp_connect}}.
#'
#' @rdname amqp_connections
#' @export
amqp_reconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  .Call(R_amqp_reconnect, conn$ptr)
  invisible(conn)
}

#' @rdname amqp_connections
#' @export
amqp_disconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  .Call(R_amqp_disconnect, conn$ptr)
  invisible(conn)
}
