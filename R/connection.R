#' Open a Connection to a RabbitMQ Server
#'
#' @param host The server host.
#' @param port The server port.
#'
#' @return An \code{amqp_connection} object.
#'
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

#' Restablish a Connection to a RabbitMQ Server
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#'
#' @export
amqp_reconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  .Call(R_amqp_reconnect, conn$ptr)
  invisible(conn)
}

#' Close a Connection to a RabbitMQ Server
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#'
#' @export
amqp_disconnect <- function(conn) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  .Call(R_amqp_disconnect, conn$ptr)
  invisible(conn)
}
