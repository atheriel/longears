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

client_properties <- function(conn) {
  .Call(R_amqp_client_properties, conn$ptr, PACKAGE = "longears")
}

server_properties <- function(conn) {
  out <- .Call(R_amqp_server_properties, conn$ptr, PACKAGE = "longears")
  # For some reason these are in the reverse order we'd expect.
  rev(out)
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
  header <- list(
    status = ifelse(is_connected(x), "connected", "disconnected"),
    address = sprintf("%s:%s", x$host, x$port),
    vhost = sprintf("'%s'", x$vhost)
  )

  if (!full) {
    cat(sep = "", "AMQP Connection:\n", format_fields(header, "  "), "\n")
    return()
  }

  cprops <- client_properties(x)
  ccapabilities <- cprops$capabilities
  cprops$capabilities <- NULL

  sprops <- server_properties(x)
  scapabilities <- sprops$capabilities
  sprops$capabilities <- NULL

  cat(
    sep = "", "AMQP Connection:\n", format_fields(header, "  "), "\n",
    "  client properties:\n", format_fields(cprops, "    "), "\n",
    "  client capabilities:\n", format_fields(ccapabilities, "    "), "\n",
    "  server properties:\n", format_fields(sprops, "    "), "\n",
    "  server capabilities:\n", format_fields(scapabilities, "    "), "\n"
  )
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
