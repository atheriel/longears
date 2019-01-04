#' Declare or Delete Exchanges
#'
#' AMQP exchanges route messages to queues. By default the server will have a
#' few exchanges defined (including the default exchange \code{""}), but
#' additional exchanges can be defined and subsequently deleted by clients.
#'
#' Both \code{amqp_declare_exchange} and \code{amqp_delete_exchange} will
#' raise errors if there is a problem declaring/deleting the exchange.
#'
#' @param conn An object returned by \code{\link{amqp_connect}}.
#' @param exchange The name of an exchange.
#' @param type The type of exchange. Usually one of \code{"direct"},
#'   \code{"fanout"}, or \code{"topic"}.
#' @param passive
#' @param durable
#' @param auto_delete
#' @param internal
#'
#' @examples
#' \dontrun{
#' conn <- amqp_connect()
#' amqp_declare_exchange(conn, "test_exchange", "fanout")
#' amqp_delete_exchange(conn, "test_exchange")
#' amqp_disconnect(conn)
#' }
#'
#' @aliases amqp_exchanges
#' @rdname amqp_exchanges
#' @export
amqp_declare_exchange <- function(conn, exchange, type = "direct",
                                  passive = FALSE, durable = FALSE,
                                  auto_delete = FALSE, internal = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(
    R_amqp_declare_exchange, conn$ptr, exchange, type, passive, durable,
    auto_delete, internal
  ))
}

#' @param if_unused Delete the exchange only if it is unused.
#'
#' @rdname amqp_exchanges
#' @export
amqp_delete_exchange <- function(conn, exchange, if_unused = FALSE) {
  if (!inherits(conn, "amqp_connection")) {
    stop("`conn` is not an amqp_connection object")
  }
  invisible(.Call(R_amqp_delete_exchange, conn$ptr, exchange, if_unused))
}
