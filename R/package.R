#' @useDynLib longears, .registration = TRUE
NULL

#' Message Properties
#'
#' The AMQP specification provides for a series of "basic properties" that can
#' be attached to messages. These functions allow for creating a set of these
#' properties and decoding them into an R list.
#'
#' @param ... Basic properties or headers. See \strong{Details}. For
#'   \code{as.list()} and \code{print()}, these are ignored.
#'
#' @details
#'
#' Arguments from the following list will be converted into the corresponding
#' AMQP Basic Property. Additional arguments are converted to headers. Note that
#' all values must be of length 1, and properties themselves are generally
#' strings or integers.
#'
#' \describe{
#'   \item{\code{content_type}}{MIME content type.}
#'   \item{\code{content_encoding}}{MIME content encoding.}
#'   \item{\code{delivery_mode}}{Non-persistent (1) or persistent (2).}
#'   \item{\code{priority}}{Message priority, 0 to 9.}
#'   \item{\code{correlation_id}}{Application correlation identifier.}
#'   \item{\code{reply_to}}{Address to reply to.}
#'   \item{\code{expiration}}{Message expiration specification.}
#'   \item{\code{message_id}}{Application message identifier.}
#'   \item{\code{timestamp}}{Message timestamp.}
#'   \item{\code{type}}{Message type name.}
#'   \item{\code{user_id}}{Client user identifier.}
#'   \item{\code{app_id}}{Client application identifier.}
#'   \item{\code{cluster_id}}{Cluster identifier.}
#' }
#'
#' @examples
#' (props <- amqp_properties(
#'   content_type = "text/plain", content_encoding = "UTF-8",
#'   delivery_mode = 2, priority = 2, correlation_id = "2",
#'   reply_to = "1", expiration = "18000", message_id = "2",
#'   type = "message", user_id = "guest",
#'   app_id = "my_app", cluster_id = "cluster"
#' ))
#' as.list(props)
#'
#' @export
amqp_properties <- function(...) {
  args <- list(...)
  if (!all(vapply(args, nchar, integer(1)) > 0)) {
    stop("All properties must be named.")
  }
  .Call(R_amqp_encode_properties, args)
}

#' @param x An object of class \code{"amqp_properties"}.
#' @rdname amqp_properties
#' @export
as.list.amqp_properties <- function(x, ...) {
  .Call(R_amqp_decode_properties, x$ptr)
}

#' @rdname amqp_properties
#' @export
print.amqp_properties <- function(x, ...) {
  cat("AMQP Message Properties\n")
}
