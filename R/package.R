#' @useDynLib longears, .registration = TRUE
NULL

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

