#' @useDynLib longears, .registration = TRUE
NULL

#' @export
amqp_properties <- function(props, ...) {
  args <- list(...)
  if (!missing(props) && inherits(props, "amqp_properties")) {
    out <- .Call(R_amqp_decode_properties, props$ptr)
    return(out)
  }
  if (!all(vapply(args, nchar, integer(1)) > 0)) {
    stop("All properties must be named.")
  }
  .Call(R_amqp_encode_properties, args)
}
