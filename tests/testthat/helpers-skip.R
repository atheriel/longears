local_rmq_available <- NULL

#' Skip Tests if No Local RMQ Server is Detected
skip_if_no_local_rmq <- function() {
  if (is.null(local_rmq_available)) {
    # Cache the check.
    local_rmq_available <<- tryCatch({
      amqp_connect()
      TRUE
    }, error = function(e) {
      FALSE
    })
  }

  if (!local_rmq_available) {
    testthat::skip("no local RMQ server detected")
  }
}

skip_if_no_rabbitmqctl <- function() {
  if (nchar(Sys.which("rabbitmqctl")) == 0) {
    testthat::skip("no rabbitmqctl program detected")
    return()
  }

  if (rabbitmqctl("status") != 0) {
    testthat::skip("insufficient priviledges to run rabbitmqctl")
  }
}

rabbitmqctl <- function(cmd, stdout = FALSE, stderr = FALSE) {
  system2("sudo", c("rabbitmqctl", cmd), stdout = stdout, stderr = stderr)
}
