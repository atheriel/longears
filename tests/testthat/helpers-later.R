wait_for_callbacks <- function(expected, timeout = 0.5, max_attempts = 20) {
  count <- 0L
  attempts <- 0L
  while (count < expected && attempts <= max_attempts) {
    ran <- as.integer(later::run_now(timeout, FALSE))
    count <- count + ran
    attempts <- attempts + 1
  }
  count
}

expect_callbacks <- function(expected, timeout = 0.5, max_attempts = 20) {
  callbacks_run <- wait_for_callbacks(expected, timeout, max_attempts)
  testthat::expect_equal(callbacks_run, expected)
}
