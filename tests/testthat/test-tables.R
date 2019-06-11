testthat::context("test-tables")

testthat::test_that("Encoding and decoding tables works correctly", {
  # Empty table.
  table <- testthat::expect_silent(amqp_table())
  empty <- list()
  names(empty) <- character(0)
  testthat::expect_equal(as.list(table), empty)

 # Missing names.
  testthat::expect_error(
    amqp_table(elt = 1, "missing name"),
    regexp = "All arguments must be named"
  )

  # Invalid types.
  testthat::expect_warning(
    amqp_table(valid = 1, invalid = function(x) x),
    regexp = "A 'closure' cannot yet be converted to a table entry"
  )

  valid_fields <- list(
    one = "one", two = 2L, three = 3.5, four = FALSE, five = charToRaw("five"),
    six = NULL, seven = list(a = 1, b = 2), eight = as.list(1:8)
  )
  table <- testthat::expect_silent(do.call(amqp_table, valid_fields))
  testthat::expect_equal(as.list(table), valid_fields)

  # We can't correctly preserve vectors in a round-trip; they are converted
  # into lists when read back into R.
  coerced_fields <- list(
    one = c("one", "1"), two = c(2L, 22L), three = c(3.5, 3.1),
    four = c(FALSE, TRUE)
  )
  table <- testthat::expect_silent(do.call(amqp_table, coerced_fields))
  testthat::expect_equal(as.list(table), lapply(coerced_fields, as.list))
})

testthat::test_that("Queues can be created with additional arguments", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()

  # Test known x-argument extensions.
  tmp <- testthat::expect_silent(amqp_declare_tmp_queue(
    conn, "x-message-ttl" = 180000L, "x-expires" = 180000L,
    "x-max-length" = 10L, "x-max-length-bytes" = 1048576L,
    "x-overflow" = "drop-head", "x-queue-mode" = "lazy",
    "x-max-priority" = 10L, "x-dead-letter-exchange" = "test.dlexchange",
    "x-dead-letter-routing-key" = "#"
  ))

  testthat::expect_equal(amqp_delete_queue(conn, tmp), 0)

  amqp_disconnect(conn)
})
