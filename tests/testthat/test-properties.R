testthat::context("test-properties")

testthat::test_that("Encoding and decoding properties works correctly", {
  # Empty properties.
  props <- testthat::expect_silent(amqp_properties())
  empty <- list()
  names(empty) <- character(0)
  testthat::expect_equal(as.list(props), empty)

  # Missing names.
  testthat::expect_error(
    amqp_properties(priority = 1, "missing name"),
    regexp = "All properties must be named"
  )

  # Invalid type.
  testthat::expect_error(
    amqp_properties(priority = "invalid"),
    regexp = "'priority' must be an integer"
  )

  valid_props <- list(
    content_type = "text/plain", content_encoding = "UTF-8", delivery_mode = 2,
    priority = 2, correlation_id = "2", reply_to = "1", expiration = "2020",
    message_id = "2", type = "message", user_id = "guest",
    app_id = "application", cluster_id = "cluster"
  )
  props <- testthat::expect_silent(do.call(amqp_properties, valid_props))
  testthat::expect_equal(as.list(props), valid_props)
})
