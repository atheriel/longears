testthat::context("test-queues.R")

testthat::test_that("Queues can be created, deleted, and receive messages", {
  conn <- amqp_connect()

  # Attempt to declare a queue in a reserved namespace.
  testthat::expect_error(
    amqp_declare_queue(conn, "amq.gen-KJFQEyh9dd8WR-m6G0Tdtg", durable = TRUE),
    regexp = "ACCESS_REFUSED"
  )

  testthat::expect_error(
    amqp_declare_queue(conn, "test.queue", passive = TRUE),
    regexp = "NOT_FOUND"
  )

  testthat::expect_silent(
    amqp_declare_queue(conn, "test.queue", durable = FALSE, auto_delete = TRUE)
  )

  # Attempt to declare a queue with inconsistent parameters.
  testthat::expect_error(
    amqp_declare_queue(conn, "test.queue", durable = TRUE),
    regexp = "PRECONDITION_FAILED"
  )

  testthat::expect_equal(
    amqp_delete_queue(conn, "test.queue", if_empty = TRUE), 0
  )

  # Queues should be able to receive messages.
  tmp <- testthat::expect_silent(amqp_declare_tmp_queue(conn))
  testthat::expect_silent(
    amqp_publish(conn, routing_key = tmp, body = "test_message")
  )
  testthat::expect_silent(
    amqp_publish(conn, routing_key = tmp, body = "test_message")
  )

  # We should be able to get() messages from queues.
  out <- testthat::expect_silent(amqp_get(conn, tmp))
  expected <- "test_message"
  # Drop attributes, for simplicity.
  attributes(out) <- NULL
  testthat::expect_equal(out, expected)

  # The existing message should prevent deletion.
  testthat::expect_error(
    amqp_delete_queue(conn, tmp, if_empty = TRUE),
    regexp = "PRECONDITION_FAILED"
  )

  # Unless use if_empty = FALSE (the default).
  testthat::expect_equal(amqp_delete_queue(conn, tmp), 2)

  amqp_disconnect(conn)
})
