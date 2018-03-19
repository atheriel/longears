testthat::context("test-queues.R")

testthat::test_that("Queues can be created, deleted, and receive messages", {
  conn <- amqp_connect()

  # Create/immediately delete a queue.
  tmp <- testthat::expect_silent(amqp_declare_tmp_queue(conn))
  testthat::expect_equal(amqp_delete_queue(conn, tmp, if_empty = TRUE), 0)

  # Temporary queues should be able to receive messages.
  tmp <- testthat::expect_silent(amqp_declare_tmp_queue(conn))
  testthat::expect_silent(amqp_publish(conn, routing_key = tmp,
                                       body = "test_message"))

  # The existing message should prevent deletion.
  testthat::expect_error(amqp_delete_queue(conn, tmp, if_empty = TRUE),
                         regexp = "PRECONDITION_FAILED")

  amqp_disconnect(conn)
})
