testthat::context("test-exchanges.R")

testthat::test_that("Exchanges can be created and deleted", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()

  testthat::expect_silent(amqp_delete_exchange(conn, "test.exchange"))

  testthat::expect_error(
    amqp_declare_exchange(conn, "test.exchange", type = "fanout", passive = TRUE),
    regexp = "NOT_FOUND"
  )

  testthat::expect_silent(amqp_declare_exchange(conn, "test.exchange"))

  # Attempt to declare an exchange with an inconsistent type.
  testthat::expect_error(
    amqp_declare_exchange(conn, "test.exchange", type = "fanout"),
    regexp = "PRECONDITION_FAILED"
  )

  testthat::expect_silent(amqp_declare_exchange(conn, "test.exchange"))
  testthat::expect_silent(amqp_delete_exchange(conn, "test.exchange"))

  # Attempt to create an exchange with an invalid type. This is actually a
  # connection-level error.
  testthat::expect_error(
    amqp_declare_exchange(conn, "test.exchange2", type = "invalid"),
    regexp = "invalid exchange type"
  )

  testthat::expect_output(amqp_disconnect(conn), "already closed")
})

testthat::test_that("Queues can be bound to exchanges", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()

  tmp <- amqp_declare_tmp_queue(conn)

  # Bind to a non-existant exchange.
  testthat::expect_error(amqp_bind_queue(conn, tmp, "doesnotexist"),
                         regexp = "NOT_FOUND")

  amqp_declare_exchange(conn, "test.exchange", auto_delete = TRUE)

  testthat::expect_silent(amqp_bind_queue(conn, tmp, "test.exchange"))

  # The (auto_delete) exchange should be deleted when the queue is unbound.
  testthat::expect_silent(amqp_unbind_queue(conn, tmp, "test.exchange"))
  testthat::expect_error(amqp_bind_queue(conn, tmp, "test.exchange"),
                         regexp = "NOT_FOUND")

  amqp_delete_queue(conn, tmp, if_empty = TRUE)
  amqp_disconnect(conn)
})
