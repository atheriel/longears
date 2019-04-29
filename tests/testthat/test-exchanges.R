testthat::context("test-exchanges.R")

testthat::test_that("Exchanges can be created and deleted", {
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

  # Attempt to create an exchange with an invalid type.
  testthat::expect_error(
    amqp_declare_exchange(conn, "test.exchange2", type = "invalid"),
    regexp = "invalid exchange type"
  )

  testthat::expect_silent(amqp_delete_exchange(conn, "test.exchange"))

  amqp_disconnect(conn)
})

testthat::test_that("Queues can be bound to exchanges", {
  conn <- amqp_connect()

  amqp_declare_exchange(conn, "test.exchange")
  tmp <- amqp_declare_tmp_queue(conn)

  testthat::expect_silent(amqp_bind_queue(conn, tmp, "test.exchange"))
  testthat::expect_silent(amqp_unbind_queue(conn, tmp, "test.exchange"))

  # Bind to a non-existant exchange.
  testthat::expect_error(amqp_bind_queue(conn, tmp, "doesnotexist"),
                         regexp = "NOT_FOUND")

  amqp_delete_exchange(conn, "test.exchange")
  amqp_delete_queue(conn, tmp, if_empty = TRUE)
  amqp_disconnect(conn)
})
