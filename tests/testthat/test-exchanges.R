testthat::context("test-exchanges.R")

testthat::test_that("Exchanges can be created and deleted", {
  conn <- amqp_connect()

  # Create/immediately delete an exchange.
  testthat::expect_silent(amqp_declare_exchange(conn, "test_exchange"))
  testthat::expect_silent(amqp_delete_exchange(conn, "test_exchange"))

  # Attempt to create an exchange with an invalid type.
  testthat::expect_error(amqp_declare_exchange(conn, "test_exchange",
                                               type = "invalid"),
                         regexp = "invalid exchange type")

  amqp_disconnect(conn)
})

testthat::test_that("Queues can be bound to exchanges", {
  conn <- amqp_connect()

  amqp_declare_exchange(conn, "test_exchange")
  tmp <- amqp_declare_tmp_queue(conn)

  testthat::expect_silent(amqp_bind_queue(conn, tmp, "test_exchange", tmp))
  testthat::expect_silent(amqp_unbind_queue(conn, tmp, "test_exchange", tmp))

  amqp_delete_exchange(conn, "test_exchange")
  amqp_delete_queue(conn, tmp, if_empty = TRUE)
  amqp_disconnect(conn)
})
