testthat::context("test-exchanges.R")

testthat::test_that("Exchanges can be created and deleted", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()

  testthat::expect_silent(exch <- amqp_declare_tmp_exchange(conn))

  testthat::expect_error(
    amqp_declare_exchange(conn, "longears-doesnotexist", passive = TRUE),
    regexp = "NOT_FOUND"
  )

  # Attempt to redeclare an exchange with an inconsistent type.
  testthat::expect_error(
    amqp_declare_exchange(conn, exch, type = "fanout"),
    regexp = "PRECONDITION_FAILED"
  )

  # Redeclare the exchange with the same parameters.
  testthat::expect_silent(
    amqp_declare_exchange(conn, exch, auto_delete = TRUE)
  )

  testthat::expect_silent(amqp_delete_exchange(conn, exch))

  # Attempt to create an exchange with an invalid type. This is actually a
  # connection-level error.
  testthat::expect_error(
    amqp_declare_tmp_exchange(conn, type = "invalid"),
    regexp = "invalid exchange type"
  )

  testthat::expect_output(amqp_disconnect(conn), "already closed")
})

testthat::test_that("Queues can be bound to exchanges", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()

  tmp <- amqp_declare_tmp_queue(conn)

  # Bind to a non-existant exchange.
  testthat::expect_error(
    amqp_bind_queue(conn, tmp, "longears-doesnotexist"),
    regexp = "NOT_FOUND"
  )

  exch1 <- amqp_declare_tmp_exchange(conn)
  exch2 <- amqp_declare_tmp_exchange(conn)

  testthat::expect_silent(amqp_bind_queue(conn, tmp, exch1))
  testthat::expect_silent(amqp_bind_exchange(conn, exch2, exch1))

  testthat::expect_silent(amqp_unbind_queue(conn, tmp, exch1))
  testthat::expect_silent(amqp_unbind_exchange(conn, exch2, exch1))

  # The (auto_delete) exchange should be deleted when the queue is unbound.
  testthat::expect_error(
    amqp_bind_queue(conn, tmp, exch1), regexp = "NOT_FOUND"
  )

  amqp_delete_queue(conn, tmp, if_empty = TRUE)
  amqp_disconnect(conn)
})
