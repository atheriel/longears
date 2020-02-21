testthat::context("test-connect.R")

testthat::test_that("Invalid servers are reported in a lucid manner", {
  testthat::expect_error(
    amqp_connect("doesnotexist.example.com"), regexp = "hostname lookup failed"
  )

  testthat::expect_error(
    amqp_connect(port = 41231), regexp = "Is the server running?"
  )
})

testthat::test_that("Local server connection errors are reported correctly", {
  skip_if_no_local_rmq()

  testthat::expect_error(
    amqp_connect(username = "invalid"), regexp = "ACCESS_REFUSED"
  )

  testthat::expect_error(
    amqp_connect(password = "invalid"), regexp = "ACCESS_REFUSED"
  )

  testthat::skip_on_travis()
  testthat::expect_error(
    amqp_connect(vhost = "invalid"), regexp = "NOT_ALLOWED"
  )
})

testthat::test_that("Disconnection and reconnection works correctly", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()

  testthat::expect_silent(amqp_disconnect(conn))
  testthat::expect_output(amqp_disconnect(conn), regexp = "already closed")
  testthat::expect_silent(amqp_reconnect(conn))
  testthat::expect_output(amqp_reconnect(conn), regexp = "already open")
})

testthat::test_that("Disconnections are handled correctly", {
  skip_if_no_local_rmq()
  skip_if_no_rabbitmqctl()

  conn <- amqp_connect()

  # Simulate an unexpected disconnection.
  testthat::expect_equal(rabbitmqctl("stop_app"), 0)
  testthat::expect_equal(rabbitmqctl("start_app"), 0)

  testthat::expect_error(
    amqp_declare_tmp_exchange(conn),
    regexp = "Disconnected from server"
  )

  testthat::expect_error(
    amqp_declare_tmp_exchange(conn),
    regexp = "Not connected to a server"
  )

  testthat::expect_silent(amqp_reconnect(conn))

  # Retry.
  testthat::expect_silent(amqp_declare_tmp_exchange(conn))
})
