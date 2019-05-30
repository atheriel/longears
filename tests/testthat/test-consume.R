testthat::context("test-consume.R")

testthat::test_that("Consume works as expected", {
  conn <- amqp_connect()

  # Must create consumers first.
  testthat::expect_error(
    amqp_listen(conn),
    regexp = "No consumers are declared on this connection"
  )

  messages <- data.frame()
  fun <- function(msg) {
    messages <<- rbind(messages, as.data.frame(msg))
    msg$delivery_tag > 1
  }

  amqp_declare_exchange(conn, "test.exchange", auto_delete = TRUE)
  q1 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q1, "test.exchange", routing_key = "#")
  q2 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q2, "test.exchange", routing_key = "#")
  q3 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q3, "test.exchange", routing_key = "#")

  c1 <- testthat::expect_silent(amqp_consume(conn, q1, fun))
  c2 <- testthat::expect_silent(amqp_consume(conn, q2, base::identity))
  c3 <- testthat::expect_silent(amqp_consume(conn, q3, base::identity))

  amqp_publish(
    conn, body = "Hello, world", exchange = "test.exchange", routing_key = "#"
  )
  amqp_publish(
    conn, body = "Hello, again", exchange = "test.exchange", routing_key = "#"
  )

  amqp_listen(conn, timeout = 1)

  testthat::expect_equal(nrow(messages), 2)

  amqp_publish(
    conn, body = "Goodbye", exchange = "test.exchange", routing_key = "#"
  )

  testthat::expect_silent(amqp_cancel_consumer(c1))

  testthat::expect_error(
    amqp_cancel_consumer(c1), regexp = "Invalid consumer object"
  )

  amqp_listen(conn, timeout = 1)

  # We don't want the cancelled consumer's callback to have been called again.
  testthat::expect_equal(nrow(messages), 2)

  testthat::expect_silent(amqp_cancel_consumer(c3))
  testthat::expect_silent(amqp_cancel_consumer(c2))

  testthat::expect_error(
    amqp_listen(conn),
    regexp = "No consumers are declared on this connection"
  )

  amqp_disconnect(conn)
})
