testthat::context("test-consume.R")

testthat::test_that("Consume works as expected", {
  conn <- amqp_connect()

  # Must create consumers first.
  testthat::expect_error(
    amqp_listen(conn, function(msg) {
      stop("Should never happen.")
    }), regexp = "No consumers are declared on this connection"
  )

  amqp_declare_exchange(conn, "test.exchange", auto_delete = TRUE)
  q1 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q1, "test.exchange", routing_key = "#")
  q2 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q2, "test.exchange", routing_key = "#")
  q3 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q3, "test.exchange", routing_key = "#")

  c1 <- testthat::expect_silent(amqp_create_consumer(conn, q1))
  c2 <- testthat::expect_silent(amqp_create_consumer(conn, q2))
  c3 <- testthat::expect_silent(amqp_create_consumer(conn, q3))

  amqp_publish(
    conn, body = "Hello, world", exchange = "test.exchange", routing_key = "#"
  )
  amqp_publish(
    conn, body = "Hello, again", exchange = "test.exchange", routing_key = "#"
  )

  messages <- data.frame()
  amqp_listen(conn, function(msg) {
    messages <<- rbind(messages, as.data.frame(msg))
    msg$delivery_tag > 1
  })

  testthat::expect_equal(nrow(messages), 2)

  testthat::expect_silent(amqp_destroy_consumer(c1))

  testthat::expect_error(
    amqp_destroy_consumer(c1), regexp = "Invalid consumer object"
  )

  testthat::expect_silent(amqp_destroy_consumer(c3))
  testthat::expect_silent(amqp_destroy_consumer(c2))

  amqp_disconnect(conn)
})
