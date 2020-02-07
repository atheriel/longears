testthat::context("test-publish.R")

testthat::test_that("basic publish works as expected", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()
  amqp_declare_exchange(conn, "test.exchange", auto_delete = TRUE)
  q1 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q1, "test.exchange", routing_key = "#")

  testthat::expect_error(
    amqp_publish(conn, body = c("Hello", "world"), exchange = "test.exchange", routing_key = "#")
  )

  testthat::expect_error(
    amqp_publish(conn, body = list("Hello"), exchange = "test.exchange", routing_key = "#")
  )

  amqp_publish(conn, body = "hello", exchange = "test.exchange", routing_key = "#")
  msg <- amqp_get(conn, q1)
  testthat::expect_equal(msg$body, charToRaw("hello"))

  msg_sent <- charToRaw("hello")
  amqp_publish(conn, body = msg_sent, exchange = "test.exchange", routing_key = "#")
  msg <- amqp_get(conn, q1)
  testthat::expect_equal(msg$body, msg_sent)

  amqp_disconnect(conn)
})
