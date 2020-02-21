testthat::context("test-publish.R")

testthat::test_that("Publish works as expected", {
  skip_if_no_local_rmq()

  conn <- amqp_connect()
  exch <- amqp_declare_tmp_exchange(conn)
  q1 <- amqp_declare_tmp_queue(conn)
  amqp_bind_queue(conn, q1, exch, routing_key = "#")

  testthat::expect_warning(
    amqp_publish(
      conn, body = c("hello", "hello"), exchange = exch, routing_key = "#"
    )
  )

  testthat::expect_error(
    amqp_publish(conn, body = complex(1), exchange = exch, routing_key = "#")
  )

  amqp_publish(conn, body = "hello", exchange = exch, routing_key = "#")
  msg <- amqp_get(conn, q1)
  testthat::expect_equal(msg$body, charToRaw("hello"))

  msg_sent <- charToRaw("hello")
  amqp_publish(conn, body = msg_sent, exchange = exch, routing_key = "#")
  msg <- amqp_get(conn, q1)
  testthat::expect_equal(msg$body, msg_sent)

  amqp_disconnect(conn)
})
