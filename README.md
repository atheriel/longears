
<!-- README.md is generated from README.Rmd. Please edit that file. -->
longears
========

This repository contains an **extremely** early stage proof-of-concept RabbitMQ library for R. It wraps the reference C library (`rabbitmq-c`) instead taking the rJava approach of the only other [existing (but abandoned) package](https://r-forge.r-project.org/projects/r-message-queue/).

Installation
------------

You will need the `rabbitmq-c` library, otherwise the installation will fail. On Ubuntu, run

``` shell
$ apt install librabbitmq-dev
```

And then build and install this package from source.

Example
-------

You will need to have a local RabbitMQ server running with the default settings to test this.

``` shell
$ systemctl start rabbitmq-server
$ rabbitmqctl status
```

To send messages to a queue:

``` r
conn <- amqp_connect()
amqp_declare_queue(conn, "my_queue")
#> Queue messages: 0 consumers: 0
amqp_publish(conn, "my_queue", "message")
amqp_publish(conn, "my_queue", "second message")
```

Check if your messages are going into the queue:

``` shell
$ rabbitmqctl list_queues
```

And to pull messages back into R:

``` r
amqp_get(conn, "my_queue")
#> [1] "message"
amqp_get(conn, "my_queue")
#> [1] "second message"
amqp_get(conn, "my_queue")
#> character(0)
```
