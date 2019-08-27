
<!-- README.md is generated from README.Rmd. Please edit that file. -->

# longears

<!-- badges: start -->
[![travis-ci build status](https://travis-ci.org/atheriel/longears.svg?branch=master)](https://travis-ci.org/atheriel/longears)
<!-- badges: end -->

This repository contains an early stage RabbitMQ client for R. It wraps
the [reference C library](https://github.com/alanxz/rabbitmq-c).

[RabbitMQ](https://www.rabbitmq.com/) is a popular and performant
open-source message broker used to build highly distributed and
asynchronous network topologies. This package may be of interest to you
if you wish to have R speak to your organization’s existing RabbitMQ
servers; if you simply need a message queue library, you may be better
off with [**txtq**](https://github.com/wlandau/txtq),
[**litq**](https://github.com/r-lib/liteq), or the ZeroMQ package
[**rzmq**](https://github.com/ropensci/rzmq).

The package implements a large portion of the Advanced Message Queuing
Protocol (AMQP) used by RabbitMQ , and the API largely reflects [the
protocol itself](https://www.rabbitmq.com/amqp-0-9-1-reference.html).
However, `longears` is not a true low-level package: it abstracts away
many details of AMQP for end users. See [Limitations](#Limitations) for
details.

## Installation

You will need `librabbitmq`, otherwise the installation will fail. On
Ubuntu, run

``` shell
$ apt install librabbitmq-dev
```

The package is only available from GitHub for now, so you can install it
with

``` r
# install.packages("devtools")
devtools::install_github("atheriel/longears")
```

## Basic Usage

If you are not already familiar with RabbitMQ and the
message/queue/binding/exchange terminology, I suggest their excellent
[conceptual
overview](https://www.rabbitmq.com/tutorials/amqp-concepts.html).

You will also need to have a local RabbitMQ server running with the
default settings to follow this guide.

``` shell
$ # apt install rabbitmq-server
$ systemctl start rabbitmq-server
$ rabbitmqctl status
```

First, connect to the server (with the default settings):

``` r
conn <- amqp_connect()
conn
#> AMQP Connection:
#>   status:  connected
#>   address: localhost:5672
#>   vhost:   '/'
```

Create an exchange to route messages and a couple of queues to store
them:

``` r
amqp_declare_exchange(conn, "my.exchange", type = "fanout")
amqp_declare_queue(conn, "my.queue1")
#> AMQP queue 'my.queue1'
#>   messages:  0
#>   consumers: 0
amqp_declare_queue(conn, "my.queue2")
#> AMQP queue 'my.queue2'
#>   messages:  0
#>   consumers: 0
amqp_bind_queue(conn, "my.queue1", "my.exchange", routing_key = "#")
amqp_bind_queue(conn, "my.queue2", "my.exchange", routing_key = "#")
```

You can also set up a consumer for one of these queues with a callback:

``` r
received <- 0
consumer <- amqp_consume(conn, "my.queue2", function(msg) {
  received <<- received + 1
})
```

Now, send a few messages to this exchange:

``` r
amqp_publish(conn, "first", exchange = "my.exchange", routing_key = "#")
amqp_publish(conn, "second", exchange = "my.exchange", routing_key = "#")
```

Check if your messages are going into the queues:

``` shell
$ rabbitmqctl list_queues
```

You can use `amqp_get()` to pull individual messages back into R:

``` r
amqp_get(conn, "my.queue1")
#> Delivery Tag:    1
#> Redelivered: FALSE
#> Exchange:    my.exchange
#> Routing Key: #
#> Message Count:   1
#> 66 69 72 73 74
amqp_get(conn, "my.queue1")
#> Delivery Tag:    2
#> Redelivered: FALSE
#> Exchange:    my.exchange
#> Routing Key: #
#> Message Count:   0
#> 73 65 63 6f 6e 64
amqp_get(conn, "my.queue1")
#> character(0)
```

Or you can use `amqp_listen()` to run consumer callbacks:

``` r
amqp_listen(conn, timeout = 1)
received
#> [1] 2
```

To clean things up, delete the queues, the exchange, and disconnect from
the server.

``` r
amqp_delete_queue(conn, "my.queue1")
amqp_delete_queue(conn, "my.queue2")
amqp_delete_exchange(conn, "my.exchange")
amqp_disconnect(conn)
conn
#> AMQP Connection:
#>   status:  disconnected
#>   address: localhost:5672
#>   vhost:   '/'
```

And check that the connection is closed:

``` shell
$ rabbitmqctl list_connections
```

## Limitations

Some AMQP features are not present, notably transaction support (which
there are no plans to implement). Others are not exposed through the API
but are handled internally according to best practices: channels,
message acknowledgements, and prefetch counts, among others. The current
design goal of the package is to keep these details from the eyes of
users, insofar as that is possible.

If you have need of an AMQP feature (or RabbitMQ extension) that is not
currently available or accessible, please consider filing an issue
explaining the use case you have in mind.

## License

The package is licensed under the GPL, version 2 or later.
