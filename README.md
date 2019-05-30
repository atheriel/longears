
<!-- README.md is generated from README.Rmd. Please edit that file. -->

# longears

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

The package implements a growing subset of the Advanced Message Queuing
Protocol (AMQP) used by RabbitMQ (see [Limitations](#Limitations) for
details), and the API largely reflects [the protocol
itself](https://www.rabbitmq.com/amqp-0-9-1-reference.html).

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

## Usage

You will need to have a local RabbitMQ server running with the default
settings to test this.

``` shell
$ # apt install rabbitmq-server
$ systemctl start rabbitmq-server
$ rabbitmqctl status
```

First, connect to the server (with default settings):

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
amqp_declare_exchange(conn, "my.exchange")
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

There are many AMQP features missing at present but that will be added
in the future. An incomplete list is as follows:

  - Support for additional AMQP methods.
  - Support for headers in Basic properties; and
  - Support for “table” arguments to e.g. exchange declarations.

## License

The package is licensed under the GPL, version 2 or later.
