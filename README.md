
<!-- README.md is generated from README.Rmd. Please edit that file. -->
longears
========

This repository contains an early stage proof-of-concept RabbitMQ client for R. It wraps the [reference C library](https://github.com/alanxz/rabbitmq-c) instead of taking the rJava approach of the only other [existing (but abandoned)](https://r-forge.r-project.org/projects/r-message-queue/) package.

Installation
------------

You will need `librabbitmq`, otherwise the installation will fail. On Ubuntu, run

``` shell
$ apt install librabbitmq-dev
```

The package is only available from GitHub for now, so you can install it with

``` r
# install.packages("devtools")
devtools::install_github("atheriel/longears")
```

Usage
-----

You will need to have a local RabbitMQ server running with the default settings to test this.

``` shell
$ # apt install rabbitmq-server
$ systemctl start rabbitmq-server
$ rabbitmqctl status
```

To connect to the server (with default settings):

``` r
conn <- amqp_connect()
conn
#> AMQP Connection:
#>   status:  connected
#>   address: localhost:5672
#>   vhost:   '/'
```

To send messages to a queue:

``` r
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

Afterwards you can delete the queue and disconnect from the server:

``` r
amqp_delete_queue(conn, "my_queue")
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

License
-------

The package is licensed under the GPL, version 2 or later.
