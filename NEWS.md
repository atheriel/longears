# longears 0.1.0 (2019-06-06)

- First alpha release.

- Allows for connecting to RMQ servers via `amqp_connect()` and
  `amqp_disconnect()`. Reconnections required by protocol errors are handled
  automatically, and AMQP channels are managed internally by the connection
  objects. Most other functions require passing a valid connection object.

- Implements a subset of the AMQP feature set, including:

  - Declaring and deleting exchanges and queues via `amqp_declare_exchange()`,
    `amqp_delete_queue()`, etc.
  - Binding queues to (and unbind them from) exchanges via `amqp_bind_queue()`
    and `amqp_unbind_queue()`.
  - Publishing messages to exchanges via `amqp_publish()`.
  - Encoding/decoding AMQP basic properties via `amqp_properties()`.
  - Retrieving individual messages from queues via `amqp_get()`. This is the
    "polling" interface.
  - Consuming many messages from queues using a combination of `amqp_consume()`
    and `amqp_listen()`. This is the "push" interface.

- Also includes an experimentatal `amqp_consume_later()` function for
  "asynchronous" message handling. Requires that the user has an event loop
  compatible with the **later** package, such as a Shiny application.
