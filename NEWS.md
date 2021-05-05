# longears 0.2.4.9000

- The package now builds and installs correctly on Windows with a stock
  [Rtools](https://cran.r-project.org/bin/windows/Rtools/) toolchain.

- Consumers created with `amqp_consume()` will now acknowledge messages only
  *after* the callback has run. Moreover, when a callback fails the message in
  question will be nacked instead. The new `requeue_on_error` parameter controls
  whether messages should be redelivered when callbacks error; by default it is
  `FALSE`, in line with current behaviour. For complete control over when and
  how messages are nacked, one can use the new `amqp_nack()` function, which
  works like `stop()` inside a message handler. The following is an example of
  complex acknowledgement behaviour for a consumer, which might be used in
  combination with a [dead letter exchange](https://www.rabbitmq.com/dlx.html):

  ```r
  consumer <- amqp_consume(conn, queue, function(msg) {
    if (msg$redelivered) {
      message("can't handle this either, dead-letter it")
      amqp_nack(requeue = FALSE)
    } else {
      stop("can't handle this, maybe someone else can")
    }
  }, requeue_on_error = TRUE)
  ```

- Consumers can now control the number of prefetched messages, though the
  default remains 50. The most likely reason to do so is to implement true
  round-robin dispatch by setting it to exactly 1. This is useful in scenarios
  where parallel workers fetch messages from the same queue and you want to
  guarantee that as many workers as possible are busy, even if there are less
  than 50 queued messages. (#12 by @SyncM1972, Closes #2)

- `ampq_connect()` now takes a `name` parameter that allows users to set a
  connection name, which will appear in the RabbitMQ management plugin web
  interface and other supported UIs.

- The `print()` method for a connection will now print out all server and client
  properties for a connection when passed `full = TRUE`.

- Message objects now have a `received` field with a timestamp.

# longears 0.2.4

- Fixes handling of connection failures in `amqp_listen()`. Previously if you
  got booted off the cluster during the acknowledgement phase the connection
  would not be closed properly and only a warning would be triggered. Now that
  warning is a hard error, as it should be.

- Fixes setting of prefetch counts on consumers; previously they were
  nonfunctional due to a misunderstanding of the AMQP spec and RabbitMQ's
  interpretation of it. This should improve performance in some cases.

# longears 0.2.3

- `amqp_listen()` will now respect the timeout value even when processing many
  messages. To reliably consume "endlessly", use a `while(TRUE)` loop.

- Connections now set common client properties, which are visible in the
  RabbitMQ management plugin web interface.

- Fixes usage of `librabbitmq` connection objects during disconnection events;
  it turns out that re-using these handles can sometimes cause issues when
  attempting to reconnect to a server.

- Fix compilation issues due to a missing header on pre-3.4 versions of R.

# longears 0.2.2

- `amqp_publish()` will now accept raw vectors for the message body in addition
  to strings. (#4 by @dselivanov)

- Connections now set a heartbeat of 60 seconds. This should hopefully eliminate
  bugs where R would hang when trying to send messages to a closed socket
  (especially inside finalizers).

- Adds support for consumer cancel notifications, under which the RabbitMQ
  broker will send an appropriate message when e.g. a consumer's queue is
  deleted on the server side. The background thread will throw a warning,
  similar to how connection errors are currently surfaced. Prior to this,
  consumers would simply fall silent without a warning of any kind.

- Handling of closed connections on background threads has been improved.

# longears 0.2.1 (2019-08-28)

- First public release.

- Includes various improvements to top-level documentation.

# longears 0.2.0 (2019-07-19)

- Additional arguments can now be passed to exchange and queue declarations,
  consumers, and bindings, allowing users to employ most of RabbitMQ's
  extensions to the AMQP protocol.

- Additional arguments passed to `amqp_properties()` are now interpreted as
  custom message headers, with R types mapped to an appropriate AMQP field
  automatically.

- Exchange-to-exchange bindings are now available through `amqp_bind_exchange()`
  and `amqp_unbind_exchange()`.

- Temporary queues now default to `exclusive = FALSE`. This is a much more
  sensible choice, but a breaking change.

- The background thread now checks messages at much higher frequency: 100 Hz
  instead of 1 Hz. This means that `consume_later()` is no longer limited to
  ingesting 1 message per second -- it can now handle as many as 100.

# longears 0.1.3 (2019-07-15)

- Fixes a C-level namespace collision with `connect(2)` that could result in
  the wrong function being used by the linker.

# longears 0.1.2 (2019-07-10)

- Connection objects will no longer attempt to automatically reconnect. Instead,
  they will surface an error for the user. This is because "automatic"
  reconnection has turned out to be a bit dishonest: connections have state --
  exclusive or auto-deleting queues and exchanges, plus consumers -- that will
  be lost during disconnections. Thus, the user must be informed about these
  events so that they can properly restore the expected state.

- Internal handling of server/socket issues has dramatically improved. These
  errors will now be identified earlier and reported in a much less cryptic
  manner. Users will also be warned about lost consumers, either during
  reconnection for the foreground consumers or via a **later** callback for
  background consumers.

- Consumers will now set a prefetch count of 50 automatically and attempt to
  acknowledge all messages even if the callback fails (unless they are in
  `no_ack` mode).

# longears 0.1.1 (2019-06-20)

- Tests requiring a local RMQ server will now be skipped unless one is detected.

- Fixes an issue where `amqp_properties()` would not handle missing names
  correctly.

- Fixes an issue where connection errors could sometimes encounter uninitialized
  memory.

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

- Also includes an experimental `amqp_consume_later()` function for
  "asynchronous" message handling. Requires that the user has an event loop
  compatible with the **later** package, such as a Shiny application.
