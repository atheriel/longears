#ifdef DISABLE_TRACING
#include "tracing.h"

/* No-op implementations. */

extern "C" void extract_span_ctx(amqp_basic_properties_t *props) {}
extern "C" void inject_span_ctx(amqp_basic_properties_t *props) {}

extern "C" void * start_publish_span(const char *exchange,
                                     const char *routing_key,
                                     connection *conn)
{
  return nullptr;
}
extern "C" void finish_publish_span(void * ctx, int result) {}
#else
#include <opentelemetry/context/context.h>
#include <opentelemetry/context/propagation/global_propagator.h>
#include <opentelemetry/trace/propagation/detail/context.h>
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>

#include "longears.h"
#include "tracing.h"

namespace context = opentelemetry::context;
namespace nostd = opentelemetry::nostd;
namespace trace = opentelemetry::trace;

/* Context propagation. */

class AmqpMessagePropertiesCarrier : public context::propagation::TextMapCarrier
{
public:
  AmqpMessagePropertiesCarrier(amqp_basic_properties_t *props) : props_(props) {}

  virtual nostd::string_view Get(nostd::string_view key) const noexcept override
  {
    /* Find a matching header in the AMQP properties, if any. */

    for (int i = 0; i < props_->headers.num_entries - 1; i++) {
      if (strncasecmp((const char *) props_->headers.entries[i].key.bytes,
                      key.data(), key.size()) == 0 &&
          props_->headers.entries[i].value.kind == AMQP_FIELD_KIND_UTF8) {
        return {
          (const char*) props_->headers.entries[i].value.value.bytes.bytes,
          props_->headers.entries[i].value.value.bytes.len
        };
      }
    }

    return {};
  }

  virtual void Set(nostd::string_view key, nostd::string_view value) noexcept override
  {
    amqp_field_value_t field_value;
    field_value.kind = AMQP_FIELD_KIND_UTF8;
    field_value.value.bytes = amqp_bytes_malloc(value.size());
    field_value.value.bytes.bytes = (void *) value.data();

    /* Easy case: there are no existing headers. */
    if (props_->headers.num_entries == 0) {
      props_->headers.num_entries++;
      props_->headers.entries =
        (amqp_table_entry_t *) calloc(1, sizeof(amqp_table_entry_t));
      props_->headers.entries[0].key = amqp_bytes_malloc(key.size());
      props_->headers.entries[0].key.bytes = (void *) key.data();
      props_->headers.entries[0].value = field_value;
      return;
    }

    /* Replace an existing key. */
    int i;
    for (i = 0; i < props_->headers.num_entries - 1; i++) {
      if (strncasecmp((const char *) props_->headers.entries[i].key.bytes,
                      key.data(), key.size()) == 0) {
        /* TODO: Free the existing value? See free_value(). */
        props_->headers.entries[i].value = field_value;
        return;
      }
    }

    /* Copy the existing table and add a new key. */
    amqp_table_entry_t *entries =
      (amqp_table_entry_t *) calloc(props_->headers.num_entries + 1,
                                    sizeof(amqp_table_entry_t));
    for (i = 0; i < props_->headers.num_entries - 1; i++) {
      entries[i].key = props_->headers.entries[0].key;
      entries[i].value = props_->headers.entries[0].value;
    }
    i++; // Advance to the last entry.
    entries[i].key = amqp_bytes_malloc(key.size());
    entries[i].key.bytes = (void *) key.data();
    entries[i].value = field_value;

    /* Swap in the new entries. */
    free(props_->headers.entries);
    props_->headers.num_entries++;
    props_->headers.entries = entries;
  }

  amqp_basic_properties_t *props_;
};

extern "C" void extract_span_ctx(amqp_basic_properties_t *props)
{
  AmqpMessagePropertiesCarrier carrier(props);
  auto context = context::RuntimeContext::GetCurrent();
  auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
  propagator->Extract(carrier, context);
}

extern "C" void inject_span_ctx(amqp_basic_properties_t *props)
{
  AmqpMessagePropertiesCarrier carrier(props);
  auto context = context::RuntimeContext::GetCurrent();
  auto propagator = context::propagation::GlobalTextMapPropagator::GetGlobalPropagator();
  propagator->Inject(carrier, context);
}

// For semantic conventions see:
// https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/semantic_conventions/messaging.md

nostd::shared_ptr<trace::Tracer> get_tracer()
{
  auto provider = trace::Provider::GetTracerProvider();
  return provider->GetTracer("longears", LONGEARS_VERSION);
}

struct TraceContext {
  nostd::shared_ptr<trace::Span> current_span;
};

extern "C" void * start_publish_span(const char *exchange,
                                     const char *routing_key,
                                     connection *conn)
{
  std::string span_name = exchange + std::string(" send");
  trace::StartSpanOptions options;
  options.kind = trace::SpanKind::kProducer;

  // Set the parent as the existing span context, if it exists.
  auto context = context::RuntimeContext::GetCurrent();
  auto parent = trace::propagation::GetSpan(context);
  options.parent = parent->GetContext();

  auto ctx = new TraceContext();

  // Note: most of the OpenTelemetry APIs are marked noexcept, so we can assume
  // that they will not throw exceptions and screw up the R stack.

  ctx->current_span = get_tracer()->StartSpan(
     span_name,
     {
      {"messaging.system", "rabbitmq"},
      {"messaging.destination", exchange},
      {"messaging.destination_kind", "topic"},
      {"messaging.rabbitmq.routing_key", routing_key},
      {"net.peer.name", conn->host},
      {"net.peer.port", conn->port},
     },
     options);

  return (void *) ctx;
}

extern "C" void finish_publish_span(void * ptr, int result)
{
  auto ctx = (TraceContext *) ptr;
  if (result != AMQP_STATUS_OK) {
    ctx->current_span->SetStatus(trace::StatusCode::kError,
                                 amqp_error_string2(result));
  }
  ctx->current_span->End();
}
#endif
