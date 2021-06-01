#ifdef DISABLE_TRACING
#include "tracing.h"

/* No-op implementations. */

extern "C" void * start_publish_span(const char *exchange,
                                     const char *routing_key,
                                     connection *conn)
{
  return nullptr;
}
extern "C" void finish_publish_span(void * ctx, int result) {}
#else
#include <opentelemetry/trace/provider.h>
#include <opentelemetry/trace/tracer.h>

#include "longears.h"
#include "tracing.h"

namespace nostd = opentelemetry::nostd;
namespace trace = opentelemetry::trace;

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
