# ADR-0006: Dependency-free tracing over OTLP/HTTP + W3C Trace Context

**Status:** accepted

**Context.** The roadmap called for OpenTelemetry. The official Go SDK pulls in
`go.opentelemetry.io/*` and `golang.org/x/*`; both vanity domains are unreachable in
the restricted build environments this project targets, and ADR-0003 keeps the core
stdlib-only.

**Decision.** Implement the small slice we need in `internal/tracing` against the
**standards**, not an SDK:
* **Propagation:** W3C Trace Context `traceparent`, carried *inside the event
  payload* (field `traceparent`) so it works identically on every broker and in both
  wire formats — no dependence on record-header support.
* **Export:** OTLP/HTTP with JSON encoding (`POST {endpoint}/v1/traces`), batched,
  one retry on 429/5xx, none on other 4xx.
* **Sampling:** decided once at the trace root; inherited via the `sampled` flag, so a
  trace is complete or absent. Default ratio 1 % (`SF_TRACE_SAMPLE`).
* **Config:** the standard `OTEL_EXPORTER_OTLP_ENDPOINT` / `OTEL_SERVICE_NAME`.

**Hard rule:** telemetry must never hurt the pipeline. `Span.End` is a non-blocking
channel send; a full queue **drops** the span and counts it. A nil tracer/span is a
valid no-op.

**Spans:** `publish` (PRODUCER) → `process <topic>` (CONSUMER, child, across the
broker) ; `sink.write` (CLIENT, own sampled root).

**Verified** against a real OpenTelemetry Collector (`otelcol-contrib` 0.165,
OTLP/HTTP receiver): spans arrive in the correct JSON shape and a single trace links a
producer-process span to a processor-process span (see `docs/VERIFICATION.md`).
**Not verified:** Jaeger/Tempo specifically, and interop with the official SDK's
propagators (the wire format is standard, so it should work).

**Trade-offs / when to revisit.** No metrics/logs signals, no span links or events,
no baggage, JSON is larger than protobuf. If richer telemetry is needed, replace this
package with the official SDK — the `Tracer/Span/SetAttr/End` shape deliberately
mirrors the OTel API so call sites don't change.
