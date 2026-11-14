# ADR-0003: Stdlib-only core behind a Broker abstraction

**Status:** accepted

**Decision.** The engine depends only on the Go standard library and talks to the
broker through `streaming.Broker/Producer/Consumer`. A full in-memory broker
implements group coordination, rebalancing, offset commits and pause/resume. The
Redpanda adapter (franz-go) is behind the `redpanda` build tag.

**Why.** (1) Crash, rebalance and overload behaviour becomes testable in
milliseconds without Docker. (2) The default build needs no network. (3) The
interface documents exactly which Kafka semantics the engine relies on.

**Trade-off.** The in-memory broker is a model, not the real thing: it has no
network, replication or heartbeats/session timeouts. The adapter is compile-checked
but must be exercised against a live Redpanda (see docs/architecture.md §6).
