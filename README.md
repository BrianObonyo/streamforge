# StreamForge

**Real-Time Distributed Event Streaming & Analytics Engine**

StreamForge ingests, processes and analyses high-volume event streams in real time.
Built with **Go**, **Redpanda** and **ClickHouse**, it focuses on the hard parts of
distributed stream processing: partitioning, event-time windowing, out-of-order
and late data, backpressure, and fault recovery that never silently loses or
double-counts data.

> **Author:** Brian Obonyo · Apache-2.0 · design references credited in source headers
> and `docs/`.

## See it work in 20 seconds

```bash
go run ./cmd/demo
```

The demo runs the whole pipeline in one process, injects out-of-order events,
duplicates and garbage, **hard-kills a processor mid-stream**, starts a replacement,
then compares unique events produced against events counted in the store:

```
messages produced:           48397
  unique valid events:       45047
  injected duplicates:       2358
  injected garbage:          992
events counted in store:     45047

✓ EXACT: every unique event was counted exactly once — through disorder, duplicates, garbage and a crash.
```

## Status — what is built and what has been verified

| Area | State |
|---|---|
| Event model, validation, partitioning (hash / jump / round-robin) | ✅ built, unit-tested |
| In-memory broker (partitions, consumer groups, rebalance, commits, pause/resume) | ✅ built, tested |
| Event-time windows (tumbling, sliding), watermarks, allowed lateness | ✅ built, tested |
| Backpressure (bounded queues, broker pause, token bucket, AIMD) | ✅ built, tested incl. end-to-end |
| Reliability (dedup, jittered retry, circuit breaker, DLQ, idempotent sink) | ✅ built, tested |
| Offset management with safe-offset commits | ✅ built, crash-tested |
| ClickHouse store (HTTP, parameterised) | ✅ built, tested against an **HTTP stub only** |
| Analytics API, ingestion API, Prometheus metrics | ✅ built, tested |
| Redpanda adapter (franz-go) | 🟡 **compiles & vets with `-tags redpanda`; never run against a live broker** |
| Docker Compose, Grafana dashboard, Prometheus config | 🟡 written & syntax-validated; **never started** |
| k6 load test | 🟡 written; **never run** |
| OpenTelemetry tracing | ⬜ not started (metrics via Prometheus are done) |
| Published benchmark vs real Redpanda/ClickHouse | ⬜ pending — see `benchmarks/RESULTS.md` for engine-only numbers |

Everything marked ✅ is covered by `go test -race ./...`; the chaos integration test
passed 6 consecutive runs under the race detector.

## Architecture

```
 Event producers / HTTP ──▶ Ingestion (Go) ──▶ Redpanda topic (partitions)
                                                     │
                                  consumer group of Go stream processors
                                                     │
                       per partition: Queue ▶ validate ▶ dedup ▶ windows+watermark
                                                     │
                                  idempotent upsert  ▼
                                               ClickHouse ──▶ Analytics API ──▶ Grafana
```

Full design, concurrency model and delivery guarantees: [`docs/architecture.md`](docs/architecture.md).
Decisions and trade-offs: [`docs/adr/`](docs/adr/).

## Core engineering features

1. **Distributed ingestion** — configurable generator (IoT random walk, log-normal
   clickstream dwell times, GBM market ticks) and an HTTP API that answers `429` under load.
2. **Partitioning & consumer groups** — key-affine routing; rebalance-aware tasks; recovery from committed offsets.
3. **Event-time processing** — tumbling/sliding windows, bounded-out-of-orderness watermarks, idle-partition fallback, allowed lateness with window refinement.
4. **Backpressure** — bounded queues → broker pause/resume with hysteresis → AIMD throttling.
5. **Fault tolerance** — safe-offset commits, dedup, retry + circuit breaker, dead-letter queue, idempotent versioned sink.
6. **Real-time analytics** — events/interval, avg/max, distribution by category, throughput, late-arrival stats.

## Quick start

```bash
make test            # unit tests
make integration     # chaos end-to-end tests (crash, dupes, sink outage, backpressure)
make demo            # the 20-second proof above
make bench           # micro + engine benchmarks
make build           # binaries in ./bin  (stdlib-only core)
make redpanda-build  # same, with the Redpanda/Kafka adapter
make up              # full stack via Docker Compose (see status table!)
```

Analytics API (running in the demo on :8080, or `cmd/api`):

```
GET /v1/summary?since=15m            events / avg / min / max per category
GET /v1/series?kind=iot&key=sensor-001&since=5m
GET /v1/throughput?window=60s
GET /v1/late?since=1h                on-time / late / dropped + max lateness
GET /metrics   /healthz   /readyz
```

## Performance

Measured, not assumed. Engine-only (in-memory broker/store, **1 vCPU**): ~105–115 k
events/s end to end; JSON decode is the dominant cost. Full context and honest
caveats in [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md). Numbers against real
Redpanda/ClickHouse are pending.

## Project structure

```
cmd/            producer · processor · api · demo
internal/       events partitioning streaming backpressure aggregation
                reliability storage processor ingestion api metrics config
tests/          integration/ (chaos)  load/ (k6)
benchmarks/     Go benchmarks + recorded results
deployments/    docker-compose, Prometheus, Grafana
docs/           architecture.md  adr/
```

## Roadmap

- [x] Go project foundation
- [x] Configurable event producers
- [x] Broker abstraction + in-memory broker; Redpanda adapter (compile-verified)
- [x] Partition-aware stream consumers
- [x] Event-time windowing and aggregation
- [x] Out-of-order and late-arriving events
- [x] Backpressure management
- [x] Offset management and duplicate handling
- [x] ClickHouse integration (HTTP)
- [x] Analytics API
- [x] Prometheus + Grafana (OpenTelemetry **pending**)
- [x] Integration and failure-recovery tests
- [ ] Run against live Redpanda + ClickHouse; publish reproducible benchmarks
- [ ] OpenTelemetry tracing
- [ ] Binary encoding (protobuf) to cut decode cost

## Credits

Ideas this project builds on: Akidau et al., *The Dataflow Model* (VLDB 2015) ·
Lamping & Veach, *Jump Consistent Hash* (2014) · Chiu & Jain, AIMD (1989) ·
Brooker, *Exponential Backoff and Jitter* (AWS, 2015) · Nygard, *Release It!* ·
Kreps et al., *Kafka* (NetDB 2011) · franz-go by Travis Bischel.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
