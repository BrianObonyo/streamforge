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

For reproducible replay of your own newline-delimited event data, see
[`docs/simulator.md`](docs/simulator.md). The simulator requires an input file and
does not ship or embed a sample dataset.

## Status — what is built, and exactly how it was verified

Full evidence, versions and reproduction steps: [`docs/VERIFICATION.md`](docs/VERIFICATION.md)
(final end-to-end log: [`docs/e2e-run.log`](docs/e2e-run.log)).

| Area | State | Verified against |
|---|---|---|
| Event model, partitioning, windows, watermarks, backpressure, dedup, DLQ | ✅ | unit + chaos integration tests, race detector |
| Crash recovery (`kill -9` mid-stream) | ✅ exact | real separate processes + real ClickHouse |
| **ClickHouse store** | ✅ | **real ClickHouse 26.8 LTS** (DDL, `FINAL` dedup, typed params, self-heal) |
| **Redpanda adapter** | 🟡 | **real franz-go client over the real Kafka protocol (kfake)**, incl. full chaos pipeline. **Not yet run on Redpanda itself** |
| k6 load script | ✅ | k6 2.4.0 vs the real producer binary |
| Prometheus config + all 15 dashboard queries | ✅ | Prometheus 3.16 scraping real processes |
| **OpenTelemetry tracing** | ✅ | real OTel Collector 0.165 — one trace spans producer → broker → processor processes |
| Grafana dashboard JSON | 🟡 | queries verified; JSON **never loaded into Grafana** |
| Docker Compose stack | 🟡 | **never run**; flags/env/targets statically checked (negative-tested) |
| Container image tags | ⬜ | unverified — CI jobs defined (`clickhouse-live` on 24.8, `redpanda-live`) |
| Throughput & multi-core scaling | ⬜ | **not measured** (1-vCPU sandbox) — see below |

**Headline result** (real processes, real ClickHouse, one processor `kill -9`'d
mid-stream): 29,231 unique events produced → **29,231 counted, exactly**; then 48,750
more via k6 → **77,981, exactly**.

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
5. **Fault tolerance** — safe-offset commits, dedup, jittered retry + circuit breaker, dead-letter queue, idempotent versioned sink, **transient-vs-permanent error classification** (a persistent sink failure dead-letters its rows instead of wedging a partition), self-healing ClickHouse schema, exponential poll backoff. Full matrix: [`docs/architecture.md` §6](docs/architecture.md).
6. **Observability** — Prometheus metrics, a 10-panel Grafana dashboard, and **distributed tracing** (W3C `traceparent` in the payload → OTLP/HTTP, dependency-free).
7. **Real-time analytics** — events/interval, avg/max, distribution by category, throughput, late-arrival stats.

## Quick start

```bash
make test            # unit tests
make integration     # chaos end-to-end tests (crash, dupes, sink outage, backpressure)
make demo            # the 20-second proof above
make simulator INPUT=path/to/events.jsonl # deterministic replay of caller-provided events
make bench           # micro + engine benchmarks
make build           # binaries in ./bin  (stdlib-only core)
make redpanda-build  # same, with the Redpanda/Kafka adapter
make e2e             # real processes vs real ClickHouse/Prometheus/k6/OTel (needs those binaries)
make verify-live     # live-service tests (set SF_TEST_CLICKHOUSE_URL / SF_TEST_BROKERS)
make up              # full stack via Docker Compose (NEVER RUN — see status table)
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

Measured on **one vCPU** (Go 1.22, details in [`benchmarks/RESULTS.md`](benchmarks/RESULTS.md)):

| Optimisation | Before → after |
|---|---|
| Binary codec decode (vs JSON) | 2,598 → **340 ns** (7.6×) |
| Binary codec encode | 1,552 → **127 ns** (12×) |
| Engine pipeline, in memory | ~178 k → **~296 k events/s** |
| Batched Kafka produce (loopback kfake) | ~39 k → **~1.08 M msgs/s** (27×) |

**Not measured, and not claimed:** multi-core scaling, throughput against a real
replicated Redpanda, sustained-load latency percentiles. The end-to-end run is a
*correctness* run on a contended core, not a capacity test.

## Project structure

```
cmd/            producer · processor · api · demo
internal/       events partitioning streaming backpressure aggregation reliability
                storage processor ingestion api metrics tracing config
tests/          integration/ (chaos)  load/ (k6)  tools/ (kafkasim, genstats)
scripts/        e2e-local.sh  check-compose.py  send-sample.sh
benchmarks/     Go benchmarks + recorded results
deployments/    docker-compose, Prometheus, Grafana
docs/           architecture.md  VERIFICATION.md  e2e-run.log  adr/ (7 decisions)
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
- [x] Prometheus + Grafana + OpenTelemetry tracing
- [x] Integration and failure-recovery tests (incl. real-process crash test)
- [x] Verified against a real ClickHouse and a real Kafka-protocol cluster
- [x] Binary wire format (7.6× faster decode)
- [ ] Extend the deterministic replay simulator with broker/process failure scenarios
- [ ] Add TLS, SASL, and service authentication for broker connections
- [ ] Add durable state checkpointing for windows and deduplication
- [ ] Run the adapter against **real Redpanda** (`make verify-live`, CI `redpanda-live`)
- [ ] Execute the Docker Compose stack once; load the dashboard into Grafana
- [ ] Multi-core and real-cluster throughput benchmarks

## Credits

Ideas this project builds on: Akidau et al., *The Dataflow Model* (VLDB 2015) ·
Lamping & Veach, *Jump Consistent Hash* (2014) · Chiu & Jain, AIMD (1989) ·
Brooker, *Exponential Backoff and Jitter* (AWS, 2015) · Nygard, *Release It!* ·
Kreps et al., *Kafka* (NetDB 2011) · W3C Trace Context & OTLP specs · franz-go (incl. kfake) by Travis Bischel.

## License

Apache License 2.0 — see [LICENSE](LICENSE).
