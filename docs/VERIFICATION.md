# Verification record

This file states, claim by claim, **what was verified, against what, and what was
not**. It exists because "it compiles" and "it works" are different claims, and
because several components of this project talk to external systems that are easy to
mock incorrectly.

Environment: Linux x86-64, **1 vCPU**, 4 GB RAM, Go 1.22.2, **no Docker**.
Full log of the final end-to-end run: [`e2e-run.log`](e2e-run.log).
Reproduce it with `scripts/e2e-local.sh` (see "How to reproduce").

## Summary

| Component | Status | Verified against |
|---|---|---|
| ClickHouse store | ✅ **verified on a real server** | ClickHouse **26.8.37 LTS** (official build) |
| Redpanda adapter | 🟡 **verified on a real Kafka-protocol cluster; not on Redpanda itself** | franz-go **kfake** (real client, real wire protocol) |
| Kafka TLS/SASL client configuration | 🟡 **client protocol tests pass; live SCRAM CI configured, pending run** | kfake: TLS/mTLS, PLAIN, SCRAM-SHA-256/512, invalid credentials; Redpanda CI: SCRAM-SHA-256 |
| Processor state checkpointing | 🟡 **in-memory recovery passes; live-service CI test added, pending run** | integration test with real Redpanda + ClickHouse in CI |
| Full pipeline as separate processes | ✅ verified | real ClickHouse + Kafka-protocol broker + 2 processors + producer + API |
| Crash recovery (`kill -9`) | ✅ verified, exact | real processes, real ClickHouse |
| k6 load script | ✅ verified (smoke scale) | k6 **2.4.0** against the real producer binary |
| Prometheus config & every dashboard query | ✅ verified | Prometheus **3.16.0**, real scrapes |
| OpenTelemetry tracing | ✅ verified (OTLP/HTTP) | OpenTelemetry Collector **otelcol-contrib 0.165.0** |
| Grafana dashboard JSON | 🟡 queries verified; **never loaded into Grafana** | — |
| Docker Compose stack | 🟡 **never run**; statically checked only | `scripts/check-compose.py` |
| Image tags (Redpanda, ClickHouse 24.8, Prometheus 2.54, Grafana 11.2, Jaeger 1.62) | ⬜ **unverified** | CI jobs defined, not yet executed |
| Throughput / parallel scaling | ⬜ **not measured** | 1 vCPU cannot show it |

## 1. ClickHouse store — verified on a real server

`internal/storage/clickhouse_live_test.go` runs when `SF_TEST_CLICKHOUSE_URL` is set.
Run against a real ClickHouse 26.8.37; all pass:

* **Schema:** `EnsureSchema` is valid DDL on a real server and idempotent.
* **The central correctness claim (ADR-0002) holds on the real engine.** Four INSERTs
  of the same window with versions 10, 4, 12, 7 land as separate parts; a `FINAL` read
  returns exactly the version-12 row — *before any background merge*.
* **Typed parameters** (`{from:DateTime64(3,'UTC')}`) are accepted; time-range, kind and
  key filters return correct rows.
* **Injection attempt is inert:** a key of `' OR 1=1; DROP TABLE … --` matches nothing
  and the table is intact.
* **Late-stats** round-trip and aggregate correctly; empty ranges return zeros.
* **Error classification** (found *by* this verification): a missing table returned a
  404 that the original code retried forever. Now: `HTTPError` with parsed ClickHouse
  code, permanent vs transient, and **self-healing** — dropping the table mid-run
  recreates it and the write succeeds (`TestLiveSelfHealsMissingTable`).
* **Least privilege:** the end-to-end run uses a ClickHouse user granted only
  `CREATE, INSERT, SELECT` — proving those three privileges suffice.

Not verified: ClickHouse **24.8** (the tag compose pins) — CI job `clickhouse-live`
runs the same tests against it.

## 2. Redpanda adapter — real protocol, not real Redpanda

`internal/streaming/redpanda_test.go` (build tag `redpanda`) uses the **real franz-go
client** against **kfake**, franz-go's in-process Kafka-protocol cluster. Verified:
topic creation (idempotent), key→partition stability, produce/consume, **manual commit
and resume with exact redelivery counts**, group rebalance (2 members split 4
partitions), pause/resume, `ErrClosed` after close, batched produce (500 messages).

`tests/integration/kafka_test.go` runs the **entire chaos pipeline through the adapter**
— 30 % disorder, duplicates, garbage, sink outage, one member hard-killed — and the
stored total is exact (2,140 unique events).

**What this does not prove:** Redpanda-specific replication performance and production
rebalance behavior. `kfake` is a simplified Kafka. CI job `redpanda-live` exercises the
real broker with SASL/SCRAM authentication and runs the checkpoint recovery test
against real ClickHouse; this job has not been run from the current local environment.

A defect found here: franz-go's default 45 s session timeout would leave a killed
member's partitions orphaned for 45 s. The adapter now defaults to 10 s / 3 s
heartbeat (`SF_KAFKA_SESSION_TIMEOUT`, `SF_KAFKA_HEARTBEAT_INTERVAL`).

### Kafka TLS and SASL

The adapter applies optional TLS and SASL configuration to admin, producer, and
consumer clients. Build-tagged tests verify TLS certificate validation, PLAIN over
TLS, SCRAM-SHA-256, SCRAM-SHA-512, rejection of invalid credentials, and invalid
configuration handling against kfake. `PLAIN` without TLS fails during adapter
initialization.

The `redpanda-live` CI job configures an ephemeral SCRAM-SHA-256 credential on a real
Redpanda listener, then verifies authenticated topic creation, produce, consume, and
offset commit. TLS/mTLS remain protocol-tested with kfake; real-broker TLS listener
configuration, other SASL mechanisms, and production ACL policy require
deployment-specific verification.

### Processor state checkpoints

The processor saves a versioned partition snapshot to `processor_checkpoints`
before making its next processed offset committable. A crash-recovery integration
test deliberately disables periodic broker commits, waits until the snapshot
covers the input, kills the processor, and verifies a successor restores and
commits the checkpoint offset without changing exact stored counts. Aggregator and
deduper snapshots are unit-tested independently. ClickHouse checkpoint
parameterization, JSON encoding, and decoding are exercised with an HTTP test
server.

The `redpanda-live` CI job additionally runs `TestLiveCheckpointRecoveryAcrossServices`
against real Redpanda (SCRAM) and ClickHouse: it saves state before a broker commit,
closes the first consumer, verifies the successor commits the restored checkpoint
offset, and checks the exact aggregate total after processing more input.

Not verified: multi-replica failure timing or checkpoint-table retention/compaction
under sustained load. The live CI job is configured but has not been run from the
current local environment.

## 3. End-to-end: real processes, real infrastructure

`scripts/e2e-local.sh` starts, as separate OS processes: ClickHouse 26.8 (authenticated,
least-privilege user), a Kafka-protocol broker (`tests/tools/kafkasim`), the OTel
Collector, Prometheus, **two `processor`s**, the `api`, and the `producer`. Final run
(all checks passed — see `e2e-run.log`):

**Phase A — crash recovery.** A deterministic 30,000-message stream (seed 99; ground
truth from `tests/tools/genstats`: **29,231 unique**, 612 duplicates, 157 garbage) with
10 % disorder. At t+4 s one processor is **`kill -9`'d** (no flush, no commit, no
leave-group); a replacement starts 6 s later.
→ ClickHouse holds **exactly 29,231** events; stable 3 s later (no drift); a raw
`FINAL` sum agrees with the API.

**Phase B — load.** k6 drives the HTTP ingestion API (975 requests × 50 events,
~48.7 k unique events, 0 failed, 0 throttled; **p95 64.6 ms, p99 88.6 ms**, on one core
shared with the entire stack (ClickHouse, broker, Prometheus, collector, 3 Go services)).
→ ClickHouse holds exactly **29,231 + 48,750 = 77,981**.

## 4. Prometheus and Grafana

Prometheus 3.16.0 scraped two processors, the producer and the API. **All 15 queries in
`deployments/grafana/dashboards/streamforge.json` are valid PromQL and returned data.**
The Grafana dashboard JSON itself was **never loaded into Grafana** (no Grafana binary
was obtainable); panel layout and datasource wiring are unverified.

## 5. OpenTelemetry

The Collector received ~7,970 spans over OTLP/HTTP (JSON). **1,613 `publish→process`
links crossed a process boundary** (producer process → processor process), proving
W3C `traceparent` propagation through the broker in the payload. `sink.write` spans
also arrived. Verified against the Collector only — not Jaeger, Tempo, or the official
SDK's propagators.

## 6. Docker Compose — static checks only

`scripts/check-compose.py` verifies, **without Docker**: the YAML parses; every
`depends_on` target exists; each service's `command:` flags are accepted by the real
binary (a typo'd flag exits 2 — **negative-tested**: injecting `-max-oooo` and
`SF_CLICKHOUSE_USR` is caught); every `SF_*`/`OTEL_*` variable is read by the code;
Prometheus targets match services and ports. This found and fixed one real issue
(`api` needlessly depended on Redpanda).

**Never verified:** that the images start, that Redpanda's `rpk` healthcheck string is
right, that ClickHouse's env-var user creation works as written, Grafana provisioning,
container networking. Treat `make up` as untested until you have run it once.

## 7. Performance

See `benchmarks/RESULTS.md` (Run 2). Verified improvements (same machine, same
conditions): binary decode **7.6×**, encode **12×**, engine pipeline **1.66×**, batched
Kafka produce **~27×** (over loopback kfake). **Not measured:** multi-core scaling,
real-Redpanda or replicated throughput, sustained-load latency percentiles.

## 8. Unit / integration test inventory

`go test -race ./...` passes, plus `-tags redpanda`. Integration tests cover: exact
aggregation under chaos (disorder + duplicates + garbage + sink outage + killed member),
backpressure engagement, graceful shutdown committing to log end, permanent sink error
recovering without loss, permanent sink error dead-lettering (payload decodable,
partition unblocked), binary codec end-to-end, and a trace linking publish→process.
Fuzzing: `FuzzDecodeBinary` (~152 k execs) and `FuzzParseTraceparent`, no findings.

## How to reproduce

```bash
# 1. real ClickHouse tests
SF_TEST_CLICKHOUSE_URL=http://localhost:8123 go test -run Live -v ./internal/storage/
# 2. adapter on a real Kafka-protocol cluster (kfake) and on a real Redpanda
go test -tags redpanda -race ./internal/streaming/ ./tests/integration/
SF_TEST_BROKERS=localhost:19092 go test -tags redpanda -run LiveBroker -v ./internal/streaming/
# 3. full multi-process run (needs clickhouse running + k6, prometheus, otelcol-contrib)
go build -tags redpanda -o bin/ ./cmd/... ./tests/tools/...
BIN=./bin K6=k6 PROM=prometheus OTEL=otelcol-contrib scripts/e2e-local.sh
# 4. compose static checks
python3 scripts/check-compose.py ./bin
```
