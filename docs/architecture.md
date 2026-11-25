# StreamForge architecture

> Project: StreamForge — Brian Obonyo. Design references are credited inline.

## 1. Data flow

```
 generator / HTTP ─▶ Publisher ─▶ Broker topic "events" (N partitions)
                                        │ key = sensor/user/ticker  (same key ⇒ same partition)
                                        ▼
                         consumer group: M Processor members
                                        │ broker assigns partitions to members
                                        ▼
        ┌────────────── per owned partition: partitionTask (1 goroutine) ──────────────┐
        │ bounded Queue ─▶ decode ─▶ validate ─▶ dedup ─▶ Aggregator (windows+watermark) │
        │       ▲                                              │ every tick               │
        │  pause/resume at high/low water                      ▼                          │
        │                                      Collect ─▶ Sink(retry+breaker) ─▶ Ack      │
        └────────────────────────────────────────────────────────────────────────────────┘
                     │ checkpoint then commit                     │ rows (idempotent upsert)
                     ▼                                            ▼
             broker offsets ◀────────────────────────── ClickHouse (state + results)
                                                                 │
                                                     Analytics API ─▶ Grafana / clients
```

## 2. Package map

| Package | Responsibility |
|---|---|
| `events` | Event model, wire format, validation (incl. future-skew guard) |
| `partitioning` | key → partition: FNV-1a, Jump Consistent Hash, round-robin |
| `streaming` | `Broker`/`Producer`/`Consumer` interfaces; in-memory broker; Redpanda adapter (`-tags redpanda`) |
| `backpressure` | bounded queue with hysteresis, token bucket, AIMD controller |
| `aggregation` | window assigners, watermark, aggregator, two-phase emit, safe offset |
| `reliability` | dedup, jittered retry, circuit breaker, DLQ |
| `storage` | `Store` and `CheckpointStore` interfaces; `MemStore`; ClickHouse over HTTP |
| `processor` | the engine: rebalance handling, tasks, durable checkpoints, commits, shutdown |
| `ingestion` | workload generator, publisher, HTTP ingestion API |
| `api` | analytics HTTP API, health, `/metrics` |
| `metrics` | dependency-free Prometheus registry |

## 3. The five problems this design is organised around

### 3.1 Out-of-order data → event time + watermarks
Windows are defined on `event_time`. A per-partition **watermark**
`W = max(event_time) − MaxOutOfOrder` declares "nothing older than W is expected";
a window `[s,e)` fires when `W ≥ e`. After firing it is retained for
`AllowedLateness` and **re-emitted** if late events refine it. Beyond that, events
are counted as `too_late` (optionally dead-lettered). Idle partitions fall back to
processing time so their windows don't freeze. *(Akidau et al., The Dataflow Model, 2015.)*

### 3.2 Overload → pressure is propagated, never absorbed
Every stage boundary is a bounded queue. At the **high-water** mark the partition is
paused **at the broker** (records wait on disk, not in the heap); at the
**low-water** mark it resumes. The gap is hysteresis. An optional AIMD controller
additionally shrinks the consume rate on congestion and probes upward when healthy.
External HTTP clients get `429 + Retry-After`. *(Chiu & Jain, 1989.)*

### 3.3 Crashes → persist state before advancing offsets
Delivery is at-least-once. Without a checkpoint store, each partition commits
`min(next, lowest offset held by any retained window)` — the **safe offset** —
so a restart can rebuild open state from the broker log. The production processor
uses the configured checkpoint-capable store: it saves the full partition state
(windows, watermark, dedup IDs, and next offset) before committing that next
offset. On reassignment it restores the snapshot, advances the broker offset, and
continues without rebuilding the saved state. Checkpoint failures hold back
offset commits. Details and limits: [`checkpoints.md`](checkpoints.md).

### 3.4 Replays → idempotent sinks
Replay, retry and late refinement all re-emit rows. Each row carries
`version = event_count`. Window contents only grow, so the most complete row has
the highest version and **highest version wins** (MemStore and
`ReplacingMergeTree(version)`). A partial replay can never overwrite a complete
result. See ADR-0002.

### 3.5 Bad data and flaky dependencies
Undecodable or invalid records go to the **DLQ** with their reason and original
bytes; they never block a partition. Sink writes use **jittered exponential
backoff** behind a **circuit breaker**. A failed write is never acknowledged, so
the same results are simply due again next tick — Collect/Ack is two-phase for
exactly this reason.

## 4. Concurrency model

* `Processor.Run` is a single goroutine: poll → reconcile → dispatch → commit.
* Each partition has exactly **one** task goroutine that owns its aggregator and
  dedup cache. **No locks on the hot path.**
* Cross-goroutine state is limited to atomics (`safe`, `open`, `consumed`), the
  queue (a channel), the shared circuit breaker, and the metrics registry.
* Revocation cancels a task without flushing or committing: the broker has already
  reassigned the partition and a commit would be fenced. The successor restores the
  latest durable checkpoint and commits its offset before processing.

## 5. Delivery guarantees (precisely)

| Property | Guarantee |
|---|---|
| Delivery | at-least-once from the broker |
| Counting | each unique event counted exactly once **provided** duplicates are caught by the dedup cache *or* superseded by the version rule |
| Ordering | per partition (so per key) |
| Loss | none on crash, rebalance, sink outage; records beyond `AllowedLateness` are *deliberately* excluded and counted |

**Known limits (stated, not hidden):**
* Dedup window = `min(TTL, MaxEntries / rate)`. A duplicate arriving after its
  original was evicted from the cache is counted again in the live window; the
  version rule only protects against *under*-counted replays, not over-counted ones.
* Event totals in `/v1/summary` are exact for tumbling windows only (see `storage.Store`).
* `sf_consumer_lag` measures *commit* lag, which includes the replay span held
  back by retained windows — it is not pure processing delay.
* Watermark idle-advance can misclassify a stalled producer as a quiet one.

## 6. Error-handling matrix

Every failure has a defined detection, response and signal. "Pending" means results
are kept in memory and retried — nothing is dropped.

| Failure | Detected by | Response | Signal |
|---|---|---|---|
| Undecodable record | `DecodeAny` error | DLQ `poison_undecodable`; offset still advances | `sf_events_processed_total{outcome="poison"}` |
| Invalid event (NaN, future date…) | `Validate` | DLQ `invalid_event`; **never reaches the watermark** | `outcome="invalid"` |
| Duplicate event | dedup cache | dropped | `outcome="duplicate"` |
| Late, within lateness | aggregator | accepted; window re-emitted with higher version | `outcome="late"` |
| Late, beyond lateness | aggregator | excluded (optionally DLQ `too_late`) | `outcome="too_late"` |
| Sink transient (5xx, timeout, breaker open) | `HTTPError.Retryable`, breaker | jittered retry; batch pending; queue fills → broker paused | `sf_sink_errors_total{class="transient"}` |
| Sink permanent (4xx) | `reliability.IsPermanent` | pending for N ticks, then rows → DLQ `sink_failure`, partition unblocked | `class="permanent"`, `sf_sink_rows_dead_lettered_total` |
| ClickHouse table/db missing | code 60 / 81 | recreate schema, retry once | — |
| Broker poll error | `Poll` returns error | exponential backoff 100 ms–5 s, ±20 % jitter | `sf_poll_errors_total` |
| Publish transient | `Do` retry | jittered backoff; batch retry may duplicate (dedup covers it) | `sf_ingest_retries_total` |
| Publish permanent (unknown topic) | `ErrUnknownTopic` | fail fast, no retry | `sf_ingest_failed_total` |
| Commit fenced (rebalance race) | `ErrNotAssigned` | ignored; next loop reconciles | — |
| Processor killed (-9) | broker session timeout | successor restores checkpoint and resumes at its offset; without a checkpoint, replay starts at the safe offset | `sf_rebalances_total`, `sf_state_checkpoints_total` |
| Checkpoint write fails | checkpoint store error | hold back new offset commits; retry checkpoint on a later task tick | `sf_state_checkpoint_errors_total` |
| Overload | queue high-water | pause partition at broker; AIMD shrinks rate | `sf_paused_partitions`, `sf_throttle_rate` |
| Telemetry backend down | exporter | spans dropped, **never blocks pipeline** | tracer `Dropped`/`ExportErrors` |

Rationale and trade-offs: ADR-0007.

## 7. Wire formats and tracing

* **Two wire formats** share a topic: JSON (default, debuggable) and a compact binary
  format (7.6× faster decode); consumers sniff the first byte. ADR-0005.
* **Distributed tracing:** W3C `traceparent` travels in the event payload; spans are
  exported as OTLP/HTTP JSON to any collector. One trace follows an event from the
  producer process through the broker into the processor process. ADR-0006.

## 8. Testing strategy

* Unit tests per package, including property-style checks (jump-hash key movement,
  monoid order-independence, backoff bounds).
* `tests/integration`: chaos run — disorder + duplicates + garbage + sink outage +
  **hard-killed group member** — then asserts stored totals equal unique events
  exactly. Also asserts backpressure engages and graceful shutdown commits to log end.
* Checkpoint integration kills a processor after its state snapshot but before its
  broker offset commit, then verifies restore and exact counts.
* `cmd/demo` repeats that proof interactively.
* ClickHouse code is tested twice: against an `httptest` stub (wire format,
  parameterisation) and, when `SF_TEST_CLICKHOUSE_URL` is set, against a **real
  server** (DDL, `FINAL` dedup of unmerged versions, typed parameters, injection
  inertness, self-healing).
* The Redpanda adapter is tested with `-tags redpanda` against franz-go's `kfake`
  (real client, real Kafka protocol), including the full chaos pipeline.
* `scripts/e2e-local.sh` runs the real binaries as separate processes against real
  ClickHouse, a Kafka-protocol broker, Prometheus, k6 and an OTel Collector.
* What remains unverified is listed explicitly in `docs/VERIFICATION.md`.
