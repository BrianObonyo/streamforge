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
                     │ safe offsets                              │ rows (idempotent upsert)
                     ▼                                           ▼
              commit to broker                         ClickHouse (ReplacingMergeTree)
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
| `storage` | `Store` interface; `MemStore`; ClickHouse over HTTP |
| `processor` | the engine: rebalance handling, tasks, commits, shutdown |
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

### 3.3 Crashes → commit only what is safe to forget
Delivery is at-least-once. The subtlety: a record can't be forgotten while any
window it contributed to is still in memory, because a crash would lose that
window. So each partition commits `min(next, lowest offset held by any retained
window)` — the **safe offset**. After a crash the new owner resumes there and
rebuilds exactly the state that was lost.

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
  reassigned the partition and a commit would be fenced.

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

## 6. Testing strategy

* Unit tests per package, including property-style checks (jump-hash key movement,
  monoid order-independence, backoff bounds).
* `tests/integration`: chaos run — disorder + duplicates + garbage + sink outage +
  **hard-killed group member** — then asserts stored totals equal unique events
  exactly. Also asserts backpressure engages and graceful shutdown commits to log end.
* `cmd/demo` repeats that proof interactively.
* ClickHouse code is tested against an `httptest` server (wire format,
  parameterisation); **not yet** against a real ClickHouse.
