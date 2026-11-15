# Recorded benchmark results

Append-only. Each entry states the full context so it can be reproduced or
disproved.

---

## Run 1 — engine micro-benchmarks and in-process pipeline

| Field | Value |
|---|---|
| Date | 2026-11-14 |
| Go | go1.22.2 linux/amd64 |
| CPU | Intel Xeon @ 2.10 GHz — **1 vCPU** (shared sandbox VM) |
| Command | `go test -bench . -benchmem -run '^$' -benchtime=1s ./benchmarks/` |
| Scope | Go engine only; in-memory broker and store. **No Redpanda, no ClickHouse.** |

| Benchmark | ns/op | allocs/op | Throughput |
|---|---:|---:|---|
| AggregatorAdd (tumbling, 256 keys) | 414 | 1 | ~2.4 M events/s |
| SlidingAssign (1m / 10s) | 270 | 1 | |
| Dedup lookup (steady-state eviction) | 530 | 2 | |
| Partitioner: hash (FNV-1a) | 13.8 | 0 | |
| Partitioner: jump hash | 24.6 | 0 | |
| Event decode + validate (JSON) | 3,236 | 8 | 41 MB/s |
| **Pipeline, 1 partition** | 9,574 | 21 | **~104 k events/s** |
| **Pipeline, 4 partitions** | 8,678 | 21 | **~115 k events/s** |

### Reading these numbers honestly

* **JSON decoding is the dominant per-event cost** (3.2 µs of ~9 µs). The window
  logic itself is ~0.4 µs. A binary encoding (protobuf / msgpack) or a faster JSON
  decoder is the obvious first optimisation; see roadmap.
* **The 4-partition result is NOT evidence of parallel scaling.** This VM has one
  vCPU, so four partition goroutines time-slice on a single core. The ~10 %
  difference is scheduling noise. Re-run on a multi-core machine before drawing
  any scaling conclusion.
* These figures bound the *Go code*, not the system. Real end-to-end throughput
  will be lower once network, Redpanda replication and ClickHouse inserts are in
  the path.

---

## Run 2 — binary codec, batched produce, and the first real-protocol numbers

| Field | Value |
|---|---|
| Date | 2026-11-15 |
| Go | go1.22.2 linux/amd64 |
| CPU | Intel Xeon @ 2.30 GHz — **1 vCPU** (shared sandbox VM; ClickHouse server idle in background) |
| Command | `go test -tags redpanda -bench 'Decode\|Encode\|AggregatorAdd\|Pipeline\|KafkaProduce' -benchmem -run '^$' -benchtime=1500ms ./benchmarks/` |
| Scope | engine micro-benchmarks, in-memory pipeline, and franz-go → **kfake** (real Kafka wire protocol over loopback; *not* Redpanda) |

### Codec (ADR-0005) — same event, same machine

| | JSON | Binary | Speed-up |
|---|---:|---:|---:|
| Decode (via `DecodeAny`) | 2,598 ns · 15 allocs | **340 ns** · 6 allocs | **7.6×** |
| Encode | 1,552 ns · 7 allocs | **127 ns** · 1 alloc | **12×** |
| Engine pipeline (events/s) | ~178 k | **~296 k** | **1.66×** |

The pipeline gain is smaller than the decode gain because decode is only part of
per-event cost; window folding, dedup and metrics are unchanged (Amdahl).

### Batched produce over the Kafka protocol

| Producer mode | msgs/s | ns/msg | allocs/msg |
|---|---:|---:|---:|
| one `Produce` per message (sync) | ~39 k | 25,440 | 69 |
| `ProduceBatch` of 100 | **~1.08 M** | 926 | 8 |

**≈27×.** Per-message synchronous produce pays one network round trip each; batching
pipelines them. Absolute numbers are optimistic (in-process loopback, no replication,
no disk) — the *ratio* is the finding, and it is why `Generator.Run` and
`Publisher.PublishBatch` batch by default. Against a real replicated Redpanda the
absolute rate will be far lower and per-message sync produce will suffer more, since
a real round trip costs more than loopback.

### Other results (unchanged code paths, re-measured)

| Benchmark | Result |
|---|---|
| AggregatorAdd | 305 ns/op, 1 alloc |
| Pipeline, 1 partition (per-message produce) | ~101 k events/s |
| Pipeline, 4 partitions (per-message produce) | ~127 k events/s |

### What these numbers do and do not show

* ✅ The codec and batching improvements are real and large, measured on one machine
  under identical conditions.
* ❌ **Parallel scaling is still unmeasured.** One vCPU means 4 partitions time-slice
  one core; the 101 k vs 127 k difference is scheduling noise, not scaling.
* ❌ These are not Redpanda or ClickHouse throughput figures. The end-to-end run in
  `docs/VERIFICATION.md` shows the real stack *works* and is *exact*, but it is a
  correctness run on a contended single core, not a capacity test.
* ❌ No percentile latency under sustained load is claimed; the one k6 smoke figure in
  VERIFICATION.md is from a 25-second run sharing one core with six other processes.
