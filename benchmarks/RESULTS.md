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
