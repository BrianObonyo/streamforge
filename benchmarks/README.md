# Benchmarks

Run:

```bash
go test -bench . -benchmem -run '^$' -benchtime=2s ./benchmarks/
```

## Rules for publishing numbers

A number without its context is noise. Whenever you record a result, include:

| Field | How to get it |
|---|---|
| Commit | `git rev-parse --short HEAD` |
| Go version | `go version` |
| CPU / cores | `lscpu \| grep -E 'Model name\|^CPU\(s\)'` |
| GOMAXPROCS | `go env GOMAXPROCS` or the env var |
| Workload | benchmark name + flags (seed, rate, partitions) |
| Duration | `-benchtime` |

## What each benchmark measures

| Benchmark | Scope |
|---|---|
| `AggregatorAdd` | one event folded into a tumbling window (no I/O) |
| `SlidingAssign` | window assignment for a 1m/10s sliding window |
| `Dedup` | duplicate-detection lookup at steady-state eviction |
| `Partitioners` | hash vs jump-consistent-hash key routing |
| `EventDecodeValidate` | JSON decode + validation of one event |
| `Pipeline` | **engine only**: in-memory broker → processor → in-memory store |

`Pipeline` deliberately excludes Redpanda and ClickHouse; it bounds what the Go
code itself can do. End-to-end numbers against real infrastructure come from the
k6 + Docker Compose setup in `tests/load/` and must be recorded separately.

## Recorded results

See `RESULTS.md` (append new runs; never overwrite old ones).
