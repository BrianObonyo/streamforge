# Deterministic replay simulator

The simulator replays a caller-supplied newline-delimited JSON event file through
partition assignment, event decoding and validation, per-partition deduplication,
event-time aggregation, and the in-memory sink. It does not contain an embedded
sample dataset or contact external services.

```sh
go run ./cmd/simulator \
  -input path/to/events.jsonl \
  -partitions 6 \
  -window 10s \
  -max-ooo 2s \
  -lateness 5s
```

Each non-empty line must be one JSON object matching `events.Event`. Input order is
the arrival order. The simulator derives its virtual arrival time from `ingest_time`
when present, otherwise `event_time`; repeated or missing timestamps advance by
`-arrival-step`. Use `-clock-start` to instead supply a virtual clock origin and
advance by `-arrival-step` for every record. This makes future-skew validation
reproducible when the input includes ingest timestamps or the clock is explicitly
configured.

The `-sink-failures` and `-sink-retries` options inject a deterministic number of
initial in-memory sink failures. Retries do not sleep, so the same input and
options produce the same sorted JSON report.

This tool exercises StreamForge's deterministic processing primitives in process;
it does not emulate broker networking, consumer-group coordination, processor
scheduling, or TLS/SASL. Those remain separate verification targets.

The CI suite separately guards two replay invariants: an open window must hold
back its safe commit offset, and a stale aggregate written after a newer version
must not replace it. The crash-recovery integration test checks the end-to-end
count after a processor is killed and its records are replayed.
