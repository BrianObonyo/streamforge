# Durable processor checkpoints

The `cmd/processor` binary uses the configured store as a checkpoint store when
that store supports checkpoints. Both the ClickHouse store and in-memory store
implement the interface; only ClickHouse checkpoints survive process restarts.
The checkpoint table (`processor_checkpoints`) is created by the existing
ClickHouse schema setup.

Each checkpoint is scoped by consumer group, topic, and partition. It contains:

- The next broker offset to consume.
- Retained aggregation windows, their emitted/dirty state, and the watermark.
- Deduplication IDs with their original observation times.
- A configuration fingerprint for windowing, lateness, idleness, and dedup limits.

On assignment, the processor loads and validates the latest checkpoint, restores
state, then advances the broker's committed offset to the checkpoint offset before
processing new records. A mismatched configuration or corrupt checkpoint fails
the processor explicitly; it does not silently start with empty state.

Before committing a newer offset, the processor writes the matching state snapshot
to ClickHouse. If checkpoint persistence fails, it holds back further offset
commits and retries on a later task tick. This ordering means a crash before the
broker commit can recover the already-written snapshot; prefetched records covered
by the checkpoint are skipped. The ClickHouse `ReplacingMergeTree` revision and
`FINAL` reads make the latest checkpoint available before background merges.

The processor uses the same ClickHouse credentials as its analytics sink. Grant
the processor user `CREATE`, `INSERT`, and `SELECT` on the configured database
and checkpoint table. Checkpoints currently have no automatic TTL; plan storage
retention for old consumer groups and partitions.

## Limits

- Checkpoints preserve processor memory state, not the broker log or sink data.
- The broker must retain records that have not yet been checkpointed/committed.
- Keep the checkpoint-relevant configuration stable for a consumer group. To
  intentionally change window, lateness, or dedup settings, start a new group or
  provide a state migration; the fingerprint prevents unsafe implicit restores.
- This is not a transaction spanning ClickHouse and Kafka. The snapshot is durable
  before the offset commit; a failed commit is retried by restoring the snapshot.
- Integration coverage uses the in-memory checkpoint store and also includes a
  live-service test that combines ClickHouse checkpoints with a Redpanda consumer
  crash/restart. The `redpanda-live` CI job runs that test against the pinned
  service images; the job itself must pass in GitHub Actions before claiming
  production-cluster verification.
