# ADR-0002: Safe-offset commits and a version-by-count idempotent sink

**Status:** accepted

**Context.** At-least-once delivery plus in-memory window state creates two
hazards: (a) committing an offset before a window built from it is emitted loses
that window on crash; (b) replay re-emits results, risking double counts or a
partial replay overwriting a complete result.

**Decision.**
1. Commit only `min(next, lowest offset of any retained window)` per partition.
2. Give each result `version = event_count` and have sinks keep the highest
   version per `(window, kind, key)`.

**Why count, not wall-clock?** A wall-clock version would let a *partial* replay
(newer, but with fewer events) overwrite a complete earlier row. Window contents
are grow-only, so event count is a monotone, replay-safe version (a grow-only
register, as in CRDTs). Found by reasoning about a window whose events straddle the
committed offset: replay sees only the later events.

**Consequences.** Commits lag by up to `AllowedLateness` (more replay after a
crash, never less correctness). Sinks need only an "upsert if version greater"
primitive — ClickHouse `ReplacingMergeTree(version)` provides it natively.
Reads must use `FINAL` until background merges run.

**Rejected.** Transactional exactly-once (Kafka EOS + transactional sink): heavier
and not supported by ClickHouse.
