# ADR-0001: Event-time windowing with bounded-out-of-orderness watermarks

**Status:** accepted

**Context.** Sensors and clients deliver data late and out of order. Windowing on
arrival time produces wrong answers that change with network conditions.

**Decision.** Window on `event_time`. Track a per-partition watermark
`max(event_time) − MaxOutOfOrder`. Fire a window when the watermark passes its end;
keep it for `AllowedLateness` and re-emit on refinement; count the rest as too-late.
Fall back to processing time on idle partitions.

**Alternatives.** *Processing-time windows* — simple but non-deterministic.
*Perfect watermarks* — impossible without source cooperation. *Per-key watermarks*
— better completeness, much more state; unnecessary because a key is pinned to one
partition.

**Consequences.** One tunable pair (slack, lateness) trades latency against
completeness. Per-partition watermarks mean no cross-partition min-merge is needed.
Idle fallback trades a small misclassification risk for liveness.

**References.** Akidau et al., "The Dataflow Model", VLDB 2015.
