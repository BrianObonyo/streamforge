# ADR-0007: Classify errors as transient vs permanent; never wedge a partition

**Status:** accepted

**Context.** Verification against a real ClickHouse showed the original sink error
handling retried *every* failure identically. A missing table (HTTP 404, code 60) or a
rejected payload will fail forever; retrying it just burns the budget and masks the
real error behind "circuit open".

**Decision.**
1. **Classify at the source.** `storage.HTTPError` carries the ClickHouse exception
   code; 5xx/408/429 and network errors are *transient*; other 4xx are wrapped with
   `reliability.Permanent`. `reliability.IsPermanent` survives `%w` wrapping.
2. **`reliability.Do` stops immediately on permanent errors** and keeps the marker.
3. **The circuit breaker ignores permanent errors** — it guards *availability*; a
   permanent error proves the dependency answered.
4. **Self-heal the one recoverable permanent case:** table/database missing (codes
   60/81) → recreate schema, retry once.
5. **Processor policy.** Transient → batch stays pending, retried each tick
   (backpressure absorbs the delay). Permanent → pending for
   `MaxPermanentSinkFailures` (default 5) consecutive ticks so an operator can fix the
   cause without data loss; then rows are preserved in the DLQ (`sink_failure`) and the
   partition is unblocked.
6. **Poll errors back off exponentially** (100 ms → 5 s, ±20 % jitter, reset on success).
7. **Publisher** fails fast on `ErrUnknownTopic`/`ErrClosed`.

**Consequences.** Failures are visible by class (`sf_sink_errors_total{class=…}`),
no error can silently spin, and no failure can stall a partition indefinitely.
**Trade-off:** dead-lettering after N permanent failures trades availability for
completeness of the *store* — the rows are not lost, but they are not queryable until
replayed from the DLQ.
