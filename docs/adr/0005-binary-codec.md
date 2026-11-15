# ADR-0005: Hand-rolled binary wire format alongside JSON

**Status:** accepted · **Measured:** see `benchmarks/RESULTS.md` (Run 2)

**Context.** Profiling showed JSON decoding was the dominant per-event cost
(~2.6 µs of a ~9 µs end-to-end event on the reference VM). The window logic itself
costs ~0.3 µs.

**Decision.** Add a compact binary encoding ("sfb1", `internal/events/binary.go`)
next to JSON. Consumers **sniff the first byte** (`0xB1` = binary, `{` = JSON), so
both formats coexist on one topic and migration needs no flag day. Producers choose
with `-codec json|binary` (default JSON, which stays human-debuggable).

**Why not protobuf / Avro / MessagePack?**
* They need third-party modules; ADR-0003 keeps the core stdlib-only, and the
  module proxy for `google.golang.org/…` is unreachable in restricted builds.
* The schema is six fields; a schema registry and codegen are cost without benefit
  at this size.
* The format is ~60 lines of codec with a documented layout.

**Safety.** Decoding is bounds-checked *before* every allocation, rejects trailing
bytes and unknown kinds, and caps field sizes (64 KiB) and attribute counts (256), so
hostile input yields an error, never a panic or attacker-sized allocation. Verified by
unit tests (every prefix of a valid message must fail) and `FuzzDecodeBinary`
(≈150 k executions, no findings).

**Measured (1 vCPU, Go 1.22):** decode 2,598 → 340 ns (7.6×), encode 1,552 → 127 ns
(12×), engine pipeline ~178 k → ~296 k events/s (1.66×).

**Trade-offs.** Not self-describing: adding a field means a new magic/version byte.
Not interoperable with external consumers without a spec — this document and the
header comment in `binary.go` are that spec. If the schema grows or other teams
consume the topic, move to protobuf (the `Codec` seam makes that local).
