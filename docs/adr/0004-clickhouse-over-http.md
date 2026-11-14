# ADR-0004: ClickHouse via the HTTP interface with server-side parameters

**Status:** accepted

**Decision.** Talk to ClickHouse over HTTP (JSONEachRow inserts; parameterised
`SELECT … FINAL`). Identifiers are validated against `^[A-Za-z_][A-Za-z0-9_]*$`;
every client-controlled value travels as a typed `param_*` query parameter.

**Why.** No driver dependency; one POST is one INSERT (one data part, which is
what ClickHouse wants); injection-safe by construction.

**Trade-offs.** Text encoding is slower than the native protocol; acceptable
because writes are batched per window-emit tick, not per event.
