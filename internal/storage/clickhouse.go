package storage

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
)

// CHConfig configures the ClickHouse store.
type CHConfig struct {
	URL      string // e.g. http://localhost:8123
	Database string // default "streamforge"
	Table    string // results table, default "window_results"
	LateTbl  string // late-stats table, default "late_stats"
	User     string
	Password string
	Timeout  time.Duration
}

// ClickHouse implements Store over ClickHouse's HTTP interface
// (https://clickhouse.com/docs/en/interfaces/http).
//
// Why HTTP instead of the native protocol? It needs no third-party driver, works
// through any proxy, batches naturally (one POST = one INSERT = one data part),
// and supports server-side typed query PARAMETERS — which is how every value that
// originates from a client is passed here. SQL text is never built from user
// input; the only interpolated strings are identifiers validated by identRe.
type ClickHouse struct {
	cfg CHConfig
	hc  *http.Client
}

var identRe = regexp.MustCompile(`^[A-Za-z_][A-Za-z0-9_]*$`)

// NewClickHouse validates the config and returns a store (no network I/O yet).
func NewClickHouse(cfg CHConfig) (*ClickHouse, error) {
	if cfg.Database == "" {
		cfg.Database = "streamforge"
	}
	if cfg.Table == "" {
		cfg.Table = "window_results"
	}
	if cfg.LateTbl == "" {
		cfg.LateTbl = "late_stats"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 10 * time.Second
	}
	if cfg.URL == "" {
		return nil, fmt.Errorf("clickhouse: URL is required")
	}
	for _, id := range []string{cfg.Database, cfg.Table, cfg.LateTbl} {
		if !identRe.MatchString(id) {
			return nil, fmt.Errorf("clickhouse: %q is not a safe identifier", id)
		}
	}
	return &ClickHouse{cfg: cfg, hc: &http.Client{Timeout: cfg.Timeout}}, nil
}

// do POSTs `body` to ClickHouse. `query` (if set) travels in the URL — the form
// required for INSERT, where the body is the data; otherwise the body is the SQL.
func (c *ClickHouse) do(ctx context.Context, query string, params map[string]string, body io.Reader) ([]byte, error) {
	u, err := url.Parse(c.cfg.URL)
	if err != nil {
		return nil, err
	}
	qs := u.Query()
	if query != "" {
		qs.Set("query", query)
	}
	for k, v := range params {
		qs.Set("param_"+k, v)
	}
	// Plain JSON numbers for 64-bit ints (default quotes them as strings).
	qs.Set("output_format_json_quote_64bit_integers", "0")
	u.RawQuery = qs.Encode()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), body)
	if err != nil {
		return nil, err
	}
	if c.cfg.User != "" {
		req.Header.Set("X-ClickHouse-User", c.cfg.User)
		req.Header.Set("X-ClickHouse-Key", c.cfg.Password)
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("clickhouse request: %w", err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 64<<20))
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("clickhouse: HTTP %d: %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	return b, nil
}

// EnsureSchema creates the database and tables if missing.
//
// window_results uses ReplacingMergeTree(version): when parts merge, ClickHouse
// keeps one row per ORDER BY key — the one with the greatest `version`. That is
// the storage-level half of our idempotency story (the other half is that
// version = event count, so fuller aggregates always win).
//
// NOTE: merges are ASYNCHRONOUS. Until one runs, duplicates coexist on disk, so
// every read below uses FINAL to collapse them at query time.
func (c *ClickHouse) EnsureSchema(ctx context.Context) error {
	db, tbl, late := c.cfg.Database, c.cfg.Table, c.cfg.LateTbl
	stmts := []string{
		fmt.Sprintf("CREATE DATABASE IF NOT EXISTS %s", db),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
			window_start DateTime64(3, 'UTC'),
			window_end   DateTime64(3, 'UTC'),
			kind         LowCardinality(String),
			key          String,
			event_count  UInt64,
			sum_value    Float64,
			min_value    Float64,
			max_value    Float64,
			version      UInt64,
			partition    UInt32
		) ENGINE = ReplacingMergeTree(version)
		PARTITION BY toYYYYMMDD(window_start)
		ORDER BY (kind, key, window_start, window_end)`, db, tbl),
		fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s.%s (
			ts              DateTime64(3, 'UTC'),
			partition       UInt32,
			on_time         UInt64,
			late            UInt64,
			too_late        UInt64,
			max_lateness_ms UInt64
		) ENGINE = MergeTree
		PARTITION BY toYYYYMMDD(ts)
		ORDER BY ts
		TTL toDateTime(ts) + INTERVAL 30 DAY`, db, late),
	}
	for _, s := range stmts {
		if _, err := c.do(ctx, "", nil, strings.NewReader(s)); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

const chTime = "2006-01-02 15:04:05.000"

func fmtCH(t time.Time) string { return t.UTC().Format(chTime) }

// WriteResults inserts one batch as a single JSONEachRow INSERT (one data part).
func (c *ClickHouse) WriteResults(ctx context.Context, rs []aggregation.Result) error {
	if len(rs) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rs {
		if err := enc.Encode(map[string]any{
			"window_start": fmtCH(r.Window.Start), "window_end": fmtCH(r.Window.End),
			"kind": string(r.Kind), "key": r.Key,
			"event_count": r.Stats.Count, "sum_value": r.Stats.Sum,
			"min_value": r.Stats.Min, "max_value": r.Stats.Max,
			"version": r.Version, "partition": r.Partition,
		}); err != nil {
			return err
		}
	}
	q := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", c.cfg.Database, c.cfg.Table)
	_, err := c.do(ctx, q, nil, &buf)
	return err
}

// WriteLateStats inserts late-arrival counter deltas.
func (c *ClickHouse) WriteLateStats(ctx context.Context, rows []LateStatRow) error {
	if len(rows) == 0 {
		return nil
	}
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	for _, r := range rows {
		if err := enc.Encode(map[string]any{
			"ts": fmtCH(r.TS), "partition": r.Partition, "on_time": r.OnTime,
			"late": r.Late, "too_late": r.TooLate, "max_lateness_ms": r.MaxLatenessMs,
		}); err != nil {
			return err
		}
	}
	q := fmt.Sprintf("INSERT INTO %s.%s FORMAT JSONEachRow", c.cfg.Database, c.cfg.LateTbl)
	_, err := c.do(ctx, q, nil, &buf)
	return err
}

// where builds a parameterised WHERE clause. Values go in `params`, never in SQL.
func where(q Query, timeCol string) (string, map[string]string) {
	var conds []string
	p := map[string]string{}
	if q.Kind != "" {
		conds, p["kind"] = append(conds, "kind = {kind:String}"), string(q.Kind)
	}
	if q.Key != "" {
		conds, p["key"] = append(conds, "key = {key:String}"), q.Key
	}
	if !q.From.IsZero() {
		conds, p["from"] = append(conds, timeCol+" >= {from:DateTime64(3, 'UTC')}"), fmtCH(q.From)
	}
	if !q.To.IsZero() {
		conds, p["to"] = append(conds, timeCol+" < {to:DateTime64(3, 'UTC')}"), fmtCH(q.To)
	}
	if len(conds) == 0 {
		return "", p
	}
	return " WHERE " + strings.Join(conds, " AND "), p
}

// rowsOf decodes ClickHouse's JSONEachRow output into dst rows.
func rowsOf[T any](b []byte) ([]T, error) {
	var out []T
	dec := json.NewDecoder(bytes.NewReader(b))
	for dec.More() {
		var r T
		if err := dec.Decode(&r); err != nil {
			return nil, fmt.Errorf("decode clickhouse row: %w", err)
		}
		out = append(out, r)
	}
	return out, nil
}

func (c *ClickHouse) Summary(ctx context.Context, q Query) ([]SummaryRow, error) {
	w, p := where(q, "window_start")
	sql := fmt.Sprintf(`SELECT kind, uniqExact(window_start) AS windows, sum(event_count) AS events,
		sum(sum_value) AS sum, min(min_value) AS min, max(max_value) AS max,
		if(events > 0, sum / events, 0) AS avg
		FROM %s.%s FINAL%s GROUP BY kind ORDER BY kind FORMAT JSONEachRow`, c.cfg.Database, c.cfg.Table, w)
	b, err := c.do(ctx, "", p, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	return rowsOf[SummaryRow](b)
}

func (c *ClickHouse) Series(ctx context.Context, q Query) ([]SeriesPoint, error) {
	w, p := where(q, "window_start")
	limit := q.Limit
	if limit <= 0 || limit > 10000 {
		limit = 1000
	}
	sql := fmt.Sprintf(`SELECT toString(window_start) AS ws, toString(window_end) AS we, kind, key,
		event_count AS count, if(event_count > 0, sum_value / event_count, 0) AS avg,
		min_value AS min, max_value AS max
		FROM %s.%s FINAL%s ORDER BY window_start, kind, key LIMIT %d FORMAT JSONEachRow`,
		c.cfg.Database, c.cfg.Table, w, limit) // limit is an int we clamped: safe to format
	b, err := c.do(ctx, "", p, strings.NewReader(sql))
	if err != nil {
		return nil, err
	}
	type raw struct {
		WS    string      `json:"ws"`
		WE    string      `json:"we"`
		Kind  events.Kind `json:"kind"`
		Key   string      `json:"key"`
		Count int64       `json:"count"`
		Avg   float64     `json:"avg"`
		Min   float64     `json:"min"`
		Max   float64     `json:"max"`
	}
	rs, err := rowsOf[raw](b)
	if err != nil {
		return nil, err
	}
	out := make([]SeriesPoint, 0, len(rs))
	for _, r := range rs {
		ws, _ := time.Parse(chTime, r.WS)
		we, _ := time.Parse(chTime, r.WE)
		out = append(out, SeriesPoint{WindowStart: ws.UTC(), WindowEnd: we.UTC(), Kind: r.Kind, Key: r.Key,
			Count: r.Count, Avg: r.Avg, Min: r.Min, Max: r.Max})
	}
	return out, nil
}

func (c *ClickHouse) LateSummary(ctx context.Context, q Query) (LateSummary, error) {
	q.Kind, q.Key = "", "" // late stats have no kind/key dimension
	w, p := where(q, "ts")
	sql := fmt.Sprintf(`SELECT sum(on_time) AS on_time, sum(late) AS late, sum(too_late) AS too_late,
		max(max_lateness_ms) AS max_lateness_ms FROM %s.%s%s FORMAT JSONEachRow`, c.cfg.Database, c.cfg.LateTbl, w)
	b, err := c.do(ctx, "", p, strings.NewReader(sql))
	if err != nil {
		return LateSummary{}, err
	}
	rs, err := rowsOf[LateSummary](b)
	if err != nil || len(rs) == 0 {
		return LateSummary{}, err
	}
	return rs[0], nil
}

var _ Store = (*ClickHouse)(nil)
