// Package storage persists aggregated results and serves analytical queries.
//
// Two implementations share one interface:
//
//	MemStore    – in-process, for tests, the demo, and running without ClickHouse.
//	ClickHouse  – the production store, spoken to over ClickHouse's HTTP interface
//	              (no driver dependency; see clickhouse.go).
//
// Both implement the SAME idempotency rule, which is what makes the pipeline
// safe to replay: when several rows exist for one (window, kind, key), the one
// with the highest Version (= event count) wins. See aggregation.Result.
package storage

import (
	"context"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
)

// LateStatRow is a periodic delta of late-arrival counters from one partition.
type LateStatRow struct {
	TS            time.Time
	Partition     int
	OnTime        int64
	Late          int64
	TooLate       int64
	MaxLatenessMs int64
}

// Sink is the write side, used by stream processors.
type Sink interface {
	WriteResults(ctx context.Context, rs []aggregation.Result) error
	WriteLateStats(ctx context.Context, rows []LateStatRow) error
}

// Query bounds an analytics request. Zero values mean "unbounded"/"all".
type Query struct {
	Kind  events.Kind // optional filter
	Key   string      // optional filter
	From  time.Time   // window_start >= From
	To    time.Time   // window_start <  To
	Limit int         // max rows for Series (default 1000)
}

// SummaryRow aggregates all windows of one kind (event distribution by category).
type SummaryRow struct {
	Kind    events.Kind `json:"kind"`
	Windows int64       `json:"windows"`
	Events  int64       `json:"events"`
	Sum     float64     `json:"sum"`
	Min     float64     `json:"min"`
	Max     float64     `json:"max"`
	Avg     float64     `json:"avg"`
}

// SeriesPoint is one window's aggregate for time-series charts.
type SeriesPoint struct {
	WindowStart time.Time   `json:"window_start"`
	WindowEnd   time.Time   `json:"window_end"`
	Kind        events.Kind `json:"kind"`
	Key         string      `json:"key"`
	Count       int64       `json:"count"`
	Avg         float64     `json:"avg"`
	Min         float64     `json:"min"`
	Max         float64     `json:"max"`
}

// LateSummary totals the late-arrival counters over a time range.
type LateSummary struct {
	OnTime        int64 `json:"on_time"`
	Late          int64 `json:"late"`
	TooLate       int64 `json:"too_late"`
	MaxLatenessMs int64 `json:"max_lateness_ms"`
}

// Store is the read+write interface.
//
// Caveat on Summary: event totals are exact for TUMBLING windows. With SLIDING
// windows each event is deliberately counted once per overlapping window, so
// totals are inflated by Size/Slide — use Series for sliding-window analytics.
type Store interface {
	Sink
	Summary(ctx context.Context, q Query) ([]SummaryRow, error)
	Series(ctx context.Context, q Query) ([]SeriesPoint, error)
	LateSummary(ctx context.Context, q Query) (LateSummary, error)
}
