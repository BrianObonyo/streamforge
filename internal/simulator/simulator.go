package simulator

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"sort"
	"strings"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
)

// Config controls a deterministic replay. Input records are newline-delimited
// events supplied by the caller; the simulator does not embed a sample dataset.
type Config struct {
	Partitions      int
	WindowSize      time.Duration
	WindowSlide     time.Duration
	MaxOutOfOrder   time.Duration
	AllowedLateness time.Duration
	DedupTTL        time.Duration
	DedupMax        int
	ClockStart      time.Time
	ArrivalStep     time.Duration
	SinkFailures    int
	SinkRetries     int
}

// Report contains stable counters and sorted aggregates from one replay.
type Report struct {
	Records      int64                `json:"records"`
	Valid        int64                `json:"valid"`
	Duplicates   int64                `json:"duplicates"`
	Invalid      int64                `json:"invalid"`
	Poison       int64                `json:"poison"`
	OnTime       int64                `json:"on_time"`
	Late         int64                `json:"late"`
	TooLate      int64                `json:"too_late"`
	SinkAttempts int64                `json:"sink_attempts"`
	SinkFailures int64                `json:"sink_failures"`
	Summaries    []storage.SummaryRow `json:"summaries"`
	Rows         []aggregation.Result `json:"rows"`
}

type partitionState struct {
	aggregator *aggregation.Aggregator
	deduper    *reliability.Deduper
	offset     int64
}

// Run replays JSONL events through partition assignment, validation,
// deduplication, event-time aggregation, and the in-memory sink. Time is virtual:
// ClockStart and ArrivalStep determine it, or input timestamps do when ClockStart
// is unset. Sink failures are injected deterministically and retried without
// sleeping.
func Run(ctx context.Context, input io.Reader, cfg Config) (Report, error) {
	if ctx == nil {
		return Report{}, fmt.Errorf("simulator: context is required")
	}
	if input == nil {
		return Report{}, fmt.Errorf("simulator: input is required")
	}
	if err := cfg.validate(); err != nil {
		return Report{}, err
	}
	assigner, err := aggregation.NewAssigner(cfg.WindowSize, cfg.WindowSlide)
	if err != nil {
		return Report{}, err
	}
	partitions := make([]partitionState, cfg.Partitions)
	for i := range partitions {
		partitions[i] = partitionState{
			aggregator: aggregation.New(aggregation.Config{
				Assigner: assigner, MaxOutOfOrder: cfg.MaxOutOfOrder,
				AllowedLateness: cfg.AllowedLateness, IdleTimeout: time.Nanosecond,
			}),
			deduper: reliability.NewDeduper(cfg.DedupTTL, cfg.DedupMax),
		}
	}

	var report Report
	scanner := bufio.NewScanner(input)
	scanner.Buffer(make([]byte, 64*1024), 16*1024*1024)
	lastNow := time.Time{}
	var maxEventTime time.Time
	for scanner.Scan() {
		if err := ctx.Err(); err != nil {
			return report, err
		}
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		report.Records++
		ev, decodeErr := events.Decode([]byte(line))
		now := virtualNow(cfg, report.Records, lastNow, ev)
		lastNow = now

		if decodeErr != nil {
			report.Poison++
			partitions[0].offset++
			continue
		}

		partition := partitioning.Hash{}.Partition([]byte(ev.Key), cfg.Partitions)
		state := &partitions[partition]
		offset := state.offset
		state.offset++

		if err := ev.Validate(now); err != nil {
			report.Invalid++
			continue
		}
		report.Valid++
		if state.deduper.Seen(ev.ID, now) {
			report.Duplicates++
			continue
		}

		if ev.EventTime.After(maxEventTime) {
			maxEventTime = ev.EventTime
		}
		switch state.aggregator.Add(ev, offset, now) {
		case aggregation.OnTime:
			report.OnTime++
		case aggregation.Late:
			report.Late++
		case aggregation.TooLate:
			report.TooLate++
		}
	}
	if err := scanner.Err(); err != nil {
		return report, fmt.Errorf("simulator: read input: %w", err)
	}
	if err := ctx.Err(); err != nil {
		return report, err
	}

	store := storage.NewMemStore()
	store.FailNext(cfg.SinkFailures)
	finalNow := lastNow
	if maxEventTime.After(finalNow) {
		finalNow = maxEventTime
	}
	finalNow = finalNow.Add(cfg.MaxOutOfOrder).Add(cfg.WindowSize).
		Add(cfg.AllowedLateness).Add(cfg.ArrivalStep)
	for i := range partitions {
		batch := partitions[i].aggregator.Collect(finalNow, i)
		if len(batch.Results) == 0 {
			partitions[i].aggregator.Ack(batch)
			continue
		}
		for attempt := 0; ; attempt++ {
			report.SinkAttempts++
			err := store.WriteResults(ctx, batch.Results)
			if err == nil {
				partitions[i].aggregator.Ack(batch)
				break
			}
			report.SinkFailures++
			if attempt >= cfg.SinkRetries {
				return report, fmt.Errorf("simulator: sink write failed after %d attempt(s): %w", attempt+1, err)
			}
			if err := ctx.Err(); err != nil {
				return report, err
			}
		}
	}
	report.Rows = store.Rows()
	sort.Slice(report.Rows, func(i, j int) bool {
		a, b := report.Rows[i], report.Rows[j]
		if !a.Window.Start.Equal(b.Window.Start) {
			return a.Window.Start.Before(b.Window.Start)
		}
		if a.Kind != b.Kind {
			return a.Kind < b.Kind
		}
		if a.Key != b.Key {
			return a.Key < b.Key
		}
		return a.Partition < b.Partition
	})
	report.Summaries, err = store.Summary(ctx, storage.Query{})
	if err != nil {
		return report, fmt.Errorf("simulator: summarize results: %w", err)
	}
	if report.Summaries == nil {
		report.Summaries = []storage.SummaryRow{}
	}
	if report.Rows == nil {
		report.Rows = []aggregation.Result{}
	}
	return report, nil
}

func (c Config) validate() error {
	switch {
	case c.Partitions <= 0:
		return fmt.Errorf("simulator: partitions must be > 0")
	case c.WindowSize <= 0:
		return fmt.Errorf("simulator: window size must be > 0")
	case c.WindowSlide < 0 || c.WindowSlide > c.WindowSize:
		return fmt.Errorf("simulator: window slide must be in [0, window size]")
	case c.MaxOutOfOrder < 0:
		return fmt.Errorf("simulator: max out-of-order duration must be >= 0")
	case c.AllowedLateness < 0:
		return fmt.Errorf("simulator: allowed lateness must be >= 0")
	case c.DedupTTL < 0 || c.DedupMax < 0:
		return fmt.Errorf("simulator: dedup limits must be >= 0")
	case c.ArrivalStep <= 0:
		return fmt.Errorf("simulator: arrival step must be > 0")
	case c.SinkFailures < 0 || c.SinkRetries < 0:
		return fmt.Errorf("simulator: sink failures and retries must be >= 0")
	}
	return nil
}

func virtualNow(cfg Config, recordNumber int64, last time.Time, ev events.Event) time.Time {
	if !cfg.ClockStart.IsZero() {
		if recordNumber == 1 || last.IsZero() {
			return cfg.ClockStart
		}
		return last.Add(cfg.ArrivalStep)
	}
	candidate := ev.IngestTime
	if candidate.IsZero() {
		candidate = ev.EventTime
	}
	if candidate.IsZero() {
		if last.IsZero() {
			return time.Unix(0, 0).UTC()
		}
		return last.Add(cfg.ArrivalStep)
	}
	if !last.IsZero() && !candidate.After(last) {
		return last.Add(cfg.ArrivalStep)
	}
	return candidate
}
