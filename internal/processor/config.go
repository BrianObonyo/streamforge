// Package processor is the stream-processing engine: it consumes a topic as part
// of a consumer group and turns raw records into windowed aggregates in a sink,
// while staying correct under overload, bad data, sink outages, rebalances and
// crashes.
//
// ARCHITECTURE (one Processor instance = one consumer-group member):
//
//	                     ┌─────────────── Processor.Run (poll goroutine) ──────────────┐
//	broker ──Poll──▶ dispatch ──▶ throttle (AIMD token bucket)                        │
//	                     │                                                            │
//	       per assigned partition:                                                    │
//	                     ▼                                                            │
//	            bounded Queue ──(high-water: PAUSE partition at broker)               │
//	                     │       (low-water:  RESUME)                                 │
//	                     ▼                                                            │
//	            partitionTask goroutine  (owns ALL state for that partition)          │
//	              decode → validate → dedup → Aggregator.Add                          │
//	              every tick: Collect → sink (retry+breaker) → Ack → publish safe offset
//	                                                                                  │
//	     commit loop ◀── safe offsets (min over unfinished windows) ──────────────────┘
//
// KEY DESIGN CHOICES
//
//  1. One goroutine per partition owns that partition's windows, watermark and
//     dedup cache outright. No locks on the hot path; ordering within a partition
//     is preserved; a slow partition cannot stall the others.
//  2. Offsets are committed only up to the "safe offset" (see aggregation.SafeOffset),
//     so a crash replays exactly the records needed to rebuild lost window state.
//  3. Sink writes are idempotent (version = event count; highest wins), so replay
//     and retry can never double-count — "effectively exactly-once" on top of
//     at-least-once delivery.
//  4. Pressure is propagated, never absorbed: bounded queues → broker pause → AIMD.
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package processor

import (
	"flag"
	"fmt"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/tracing"
)

// Config holds every tunable of a Processor.
type Config struct {
	Topic    string
	Group    string
	MemberID string // empty ⇒ broker-assigned

	PollBatch   int           // max records per Poll
	PollTimeout time.Duration // how long Poll may block before the loop does housekeeping

	QueueCapacity int // per-partition bounded queue size
	HighWater     int // pause partition at this depth
	LowWater      int // resume at this depth

	WindowSize      time.Duration
	WindowSlide     time.Duration // 0 or == size ⇒ tumbling
	MaxOutOfOrder   time.Duration // watermark slack
	AllowedLateness time.Duration // refinement grace period after a window fires
	IdleTimeout     time.Duration // processing-time watermark fallback; 0 disables
	Assigner        aggregation.Assigner

	TickInterval   time.Duration // how often a task collects & emits due windows
	CommitInterval time.Duration // how often safe offsets are committed

	DedupTTL time.Duration
	DedupMax int

	Retry            reliability.RetryPolicy // sink write retries
	BreakerThreshold int                     // consecutive sink failures before the breaker opens
	BreakerOpenFor   time.Duration

	// MaxPermanentSinkFailures is how many CONSECUTIVE permanent (non-retryable)
	// sink errors a partition tolerates before dead-lettering the stuck batch. Until
	// then the batch stays pending so an operator fix (e.g. restoring a table) loses
	// nothing; after it, the partition is unblocked and the rows are preserved in the DLQ.
	MaxPermanentSinkFailures int

	Tracer *tracing.Tracer // optional; nil disables tracing

	ThrottleCeiling float64 // max events/sec the AIMD controller may allow; 0 disables throttling
	DLQTooLate      bool    // also dead-letter events beyond allowed lateness

	Now func() time.Time // injectable clock
}

// ApplyDefaults fills zero values and derives the window assigner. It is called
// by New, and exported so CLIs can validate flags early.
func (c *Config) ApplyDefaults() error {
	if c.Topic == "" {
		return fmt.Errorf("processor: topic is required")
	}
	if c.Group == "" {
		c.Group = "streamforge-processors"
	}
	def := func(d *time.Duration, v time.Duration) {
		if *d == 0 {
			*d = v
		}
	}
	defInt := func(i *int, v int) {
		if *i == 0 {
			*i = v
		}
	}
	defInt(&c.PollBatch, 500)
	def(&c.PollTimeout, 100*time.Millisecond)
	defInt(&c.QueueCapacity, 4096)
	defInt(&c.HighWater, c.QueueCapacity*3/4)
	defInt(&c.LowWater, c.QueueCapacity/4)
	def(&c.WindowSize, 10*time.Second)
	def(&c.MaxOutOfOrder, 2*time.Second)
	def(&c.AllowedLateness, 5*time.Second)
	def(&c.TickInterval, 250*time.Millisecond)
	def(&c.CommitInterval, 500*time.Millisecond)
	def(&c.DedupTTL, 10*time.Minute)
	defInt(&c.DedupMax, 1_000_000)
	if c.Retry.MaxAttempts == 0 {
		c.Retry = reliability.DefaultRetry
	}
	defInt(&c.MaxPermanentSinkFailures, 5)
	defInt(&c.BreakerThreshold, 5)
	def(&c.BreakerOpenFor, 2*time.Second)
	if c.Now == nil {
		c.Now = time.Now
	}
	if c.HighWater > c.QueueCapacity || c.LowWater >= c.HighWater {
		return fmt.Errorf("processor: need low-water < high-water <= queue capacity (got %d/%d/%d)",
			c.LowWater, c.HighWater, c.QueueCapacity)
	}
	if c.Assigner == nil {
		a, err := aggregation.NewAssigner(c.WindowSize, c.WindowSlide)
		if err != nil {
			return err
		}
		c.Assigner = a
	}
	return nil
}

// RegisterFlags binds the commonly tuned fields to a FlagSet, so every binary
// exposes the same, consistently named options.
func RegisterFlags(fs *flag.FlagSet, c *Config) {
	fs.StringVar(&c.Topic, "topic", "events", "topic to consume")
	fs.StringVar(&c.Group, "group", "streamforge-processors", "consumer group id")
	fs.DurationVar(&c.WindowSize, "window", 10*time.Second, "event-time window size")
	fs.DurationVar(&c.WindowSlide, "slide", 0, "window slide (0 = tumbling)")
	fs.DurationVar(&c.MaxOutOfOrder, "max-ooo", 2*time.Second, "watermark slack for out-of-order events")
	fs.DurationVar(&c.AllowedLateness, "lateness", 5*time.Second, "how long fired windows still accept late events")
	fs.DurationVar(&c.IdleTimeout, "idle", 3*time.Second, "advance watermark by wall clock after this much idle time (0 = off)")
	fs.IntVar(&c.QueueCapacity, "queue", 4096, "per-partition queue capacity")
	fs.Float64Var(&c.ThrottleCeiling, "throttle", 0, "AIMD ceiling in events/sec (0 = off)")
	fs.DurationVar(&c.DedupTTL, "dedup-ttl", 10*time.Minute, "how long event IDs are remembered")
	fs.BoolVar(&c.DLQTooLate, "dlq-too-late", false, "dead-letter events beyond allowed lateness")
}
