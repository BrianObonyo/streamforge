package aggregation

import (
	"fmt"
	"time"
)

// Watermark tracks "how far has event time progressed?" for ONE partition.
//
// A watermark W is the system's promise: "I don't expect any more events with
// EventTime < W." A window [s, e) can therefore be finalised once W >= e. Real
// streams can't make that promise perfectly, so we use the standard heuristic
// of BOUNDED OUT-OF-ORDERNESS:
//
//	W = (max event time seen) − MaxOutOfOrder
//
// i.e. we tolerate events arriving up to MaxOutOfOrder behind the newest one.
// Raising it improves completeness but delays results; lowering it does the
// reverse — the central latency/correctness dial in streaming (Akidau, 2015).
//
// IDLE PARTITIONS. A watermark driven only by events never moves on a partition
// that stops receiving data, which would freeze its windows forever. If a
// partition is silent for IdleTimeout we fall back to processing time
// (W = now − MaxOutOfOrder). This trades a small correctness risk (a stalled
// producer could be mistaken for a quiet one) for liveness; set IdleTimeout=0
// to disable and stay purely event-driven.
//
// The watermark never moves backwards, and the type is NOT goroutine-safe: each
// partition task owns its own instance.
type Watermark struct {
	maxOOO   time.Duration
	idle     time.Duration
	maxEvent time.Time // newest EventTime observed
	lastSeen time.Time // wall-clock time of the last observation
	current  time.Time // monotone watermark value
	seen     bool
}

// WatermarkSnapshot is the durable state needed to resume watermark progress.
type WatermarkSnapshot struct {
	MaxEvent time.Time
	LastSeen time.Time
	Current  time.Time
	Seen     bool
}

// NewWatermark creates a tracker. idle == 0 disables the processing-time fallback.
func NewWatermark(maxOutOfOrder, idle time.Duration) *Watermark {
	return &Watermark{maxOOO: maxOutOfOrder, idle: idle}
}

// Observe records an event time. now is the wall clock, used only for idleness.
func (w *Watermark) Observe(eventTime, now time.Time) {
	if !w.seen || eventTime.After(w.maxEvent) {
		w.maxEvent = eventTime
	}
	w.lastSeen = now
	w.seen = true
}

// Current returns the watermark. Before any event arrives it returns the zero
// time, which compares as "before everything" so nothing fires prematurely.
func (w *Watermark) Current(now time.Time) time.Time {
	if !w.seen {
		return time.Time{}
	}
	c := w.maxEvent.Add(-w.maxOOO)
	if w.idle > 0 && now.Sub(w.lastSeen) >= w.idle {
		if p := now.Add(-w.maxOOO); p.After(c) {
			c = p
		}
	}
	if c.After(w.current) {
		w.current = c // monotone: never regress
	}
	return w.current
}

// Snapshot returns the event-time progress without exposing configuration.
func (w *Watermark) Snapshot() WatermarkSnapshot {
	return WatermarkSnapshot{
		MaxEvent: w.maxEvent, LastSeen: w.lastSeen, Current: w.current, Seen: w.seen,
	}
}

// Restore replaces the tracked event-time progress.
func (w *Watermark) Restore(s WatermarkSnapshot) error {
	if !s.Seen && (!s.MaxEvent.IsZero() || !s.LastSeen.IsZero() || !s.Current.IsZero()) {
		return fmt.Errorf("watermark: unseen snapshot contains timestamps")
	}
	if s.Seen && (s.MaxEvent.IsZero() || s.LastSeen.IsZero()) {
		return fmt.Errorf("watermark: seen snapshot is missing timestamps")
	}
	w.maxEvent, w.lastSeen, w.current, w.seen = s.MaxEvent, s.LastSeen, s.Current, s.Seen
	return nil
}
