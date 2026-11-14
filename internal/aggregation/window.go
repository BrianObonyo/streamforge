// Package aggregation is StreamForge's event-time processing engine:
// windows, watermarks, allowed lateness, and the accumulators that sit inside
// them.
//
// The model follows Akidau et al., "The Dataflow Model: A Practical Approach to
// Balancing Correctness, Latency, and Cost in Massive-Scale, Unbounded,
// Out-of-Order Data Processing" (VLDB 2015), which answers four questions:
//
//	WHAT is computed?    → Stats (count/sum/min/max) per (kind, key)
//	WHERE in event time? → Window assigners (tumbling, sliding)
//	WHEN is it emitted?  → when the WATERMARK passes the window end
//	HOW do refinements relate? → late data re-emits the window; the sink keeps
//	                             the most complete version (see aggregator.go)
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package aggregation

import (
	"fmt"
	"time"
)

// Window is the half-open event-time interval [Start, End).
type Window struct {
	Start, End time.Time
}

// Contains reports whether t falls inside the window.
func (w Window) Contains(t time.Time) bool { return !t.Before(w.Start) && t.Before(w.End) }

func (w Window) String() string {
	return fmt.Sprintf("[%s, %s)", w.Start.UTC().Format("15:04:05.000"), w.End.UTC().Format("15:04:05.000"))
}

// Assigner decides which window(s) an event time belongs to.
type Assigner interface {
	Assign(t time.Time) []Window
	Name() string
}

// floorTime rounds t DOWN to a multiple of d measured from the Unix epoch.
// The explicit negative-remainder fix matters: Go's % keeps the dividend's sign,
// so pre-1970 times would otherwise round toward zero (up) instead of down.
func floorTime(t time.Time, d time.Duration) time.Time {
	ns := t.UnixNano()
	r := ns % int64(d)
	if r < 0 {
		r += int64(d)
	}
	return time.Unix(0, ns-r).UTC()
}

// Tumbling windows are fixed-size, aligned to the epoch, and never overlap:
// every event belongs to exactly one window.
type Tumbling struct{ Size time.Duration }

func (a Tumbling) Name() string { return fmt.Sprintf("tumbling(%s)", a.Size) }

func (a Tumbling) Assign(t time.Time) []Window {
	s := floorTime(t, a.Size)
	return []Window{{Start: s, End: s.Add(a.Size)}}
}

// Sliding windows have a Size and advance every Slide (Slide < Size ⇒ overlap),
// so each event lands in ceil(Size/Slide) windows — e.g. a "last 1 minute,
// updated every 10 seconds" moving average.
type Sliding struct{ Size, Slide time.Duration }

func (a Sliding) Name() string { return fmt.Sprintf("sliding(%s/%s)", a.Size, a.Slide) }

func (a Sliding) Assign(t time.Time) []Window {
	// Start from the latest window that begins at or before t, then walk back
	// while that window still covers t (start > t − Size).
	last := floorTime(t, a.Slide)
	out := make([]Window, 0, int(a.Size/a.Slide)+1)
	for s := last; s.After(t.Add(-a.Size)); s = s.Add(-a.Slide) {
		out = append(out, Window{Start: s, End: s.Add(a.Size)})
	}
	return out
}

// NewAssigner builds an assigner from CLI-style parameters: slide == 0 or
// slide == size gives tumbling windows.
func NewAssigner(size, slide time.Duration) (Assigner, error) {
	switch {
	case size <= 0:
		return nil, fmt.Errorf("window size must be > 0")
	case slide < 0 || slide > size:
		return nil, fmt.Errorf("window slide must be in [0, size]")
	case slide == 0 || slide == size:
		return Tumbling{Size: size}, nil
	}
	return Sliding{Size: size, Slide: slide}, nil
}
