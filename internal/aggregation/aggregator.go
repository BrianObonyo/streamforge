package aggregation

import (
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/BrianObonyo/streamforge/internal/events"
)

// Stats is the accumulator kept per (window, kind, key).
//
// It is a commutative, associative "monoid": the result is identical regardless
// of the order events are added or how partial Stats are Merged. That property
// is what lets us tolerate out-of-order data and re-process after a crash.
type Stats struct {
	Count int64
	Sum   float64
	Min   float64
	Max   float64
}

// Add folds one observation in.
func (s *Stats) Add(v float64) {
	if s.Count == 0 {
		s.Min, s.Max = v, v
	} else {
		if v < s.Min {
			s.Min = v
		}
		if v > s.Max {
			s.Max = v
		}
	}
	s.Count++
	s.Sum += v
}

// Avg is the arithmetic mean (0 for an empty accumulator).
func (s Stats) Avg() float64 {
	if s.Count == 0 {
		return 0
	}
	return s.Sum / float64(s.Count)
}

// GroupKey is the per-window grouping dimension.
type GroupKey struct {
	Kind events.Kind
	Key  string
}

// Result is one emitted aggregate row.
//
// Version is the idempotency device. It equals Stats.Count: the number of events
// folded in. Window contents only ever GROW, so a fuller aggregate always has a
// strictly higher version, and sinks keep the row with the highest version.
// This makes emission safe under every replay scenario:
//
//   - re-emitting after a sink failure        → same version, same row, harmless
//   - late event refines a fired window       → higher version wins
//   - crash + replay rebuilds a PARTIAL window → lower version loses, so a
//     complete earlier result is never overwritten by an incomplete one
//
// (This is the "grow-only register" trick from CRDT literature, applied to
// stream output.) It deliberately avoids wall-clock versions, which would let a
// stale partial replay clobber a good result simply by being newer.
type Result struct {
	Window    Window
	Kind      events.Kind
	Key       string
	Stats     Stats
	Version   uint64
	Update    bool // true when this refines a window that was already emitted
	Partition int
}

// Outcome classifies what happened to an event on arrival.
type Outcome int

const (
	OnTime  Outcome = iota // window still open
	Late                   // window already fired, but within AllowedLateness → accepted, will refine
	TooLate                // beyond AllowedLateness → dropped from windows (side output)
)

func (o Outcome) String() string {
	return [...]string{"on_time", "late", "too_late"}[o]
}

// LateStats are cumulative counters for the late-arrival analytics.
type LateStats struct {
	OnTime, Late, TooLate int64
	MaxLateness           time.Duration // largest (watermark − eventTime) seen
}

// Config tunes an Aggregator.
type Config struct {
	Assigner        Assigner
	MaxOutOfOrder   time.Duration // watermark slack
	AllowedLateness time.Duration // how long a fired window keeps accepting refinements
	IdleTimeout     time.Duration // 0 disables processing-time watermark advance
}

type windowState struct {
	w         Window
	groups    map[GroupKey]*Stats
	minOffset int64 // lowest broker offset that contributed (drives safe commit)
	fired     bool  // has been emitted at least once (and acked)
	dirty     bool  // fired window changed since last emission
}

// Aggregator performs event-time windowed aggregation for ONE partition.
// It is NOT goroutine-safe; the owning partition task is its sole user, which is
// what lets the hot path run with zero locking.
type Aggregator struct {
	cfg     Config
	wm      *Watermark
	windows map[int64]*windowState // keyed by window start (unix ns)
	late    LateStats
}

// WindowSnapshot is a serializable copy of one retained event-time window.
type WindowSnapshot struct {
	Window    Window
	Groups    []GroupSnapshot
	MinOffset int64
	Fired     bool
	Dirty     bool
}

// GroupSnapshot stores one aggregate dimension in a window.
type GroupSnapshot struct {
	Kind  events.Kind
	Key   string
	Stats Stats
}

// Snapshot is the complete durable state of one partition aggregator.
type Snapshot struct {
	Version   int
	Watermark WatermarkSnapshot
	Windows   []WindowSnapshot
	Late      LateStats
}

// New creates an aggregator.
func New(cfg Config) *Aggregator {
	return &Aggregator{
		cfg:     cfg,
		wm:      NewWatermark(cfg.MaxOutOfOrder, cfg.IdleTimeout),
		windows: map[int64]*windowState{},
	}
}

// Snapshot returns a deterministic, serializable copy of the aggregator state.
func (a *Aggregator) Snapshot() Snapshot {
	s := Snapshot{Version: 1, Watermark: a.wm.Snapshot(), Late: a.late}
	for _, ws := range a.windows {
		w := WindowSnapshot{Window: ws.w, MinOffset: ws.minOffset, Fired: ws.fired, Dirty: ws.dirty}
		for key, stats := range ws.groups {
			w.Groups = append(w.Groups, GroupSnapshot{Kind: key.Kind, Key: key.Key, Stats: *stats})
		}
		sort.Slice(w.Groups, func(i, j int) bool {
			if w.Groups[i].Kind != w.Groups[j].Kind {
				return w.Groups[i].Kind < w.Groups[j].Kind
			}
			return w.Groups[i].Key < w.Groups[j].Key
		})
		s.Windows = append(s.Windows, w)
	}
	sort.Slice(s.Windows, func(i, j int) bool {
		return s.Windows[i].Window.Start.Before(s.Windows[j].Window.Start)
	})
	return s
}

// Restore replaces the aggregator state from a compatible snapshot.
func (a *Aggregator) Restore(s Snapshot) error {
	if s.Version != 1 {
		return fmt.Errorf("aggregation: unsupported snapshot version %d", s.Version)
	}
	if s.Late.OnTime < 0 || s.Late.Late < 0 || s.Late.TooLate < 0 || s.Late.MaxLateness < 0 {
		return fmt.Errorf("aggregation: invalid late statistics in snapshot")
	}
	wm := NewWatermark(a.cfg.MaxOutOfOrder, a.cfg.IdleTimeout)
	if err := wm.Restore(s.Watermark); err != nil {
		return err
	}
	windows := make(map[int64]*windowState, len(s.Windows))
	for _, saved := range s.Windows {
		if saved.Window.Start.IsZero() || !saved.Window.End.After(saved.Window.Start) {
			return fmt.Errorf("aggregation: invalid window in snapshot")
		}
		start := saved.Window.Start.UnixNano()
		if _, exists := windows[start]; exists {
			return fmt.Errorf("aggregation: duplicate window in snapshot")
		}
		if len(saved.Groups) == 0 {
			return fmt.Errorf("aggregation: empty window in snapshot")
		}
		ws := &windowState{
			w: saved.Window, groups: make(map[GroupKey]*Stats, len(saved.Groups)),
			minOffset: saved.MinOffset, fired: saved.Fired, dirty: saved.Dirty,
		}
		for _, group := range saved.Groups {
			if !group.Kind.Valid() || group.Key == "" || group.Stats.Count <= 0 ||
				math.IsNaN(group.Stats.Sum) || math.IsInf(group.Stats.Sum, 0) ||
				math.IsNaN(group.Stats.Min) || math.IsInf(group.Stats.Min, 0) ||
				math.IsNaN(group.Stats.Max) || math.IsInf(group.Stats.Max, 0) ||
				group.Stats.Min > group.Stats.Max {
				return fmt.Errorf("aggregation: invalid group in snapshot")
			}
			key := GroupKey{Kind: group.Kind, Key: group.Key}
			if _, exists := ws.groups[key]; exists {
				return fmt.Errorf("aggregation: duplicate group in snapshot")
			}
			stats := group.Stats
			ws.groups[key] = &stats
		}
		windows[start] = ws
	}
	a.wm, a.windows, a.late = wm, windows, s.Late
	return nil
}

// Add folds an event (read from broker `offset`) into its window(s).
//
// Classification per window, with wm = current watermark:
//
//	wm >= end + AllowedLateness → TooLate: window already garbage-collected → drop
//	wm >= end                   → Late:    window fired, accept and mark dirty
//	otherwise                   → OnTime
//
// An event in several sliding windows reports the "worst" outcome among the
// windows that accepted it, or TooLate only if every window rejected it.
func (a *Aggregator) Add(ev events.Event, offset int64, now time.Time) Outcome {
	a.wm.Observe(ev.EventTime, now)
	wm := a.wm.Current(now)

	if lag := wm.Sub(ev.EventTime); lag > a.late.MaxLateness && !wm.IsZero() {
		a.late.MaxLateness = lag
	}

	accepted, lateHit := 0, false
	for _, w := range a.cfg.Assigner.Assign(ev.EventTime) {
		if !wm.IsZero() && !wm.Before(w.End.Add(a.cfg.AllowedLateness)) {
			continue // too late for this window
		}
		k := w.Start.UnixNano()
		ws := a.windows[k]
		if ws == nil {
			ws = &windowState{w: w, groups: map[GroupKey]*Stats{}, minOffset: offset}
			a.windows[k] = ws
		}
		if offset < ws.minOffset {
			ws.minOffset = offset
		}
		gk := GroupKey{ev.Kind, ev.Key}
		st := ws.groups[gk]
		if st == nil {
			st = &Stats{}
			ws.groups[gk] = st
		}
		st.Add(ev.Value)
		if !wm.IsZero() && !wm.Before(w.End) {
			lateHit = true // landed in a window that has already closed in event time
			ws.dirty = true
		} else if ws.fired {
			ws.dirty = true
		}
		accepted++
	}

	switch {
	case accepted == 0:
		a.late.TooLate++
		return TooLate
	case lateHit:
		a.late.Late++
		return Late
	}
	a.late.OnTime++
	return OnTime
}

// Batch is the set of results ready to emit, plus the bookkeeping needed to
// acknowledge them.
type Batch struct {
	Results []Result
	wm      time.Time
	keys    []int64
}

// Collect gathers every result that is due, WITHOUT changing any state.
//
// Together with Ack this is a two-phase emit. If writing to the sink fails the
// caller simply skips Ack; the same results are due again at the next Collect.
// A one-phase "emit and forget" design would lose data on every sink hiccup.
//
// A window is due when (a) the watermark has passed its end and it has never
// been emitted, or (b) it was emitted but has since been refined by late data.
func (a *Aggregator) Collect(now time.Time, partition int) Batch {
	wm := a.wm.Current(now)
	b := Batch{wm: wm}
	if wm.IsZero() {
		return b
	}
	for k, ws := range a.windows {
		due := (!ws.fired && !wm.Before(ws.w.End)) || (ws.fired && ws.dirty)
		if !due {
			continue
		}
		b.keys = append(b.keys, k)
		for gk, st := range ws.groups {
			b.Results = append(b.Results, Result{
				Window: ws.w, Kind: gk.Kind, Key: gk.Key, Stats: *st,
				Version: uint64(st.Count), Update: ws.fired, Partition: partition,
			})
		}
	}
	// Deterministic order: stable tests, stable sink inserts.
	sort.Slice(b.Results, func(i, j int) bool {
		x, y := b.Results[i], b.Results[j]
		if !x.Window.Start.Equal(y.Window.Start) {
			return x.Window.Start.Before(y.Window.Start)
		}
		if x.Kind != y.Kind {
			return x.Kind < y.Kind
		}
		return x.Key < y.Key
	})
	return b
}

// Ack confirms that a Batch reached the sink: the windows are marked clean, and
// any window whose lateness grace period has also expired is garbage-collected.
func (a *Aggregator) Ack(b Batch) {
	for _, k := range b.keys {
		if ws := a.windows[k]; ws != nil {
			ws.fired, ws.dirty = true, false
		}
	}
	if b.wm.IsZero() {
		return
	}
	for k, ws := range a.windows {
		if ws.fired && !ws.dirty && !b.wm.Before(ws.w.End.Add(a.cfg.AllowedLateness)) {
			delete(a.windows, k)
		}
	}
}

// SafeOffset returns the highest broker offset the consumer may COMMIT without
// risking data loss on restart, given `next` (one past the last record handled).
//
// A record can only be forgotten once every window it contributed to has been
// emitted AND garbage-collected. Until then a crash would lose that window's
// in-memory state, so recovery must be able to replay the records that built
// it. Hence: commit min(next, lowest offset held by any retained window).
//
// This is the key correctness link between windowing and offset management —
// naïve "commit after processing" would silently drop in-flight windows.
func (a *Aggregator) SafeOffset(next int64) int64 {
	safe := next
	for _, ws := range a.windows {
		if ws.minOffset < safe {
			safe = ws.minOffset
		}
	}
	return safe
}

// OpenWindows returns how many windows are currently retained in memory.
func (a *Aggregator) OpenWindows() int { return len(a.windows) }

// LateStats returns a copy of the cumulative counters.
func (a *Aggregator) LateStats() LateStats { return a.late }

// Watermark returns the current watermark (zero before any event).
func (a *Aggregator) Watermark(now time.Time) time.Time { return a.wm.Current(now) }
