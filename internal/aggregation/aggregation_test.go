package aggregation

import (
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/events"
)

var t0 = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func ev(id string, at time.Duration, v float64) events.Event {
	return events.Event{ID: id, Kind: events.KindIoT, Key: "s1", Value: v, EventTime: t0.Add(at)}
}

func TestTumblingAssign(t *testing.T) {
	a := Tumbling{Size: 10 * time.Second}
	w := a.Assign(t0.Add(23 * time.Second))
	if len(w) != 1 || !w[0].Start.Equal(t0.Add(20*time.Second)) || !w[0].End.Equal(t0.Add(30*time.Second)) {
		t.Fatalf("got %v", w)
	}
	// Boundary: exactly on a window start belongs to THAT window (half-open).
	if w := a.Assign(t0.Add(30 * time.Second)); !w[0].Start.Equal(t0.Add(30 * time.Second)) {
		t.Fatalf("boundary: %v", w)
	}
}

func TestFloorTimeBeforeEpoch(t *testing.T) {
	got := floorTime(time.Unix(-1, 0), 10*time.Second)
	if got.Unix() != -10 {
		t.Fatalf("pre-epoch floor = %d, want -10", got.Unix())
	}
}

func TestSlidingAssign(t *testing.T) {
	a := Sliding{Size: 30 * time.Second, Slide: 10 * time.Second}
	ws := a.Assign(t0.Add(25 * time.Second))
	if len(ws) != 3 {
		t.Fatalf("want 3 overlapping windows, got %d: %v", len(ws), ws)
	}
	for _, w := range ws {
		if !w.Contains(t0.Add(25 * time.Second)) {
			t.Errorf("window %v does not contain the event", w)
		}
	}
}

func newAgg(ooo, late time.Duration) *Aggregator {
	return New(Config{Assigner: Tumbling{Size: 10 * time.Second}, MaxOutOfOrder: ooo, AllowedLateness: late})
}

func TestWindowFiresOnlyWhenWatermarkPasses(t *testing.T) {
	a := newAgg(2*time.Second, 0)
	now := t0
	a.Add(ev("a", 1*time.Second, 10), 0, now)
	a.Add(ev("b", 9*time.Second, 20), 1, now)
	// max event 9s, wm = 7s < window end 10s → nothing due.
	if b := a.Collect(now, 0); len(b.Results) != 0 {
		t.Fatalf("fired early: %v", b.Results)
	}
	a.Add(ev("c", 12*time.Second, 5), 2, now) // wm = 10s → first window due
	b := a.Collect(now, 0)
	if len(b.Results) != 1 {
		t.Fatalf("want 1 result, got %d", len(b.Results))
	}
	r := b.Results[0]
	if r.Stats.Count != 2 || r.Stats.Sum != 30 || r.Stats.Min != 10 || r.Stats.Max != 20 || r.Stats.Avg() != 15 {
		t.Fatalf("bad stats: %+v", r.Stats)
	}
}

func TestOutOfOrderWithinSlackIsOnTime(t *testing.T) {
	a := newAgg(5*time.Second, 0)
	a.Add(ev("a", 8*time.Second, 1), 0, t0)
	// 3s is behind max (8s) but inside the 5s slack → window [0,10) still open.
	if o := a.Add(ev("b", 3*time.Second, 1), 1, t0); o != OnTime {
		t.Fatalf("got %v, want on_time", o)
	}
}

func TestLateThenTooLate(t *testing.T) {
	a := newAgg(0, 5*time.Second)
	a.Add(ev("a", 1*time.Second, 1), 0, t0)
	a.Add(ev("b", 11*time.Second, 1), 1, t0) // wm=11 → window [0,10) due
	b := a.Collect(t0, 0)
	a.Ack(b)

	// Late but within the 5s grace period (wm 11 < 10+5) → accepted & refines.
	if o := a.Add(ev("late", 2*time.Second, 1), 2, t0); o != Late {
		t.Fatalf("got %v, want late", o)
	}
	b = a.Collect(t0, 0)
	if len(b.Results) != 1 || !b.Results[0].Update || b.Results[0].Stats.Count != 2 || b.Results[0].Version != 2 {
		t.Fatalf("refinement wrong: %+v", b.Results)
	}
	a.Ack(b)

	// Push watermark past end+lateness (10+5=15) → window GC'd; next one is TooLate.
	a.Add(ev("c", 20*time.Second, 1), 3, t0)
	a.Ack(a.Collect(t0, 0))
	if o := a.Add(ev("vlate", 2*time.Second, 1), 4, t0); o != TooLate {
		t.Fatalf("got %v, want too_late", o)
	}
	ls := a.LateStats()
	if ls.Late != 1 || ls.TooLate != 1 || ls.MaxLateness < 15*time.Second {
		t.Fatalf("late stats: %+v", ls)
	}
}

// The property that protects us from sink outages: not acking means the same
// results are due again, and nothing is lost.
func TestCollectWithoutAckIsRepeatable(t *testing.T) {
	a := newAgg(0, 0)
	a.Add(ev("a", 1*time.Second, 1), 0, t0)
	a.Add(ev("b", 11*time.Second, 1), 1, t0)
	b1 := a.Collect(t0, 0)
	b2 := a.Collect(t0, 0) // "sink failed, we retry"
	if len(b1.Results) != 1 || len(b2.Results) != 1 {
		t.Fatalf("results vanished: %d then %d", len(b1.Results), len(b2.Results))
	}
	a.Ack(b2)
	if b3 := a.Collect(t0, 0); len(b3.Results) != 0 {
		t.Fatalf("acked results re-emitted: %v", b3.Results)
	}
}

// SafeOffset must hold the commit back while a window that depends on earlier
// records is still retained — otherwise a crash loses that window.
func TestSafeOffsetHeldByOpenWindows(t *testing.T) {
	a := newAgg(0, 0)
	a.Add(ev("a", 1*time.Second, 1), 100, t0)  // window [0,10)  minOffset 100
	a.Add(ev("b", 11*time.Second, 1), 101, t0) // window [10,20) minOffset 101
	if got := a.SafeOffset(102); got != 100 {
		t.Fatalf("safe=%d, want 100 (window [0,10) still open)", got)
	}
	a.Ack(a.Collect(t0, 0)) // emits + GCs [0,10) (no lateness)
	if got := a.SafeOffset(102); got != 101 {
		t.Fatalf("safe=%d, want 101", got)
	}
}

func TestIdleWatermarkAdvancesByProcessingTime(t *testing.T) {
	a := New(Config{Assigner: Tumbling{Size: 10 * time.Second}, MaxOutOfOrder: time.Second, IdleTimeout: 3 * time.Second})
	a.Add(ev("a", 1*time.Second, 1), 0, t0)
	if b := a.Collect(t0.Add(time.Second), 0); len(b.Results) != 0 {
		t.Fatal("fired before idle timeout")
	}
	// Partition goes silent; wall clock moves to t0+30s → watermark follows.
	if b := a.Collect(t0.Add(30*time.Second), 0); len(b.Results) != 1 {
		t.Fatalf("idle partition never fired: %d", len(b.Results))
	}
}

func TestWatermarkNeverRegresses(t *testing.T) {
	w := NewWatermark(time.Second, 0)
	w.Observe(t0.Add(10*time.Second), t0)
	hi := w.Current(t0)
	w.Observe(t0.Add(2*time.Second), t0) // older event must not pull it back
	if got := w.Current(t0); got.Before(hi) {
		t.Fatalf("regressed %v -> %v", hi, got)
	}
}

func TestStatsMonoidOrderIndependent(t *testing.T) {
	vals := []float64{5, -3, 9, 0, 2}
	var a, b Stats
	for _, v := range vals {
		a.Add(v)
	}
	for i := len(vals) - 1; i >= 0; i-- {
		b.Add(vals[i])
	}
	if a != b {
		t.Fatalf("order changed result: %+v vs %+v", a, b)
	}
}

func TestNewAssigner(t *testing.T) {
	if a, _ := NewAssigner(time.Minute, 0); a.Name() != "tumbling(1m0s)" {
		t.Error(a.Name())
	}
	if a, _ := NewAssigner(time.Minute, 10*time.Second); a.Name() != "sliding(1m0s/10s)" {
		t.Error(a.Name())
	}
	if _, err := NewAssigner(time.Second, 2*time.Second); err == nil {
		t.Error("slide > size should fail")
	}
}
