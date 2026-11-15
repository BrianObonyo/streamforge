package reliability

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"
)

var t0 = time.Date(2026, 11, 14, 0, 0, 0, 0, time.UTC)

func TestDeduperBasicAndTTL(t *testing.T) {
	d := NewDeduper(10*time.Second, 0)
	if d.Seen("a", t0) {
		t.Fatal("first sighting is not a duplicate")
	}
	if !d.Seen("a", t0.Add(5*time.Second)) {
		t.Fatal("second sighting inside TTL must be a duplicate")
	}
	// Past the TTL the entry is forgotten (documented limit).
	if d.Seen("a", t0.Add(11*time.Second)) {
		t.Fatal("entry should have expired")
	}
}

func TestDeduperSizeBound(t *testing.T) {
	d := NewDeduper(0, 3)
	for _, id := range []string{"a", "b", "c", "d"} { // "a" evicted by "d"
		d.Seen(id, t0)
	}
	if d.Len() != 3 {
		t.Fatalf("len=%d want 3", d.Len())
	}
	if d.Seen("a", t0) { // forgotten ⇒ looks new (and evicts "b")
		t.Fatal("evicted id should look new")
	}
	if !d.Seen("d", t0) {
		t.Fatal("recent id should still be remembered")
	}
}

func TestRetryBackoffBounds(t *testing.T) {
	p := RetryPolicy{Base: 10 * time.Millisecond, Max: 80 * time.Millisecond}
	for attempt := 0; attempt < 40; attempt++ { // 40 also exercises shift-overflow clamping
		for i := 0; i < 50; i++ {
			if d := p.Backoff(attempt); d < 0 || d > p.Max {
				t.Fatalf("attempt %d: backoff %v out of [0,%v]", attempt, d, p.Max)
			}
		}
	}
}

func TestDoRetriesThenSucceeds(t *testing.T) {
	calls := 0
	err := Do(context.Background(), RetryPolicy{MaxAttempts: 5, Base: time.Millisecond, Max: 2 * time.Millisecond}, func() error {
		if calls++; calls < 3 {
			return errors.New("transient")
		}
		return nil
	})
	if err != nil || calls != 3 {
		t.Fatalf("err=%v calls=%d", err, calls)
	}
}

func TestDoStopsOnPermanentAndExhausts(t *testing.T) {
	calls := 0
	sentinel := errors.New("bad input")
	err := Do(context.Background(), DefaultRetry, func() error { calls++; return Permanent(sentinel) })
	if !errors.Is(err, sentinel) || calls != 1 {
		t.Fatalf("permanent error retried: calls=%d err=%v", calls, err)
	}
	calls = 0
	err = Do(context.Background(), RetryPolicy{MaxAttempts: 3, Base: time.Millisecond, Max: time.Millisecond}, func() error { calls++; return sentinel })
	if !errors.Is(err, sentinel) || calls != 3 {
		t.Fatalf("calls=%d err=%v", calls, err)
	}
}

func TestDoHonoursContext(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := Do(ctx, RetryPolicy{MaxAttempts: 5, Base: time.Hour, Max: time.Hour}, func() error { return errors.New("x") })
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v", err)
	}
}

func TestCircuitBreakerLifecycle(t *testing.T) {
	now := t0
	b := NewCircuitBreaker(3, 10*time.Second)
	b.now = func() time.Time { return now }
	boom := errors.New("down")

	for i := 0; i < 3; i++ {
		_ = b.Do(func() error { return boom })
	}
	if b.State() != "open" {
		t.Fatalf("state=%s want open", b.State())
	}
	called := false
	if err := b.Do(func() error { called = true; return nil }); !errors.Is(err, ErrCircuitOpen) || called {
		t.Fatal("open breaker must fail fast without calling fn")
	}

	now = now.Add(11 * time.Second) // cool-down over → half-open probe
	if err := b.Do(func() error { return boom }); err != boom {
		t.Fatalf("probe should run: %v", err)
	}
	if b.State() != "open" {
		t.Fatalf("failed probe must re-open, got %s", b.State())
	}

	now = now.Add(11 * time.Second)
	if err := b.Do(func() error { return nil }); err != nil {
		t.Fatal(err)
	}
	if b.State() != "closed" {
		t.Fatalf("successful probe must close, got %s", b.State())
	}
}

func TestMemDLQ(t *testing.T) {
	var q MemDLQ
	_ = q.Send(context.Background(), DeadLetter{Reason: ReasonPoison})
	_ = q.Send(context.Background(), DeadLetter{Reason: ReasonPoison})
	_ = q.Send(context.Background(), DeadLetter{Reason: ReasonTooLate})
	if c := q.CountByReason(); c[ReasonPoison] != 2 || c[ReasonTooLate] != 1 {
		t.Fatalf("%v", c)
	}
}

// The breaker guards AVAILABILITY. A permanent error means the dependency
// answered (and said no), so it must not count toward tripping.
func TestBreakerIgnoresPermanentErrors(t *testing.T) {
	b := NewCircuitBreaker(2, time.Hour)
	for i := 0; i < 10; i++ {
		_ = b.Do(func() error { return Permanent(errors.New("bad schema")) })
	}
	if b.State() != "closed" {
		t.Fatalf("breaker tripped on permanent errors: %s", b.State())
	}
	for i := 0; i < 2; i++ {
		_ = b.Do(func() error { return errors.New("connection refused") })
	}
	if b.State() != "open" {
		t.Fatalf("transient errors must still trip it: %s", b.State())
	}
}

func TestIsPermanentSurvivesWrapping(t *testing.T) {
	base := errors.New("x")
	if IsPermanent(base) || !IsPermanent(Permanent(base)) || !IsPermanent(fmt.Errorf("ctx: %w", Permanent(base))) {
		t.Fatal("IsPermanent misclassifies")
	}
	err := Do(context.Background(), DefaultRetry, func() error { return Permanent(base) })
	if !IsPermanent(err) || !errors.Is(err, base) {
		t.Fatalf("Do must preserve the marker AND the cause: %v", err)
	}
}
