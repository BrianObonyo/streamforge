// Package reliability holds the fault-tolerance primitives the pipeline uses to
// survive bad data, flaky dependencies and restarts without silently losing or
// double-counting events:
//
//	Deduper         drops repeated event IDs (duplicate detection)
//	Retry / Do      exponential backoff with full jitter
//	CircuitBreaker  stops hammering a dependency that is clearly down
//	DLQ             parks messages we cannot process, with the reason, for humans
//
// Credits:
//   - Backoff with jitter: Marc Brooker, "Exponential Backoff And Jitter",
//     AWS Architecture Blog (2015) — "full jitter" gave the lowest contention.
//   - Circuit breaker: Michael Nygard, "Release It!" (2007); Martin Fowler's
//     CircuitBreaker bliki entry.
//   - Dead-letter queues: the standard enterprise-integration pattern
//     (Hohpe & Woolf, "Enterprise Integration Patterns", 2003).
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package reliability

import (
	"container/list"
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"
)

// ------------------------------------------------------------------- Deduper

// Deduper remembers recently seen IDs so replays and producer retries don't get
// counted twice. It is bounded two ways — by TTL and by entry count — because an
// unbounded "seen" set is a memory leak that grows with stream lifetime.
//
// Honest limits (documented, not hidden): the window of protection is
// min(TTL, MaxEntries / event-rate). A duplicate arriving after its original has
// been evicted will be counted again. Exactly-once across ARBITRARY gaps needs
// durable state (e.g. a sink-side unique key); the processor layers idempotent,
// versioned sink writes on top of this for exactly that reason.
type Deduper struct {
	mu   sync.Mutex
	ttl  time.Duration
	max  int
	ll   *list.List // front = oldest
	byID map[string]*list.Element
}

type seenEntry struct {
	id string
	at time.Time
}

// NewDeduper creates a Deduper. ttl<=0 disables time eviction; max<=0 disables
// size eviction.
func NewDeduper(ttl time.Duration, max int) *Deduper {
	return &Deduper{ttl: ttl, max: max, ll: list.New(), byID: map[string]*list.Element{}}
}

// Seen returns true if id was already observed (a duplicate). Otherwise it
// records id and returns false. Duplicates do not refresh the entry's age.
func (d *Deduper) Seen(id string, now time.Time) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	d.evictLocked(now)
	if _, dup := d.byID[id]; dup {
		return true
	}
	d.byID[id] = d.ll.PushBack(seenEntry{id, now})
	if d.max > 0 && d.ll.Len() > d.max {
		d.dropFrontLocked()
	}
	return false
}

// Len reports the number of remembered IDs.
func (d *Deduper) Len() int {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.ll.Len()
}

func (d *Deduper) evictLocked(now time.Time) {
	if d.ttl <= 0 {
		return
	}
	for e := d.ll.Front(); e != nil; e = d.ll.Front() {
		if now.Sub(e.Value.(seenEntry).at) < d.ttl {
			return // list is age-ordered, so everything after this is younger
		}
		d.dropFrontLocked()
	}
}

func (d *Deduper) dropFrontLocked() {
	e := d.ll.Front()
	if e == nil {
		return
	}
	delete(d.byID, e.Value.(seenEntry).id)
	d.ll.Remove(e)
}

// --------------------------------------------------------------------- Retry

// RetryPolicy configures Do.
type RetryPolicy struct {
	MaxAttempts int           // total tries including the first; <1 means 1
	Base        time.Duration // first backoff ceiling
	Max         time.Duration // backoff ceiling cap
}

// DefaultRetry is a sensible production default: 5 tries, 50 ms → 2 s ceiling.
var DefaultRetry = RetryPolicy{MaxAttempts: 5, Base: 50 * time.Millisecond, Max: 2 * time.Second}

// Backoff returns how long to sleep after failed attempt number `attempt`
// (0-based). Full jitter: uniform in [0, min(Max, Base·2^attempt)].
//
// Why jitter? Without it, many clients that failed together retry together and
// re-create the overload that hurt them ("thundering herd"). Randomising the
// whole interval decorrelates them (Brooker, 2015).
func (p RetryPolicy) Backoff(attempt int) time.Duration {
	ceil := p.Base << uint(min(attempt, 30)) // clamp shift to avoid overflow
	if ceil <= 0 || ceil > p.Max {
		ceil = p.Max
	}
	if ceil <= 0 {
		return 0
	}
	return time.Duration(rand.Int64N(int64(ceil) + 1))
}

type permanentError struct{ err error }

func (e permanentError) Error() string { return e.err.Error() }
func (e permanentError) Unwrap() error { return e.err }

// Permanent marks an error as non-retryable (bad input will never succeed).
func Permanent(err error) error {
	if err == nil {
		return nil
	}
	return permanentError{err}
}

// Do runs fn, retrying failures per policy. It stops early on ctx cancellation or
// a Permanent error, and returns the last error wrapped with the attempt count.
func Do(ctx context.Context, p RetryPolicy, fn func() error) error {
	attempts := max(p.MaxAttempts, 1)
	var err error
	for i := 0; i < attempts; i++ {
		if err = fn(); err == nil {
			return nil
		}
		var perm permanentError
		if errors.As(err, &perm) {
			return perm.err
		}
		if i == attempts-1 {
			break
		}
		t := time.NewTimer(p.Backoff(i))
		select {
		case <-t.C:
		case <-ctx.Done():
			t.Stop()
			return fmt.Errorf("retry aborted after %d attempt(s): %w (last error: %v)", i+1, ctx.Err(), err)
		}
	}
	return fmt.Errorf("giving up after %d attempt(s): %w", attempts, err)
}

// ------------------------------------------------------------ CircuitBreaker

// ErrCircuitOpen is returned without calling the dependency while the breaker is open.
var ErrCircuitOpen = errors.New("circuit breaker open")

type breakerState int

const (
	stClosed   breakerState = iota // normal: calls flow through
	stOpen                         // tripped: calls fail fast
	stHalfOpen                     // probing: exactly one trial call allowed
)

func (s breakerState) String() string { return [...]string{"closed", "open", "half-open"}[s] }

// CircuitBreaker fails fast while a dependency is down, giving it room to recover
// instead of piling retries onto it.
//
//	closed ──N consecutive failures──▶ open ──OpenFor elapsed──▶ half-open
//	   ▲                                                            │
//	   └────────────── probe succeeds ──────────────────────────────┤
//	                                   open ◀── probe fails ────────┘
type CircuitBreaker struct {
	mu        sync.Mutex
	threshold int
	openFor   time.Duration
	now       func() time.Time

	state    breakerState
	failures int
	openedAt time.Time
	probing  bool // a half-open trial call is in flight
}

// NewCircuitBreaker trips after `threshold` consecutive failures and stays open
// for `openFor` before allowing a trial call.
func NewCircuitBreaker(threshold int, openFor time.Duration) *CircuitBreaker {
	return &CircuitBreaker{threshold: max(threshold, 1), openFor: openFor, now: time.Now}
}

// State returns the current state name (for metrics and logs).
func (b *CircuitBreaker) State() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.state.String()
}

// Do calls fn unless the breaker is open.
func (b *CircuitBreaker) Do(fn func() error) error {
	if err := b.before(); err != nil {
		return err
	}
	err := fn()
	b.after(err)
	return err
}

func (b *CircuitBreaker) before() error {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch b.state {
	case stOpen:
		if b.now().Sub(b.openedAt) < b.openFor {
			return ErrCircuitOpen
		}
		b.state, b.probing = stHalfOpen, false
		fallthrough
	case stHalfOpen:
		if b.probing {
			return ErrCircuitOpen // only ONE trial at a time
		}
		b.probing = true
	}
	return nil
}

func (b *CircuitBreaker) after(err error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if err == nil {
		b.state, b.failures, b.probing = stClosed, 0, false
		return
	}
	b.failures++
	if b.state == stHalfOpen || b.failures >= b.threshold {
		b.state, b.openedAt, b.probing = stOpen, b.now(), false
	}
}
