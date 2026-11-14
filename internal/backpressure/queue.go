// Package backpressure keeps a fast producer from drowning a slow consumer.
//
// The core idea: never let memory be the buffer. Every hand-off between two
// pipeline stages goes through a BOUNDED queue, and when that queue fills the
// pressure is propagated upstream — all the way back to the broker — instead of
// being absorbed by unbounded growth (the classic way stream jobs OOM).
//
// Three cooperating mechanisms live here:
//
//	Queue        bounded FIFO with high/low watermark callbacks. At the high mark
//	             the consumer PAUSES the partition at the broker; at the low mark
//	             it RESUMES. The gap between the marks is hysteresis, which stops
//	             the pause/resume flapping every few messages.
//	TokenBucket  a rate limiter (smooth throttle with bounded bursts).
//	AIMD         additive-increase / multiplicative-decrease control of a
//	             TokenBucket's rate, so throughput finds its own ceiling.
//
// Credits: AIMD is from Chiu & Jain, "Analysis of the Increase and Decrease
// Algorithms for Congestion Avoidance in Computer Networks" (1989) — the same
// feedback rule that makes TCP congestion control stable. The token bucket is
// the standard network-traffic-shaping construct.
package backpressure

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// ErrClosed is returned by Pop once a closed queue has been fully drained.
var ErrClosed = errors.New("backpressure: queue closed")

// Queue is a bounded FIFO safe for one or more producers and one or more
// consumers. Close must only be called after every Put has returned.
type Queue[T any] struct {
	ch        chan T
	high, low int

	mu        sync.Mutex
	saturated bool // true between a high-water crossing and the next low-water crossing
	onHigh    func()
	onLow     func()

	pauses atomic.Uint64
}

// NewQueue creates a queue holding up to capacity items. onHigh fires when the
// depth first reaches `high`; onLow fires when a saturated queue drains to `low`.
// Either callback may be nil. Callbacks run on the goroutine that caused the
// crossing and OUTSIDE the queue's lock, so they may call back into the queue.
func NewQueue[T any](capacity, high, low int, onHigh, onLow func()) *Queue[T] {
	if capacity < 1 {
		capacity = 1
	}
	if high < 1 || high > capacity {
		high = capacity
	}
	if low < 0 || low >= high {
		low = high / 2
	}
	return &Queue[T]{ch: make(chan T, capacity), high: high, low: low, onHigh: onHigh, onLow: onLow}
}

// Put enqueues v, blocking while the queue is full. This blocking IS the
// backpressure: whoever calls Put is stalled until the consumer catches up.
func (q *Queue[T]) Put(ctx context.Context, v T) error {
	select {
	case q.ch <- v:
	case <-ctx.Done():
		return ctx.Err()
	}
	if len(q.ch) >= q.high {
		q.mu.Lock()
		fire := !q.saturated
		q.saturated = true
		cb := q.onHigh
		q.mu.Unlock()
		if fire {
			q.pauses.Add(1)
			if cb != nil {
				cb()
			}
		}
	}
	return nil
}

// Pop blocks until an item is available, ctx ends, or wake fires.
//
//	(item, true,  nil)         got an item
//	(zero, false, nil)         wake fired (the caller's housekeeping tick)
//	(zero, false, ctx.Err())   context ended
//	(zero, false, ErrClosed)   queue closed AND empty
//
// The wake channel lets a single goroutine multiplex "process data" and "do
// periodic work" without extra locking — the partition task relies on this to
// own its window state exclusively.
func (q *Queue[T]) Pop(ctx context.Context, wake <-chan time.Time) (T, bool, error) {
	var zero T
	select {
	case v, ok := <-q.ch:
		if !ok {
			return zero, false, ErrClosed
		}
		if len(q.ch) <= q.low {
			q.mu.Lock()
			fire := q.saturated
			q.saturated = false
			cb := q.onLow
			q.mu.Unlock()
			if fire && cb != nil {
				cb()
			}
		}
		return v, true, nil
	case <-wake:
		return zero, false, nil
	case <-ctx.Done():
		return zero, false, ctx.Err()
	}
}

// Close signals that no more items will be Put. Remaining items can still be
// popped; afterwards Pop returns ErrClosed.
func (q *Queue[T]) Close() { close(q.ch) }

// Len is the current depth. Cap is the capacity.
func (q *Queue[T]) Len() int { return len(q.ch) }
func (q *Queue[T]) Cap() int { return cap(q.ch) }

// Saturated reports whether the queue is between its high and low water marks.
func (q *Queue[T]) Saturated() bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.saturated
}

// Pauses is how many times the high-water mark has been crossed.
func (q *Queue[T]) Pauses() uint64 { return q.pauses.Load() }
