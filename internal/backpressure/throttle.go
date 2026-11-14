package backpressure

import (
	"context"
	"sync"
	"time"
)

// TokenBucket is a classic token-bucket limiter: tokens accrue at `rate` per
// second up to `burst`; taking n events consumes n tokens. A nil *TokenBucket
// (or rate <= 0) means "unlimited", so callers needn't branch on whether
// throttling is configured.
type TokenBucket struct {
	mu     sync.Mutex
	rate   float64 // tokens per second
	burst  float64
	tokens float64
	last   time.Time
	now    func() time.Time
}

// NewTokenBucket returns a limiter allowing `rate` events/sec with bursts up to
// `burst` (defaults to one second's worth).
func NewTokenBucket(rate, burst float64) *TokenBucket {
	if burst < 1 {
		burst = rate
	}
	if burst < 1 {
		burst = 1
	}
	return &TokenBucket{rate: rate, burst: burst, tokens: burst, now: time.Now, last: time.Now()}
}

// refillLocked credits tokens for elapsed time. Caller holds mu.
func (b *TokenBucket) refillLocked() {
	now := b.now()
	if dt := now.Sub(b.last).Seconds(); dt > 0 {
		b.tokens += dt * b.rate
		if b.tokens > b.burst {
			b.tokens = b.burst
		}
		b.last = now
	}
}

// Allow takes n tokens if immediately available, without blocking.
func (b *TokenBucket) Allow(n float64) bool {
	if b == nil || b.rate <= 0 {
		return true
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	b.refillLocked()
	if b.tokens >= n {
		b.tokens -= n
		return true
	}
	return false
}

// Wait blocks until n tokens are available or ctx ends.
//
// It RESERVES tokens up-front (letting the balance go negative) and then sleeps
// for the computed deficit. Reserving first is what keeps concurrent waiters
// fair and prevents a thundering herd: each caller's wait already accounts for
// everyone queued ahead of it.
func (b *TokenBucket) Wait(ctx context.Context, n float64) error {
	if b == nil {
		return nil
	}
	b.mu.Lock()
	if b.rate <= 0 {
		b.mu.Unlock()
		return nil
	}
	b.refillLocked()
	b.tokens -= n
	var wait time.Duration
	if b.tokens < 0 {
		wait = time.Duration(-b.tokens / b.rate * float64(time.Second))
	}
	b.mu.Unlock()
	if wait <= 0 {
		return nil
	}
	t := time.NewTimer(wait)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		b.mu.Lock() // give the reservation back; we didn't use it
		b.tokens += n
		b.mu.Unlock()
		return ctx.Err()
	}
}

// SetRate changes the refill rate on the fly (used by AIMD).
func (b *TokenBucket) SetRate(rate float64) {
	if b == nil {
		return
	}
	b.mu.Lock()
	b.refillLocked() // settle accrued tokens at the OLD rate first
	b.rate = rate
	b.mu.Unlock()
}

// Rate returns the current rate.
func (b *TokenBucket) Rate() float64 {
	if b == nil {
		return 0
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.rate
}

// AIMD tunes a TokenBucket's rate from congestion feedback:
//
//	congestion (queue hit high-water)  → rate *= Decrease   (multiplicative cut, fast)
//	health     (queue drained to low)  → rate += Increase   (additive probe, slow)
//
// Cutting fast and probing slowly converges on the highest sustainable rate
// without oscillating wildly — Chiu & Jain proved AIMD is the combination that
// converges to a fair, efficient operating point.
type AIMD struct {
	mu       sync.Mutex
	bucket   *TokenBucket
	min, max float64
	Increase float64 // events/sec added per healthy signal
	Decrease float64 // multiplier in (0,1) applied per congestion signal
}

// NewAIMD controls bucket within [min, max], starting at max.
func NewAIMD(bucket *TokenBucket, min, max float64) *AIMD {
	if min <= 0 {
		min = 1
	}
	if max < min {
		max = min
	}
	bucket.SetRate(max)
	return &AIMD{bucket: bucket, min: min, max: max, Increase: max / 20, Decrease: 0.5}
}

// OnCongestion applies the multiplicative decrease.
func (a *AIMD) OnCongestion() {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.bucket.Rate() * a.Decrease
	if r < a.min {
		r = a.min
	}
	a.bucket.SetRate(r)
}

// OnHealthy applies the additive increase.
func (a *AIMD) OnHealthy() {
	a.mu.Lock()
	defer a.mu.Unlock()
	r := a.bucket.Rate() + a.Increase
	if r > a.max {
		r = a.max
	}
	a.bucket.SetRate(r)
}
