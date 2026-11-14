package backpressure

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestQueueHysteresis(t *testing.T) {
	var highs, lows atomic.Int32
	q := NewQueue[int](10, 8, 3, func() { highs.Add(1) }, func() { lows.Add(1) })
	ctx := context.Background()

	for i := 0; i < 8; i++ {
		_ = q.Put(ctx, i)
	}
	if highs.Load() != 1 || !q.Saturated() {
		t.Fatalf("expected one high-water event, got %d", highs.Load())
	}
	// Crossing high again while already saturated must NOT re-fire.
	_ = q.Put(ctx, 99)
	if highs.Load() != 1 {
		t.Fatal("high-water fired twice without draining")
	}
	// Drain to just above low: still saturated (hysteresis).
	for q.Len() > 4 {
		q.Pop(ctx, nil)
	}
	if lows.Load() != 0 {
		t.Fatal("low-water fired too early")
	}
	for q.Len() > 3 {
		q.Pop(ctx, nil)
	}
	if lows.Load() != 1 || q.Saturated() {
		t.Fatalf("low-water events=%d saturated=%v", lows.Load(), q.Saturated())
	}
}

func TestQueuePutBlocksWhenFull(t *testing.T) {
	q := NewQueue[int](2, 2, 0, nil, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_ = q.Put(ctx, 1)
	_ = q.Put(ctx, 2)
	if err := q.Put(ctx, 3); err == nil {
		t.Fatal("Put on a full queue should block until ctx expires")
	}
}

func TestQueueWakeAndClose(t *testing.T) {
	q := NewQueue[int](4, 4, 1, nil, nil)
	tick := make(chan time.Time, 1)
	tick <- time.Now()
	if _, ok, err := q.Pop(context.Background(), tick); ok || err != nil {
		t.Fatalf("wake should yield (zero,false,nil), got ok=%v err=%v", ok, err)
	}
	_ = q.Put(context.Background(), 7)
	q.Close()
	if v, ok, _ := q.Pop(context.Background(), nil); !ok || v != 7 {
		t.Fatal("closed queue must still drain")
	}
	if _, _, err := q.Pop(context.Background(), nil); err != ErrClosed {
		t.Fatalf("want ErrClosed, got %v", err)
	}
}

func TestTokenBucketRateLimits(t *testing.T) {
	b := NewTokenBucket(200, 10) // 200/s, burst 10
	start := time.Now()
	for i := 0; i < 50; i++ { // 10 free from burst, 40 more at 200/s ≈ 200ms
		if err := b.Wait(context.Background(), 1); err != nil {
			t.Fatal(err)
		}
	}
	if el := time.Since(start); el < 150*time.Millisecond || el > 600*time.Millisecond {
		t.Fatalf("50 events took %v, expected ~200ms", el)
	}
}

func TestTokenBucketNilIsUnlimited(t *testing.T) {
	var b *TokenBucket
	if !b.Allow(1e9) || b.Wait(context.Background(), 1e9) != nil {
		t.Fatal("nil bucket must be a no-op")
	}
}

func TestAIMD(t *testing.T) {
	b := NewTokenBucket(1000, 100)
	a := NewAIMD(b, 10, 1000)
	a.OnCongestion()
	if r := b.Rate(); r != 500 {
		t.Fatalf("multiplicative decrease: %v", r)
	}
	for i := 0; i < 5; i++ {
		a.OnCongestion()
	}
	if r := b.Rate(); r != 15.625 {
		t.Fatalf("rate after 6 cuts = %v", r)
	}
	a.OnCongestion()
	a.OnCongestion()
	if r := b.Rate(); r != 10 {
		t.Fatalf("floor not enforced: %v", r)
	}
	for i := 0; i < 100; i++ {
		a.OnHealthy()
	}
	if r := b.Rate(); r != 1000 {
		t.Fatalf("ceiling not enforced: %v", r)
	}
}
