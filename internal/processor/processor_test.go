package processor

import (
	"testing"
	"time"
)

func TestPollBackoffBoundsAndGrowth(t *testing.T) {
	prevMax := time.Duration(0)
	for n := 1; n <= 12; n++ {
		lo, hi := time.Hour, time.Duration(0)
		for i := 0; i < 200; i++ {
			d := pollBackoff(n)
			lo, hi = min(lo, d), max(hi, d)
		}
		if hi > 6*time.Second {
			t.Fatalf("n=%d: backoff %v exceeds the 5s cap (+20%% jitter)", n, hi)
		}
		if lo < 80*time.Millisecond {
			t.Fatalf("n=%d: backoff %v below the 100ms floor (-20%% jitter)", n, lo)
		}
		if n <= 4 && hi < prevMax {
			t.Fatalf("n=%d: backoff did not grow (%v < %v)", n, hi, prevMax)
		}
		prevMax = hi
	}
}

func TestConfigValidation(t *testing.T) {
	if err := (&Config{}).ApplyDefaults(); err == nil {
		t.Error("missing topic accepted")
	}
	if err := (&Config{Topic: "t", QueueCapacity: 10, HighWater: 20}).ApplyDefaults(); err == nil {
		t.Error("high-water > capacity accepted")
	}
	if err := (&Config{Topic: "t", QueueCapacity: 10, HighWater: 5, LowWater: 5}).ApplyDefaults(); err == nil {
		t.Error("low-water >= high-water accepted")
	}
	c := Config{Topic: "t", WindowSize: time.Second, WindowSlide: 2 * time.Second}
	if err := c.ApplyDefaults(); err == nil {
		t.Error("slide > size accepted")
	}
	ok := Config{Topic: "t"}
	if err := ok.ApplyDefaults(); err != nil || ok.MaxPermanentSinkFailures == 0 || ok.Assigner == nil {
		t.Errorf("defaults not applied: %+v err=%v", ok, err)
	}
}
