package partitioning

import (
	"fmt"
	"testing"
)

func TestDeterministicAndInRange(t *testing.T) {
	for _, p := range []Partitioner{Hash{}, Jump{}} {
		for i := 0; i < 1000; i++ {
			k := []byte(fmt.Sprintf("key-%d", i))
			a, b := p.Partition(k, 7), p.Partition(k, 7)
			if a != b || a < 0 || a >= 7 {
				t.Fatalf("%s: key %s -> %d,%d", p.Name(), k, a, b)
			}
		}
	}
}

func TestDistributionRoughlyUniform(t *testing.T) {
	for _, p := range []Partitioner{Hash{}, Jump{}} {
		const n, keys = 8, 80000
		counts := make([]int, n)
		for i := 0; i < keys; i++ {
			counts[p.Partition([]byte(fmt.Sprintf("sensor-%d", i)), n)]++
		}
		for part, c := range counts {
			if dev := float64(c)/float64(keys/n) - 1; dev > 0.05 || dev < -0.05 {
				t.Errorf("%s: partition %d has %d keys (%.1f%% off uniform)", p.Name(), part, c, dev*100)
			}
		}
	}
}

// The property that justifies Jump Hash: growing N -> N+1 should relocate only
// about 1/(N+1) of keys. Modulo hashing relocates the vast majority.
func TestJumpMovesMinimalKeys(t *testing.T) {
	const keys = 50000
	moved := func(p Partitioner, from, to int) float64 {
		m := 0
		for i := 0; i < keys; i++ {
			k := []byte(fmt.Sprintf("k%d", i))
			if p.Partition(k, from) != p.Partition(k, to) {
				m++
			}
		}
		return float64(m) / keys
	}
	j, h := moved(Jump{}, 10, 11), moved(Hash{}, 10, 11)
	if j > 0.12 { // ideal 1/11 ≈ 9.1%
		t.Errorf("jump moved %.1f%% of keys, want ≈9%%", j*100)
	}
	if h < 0.5 {
		t.Errorf("modulo moved only %.1f%%; expected most keys to move", h*100)
	}
	t.Logf("10→11 partitions: jump moved %.1f%%, modulo moved %.1f%%", j*100, h*100)
}

func TestRoundRobinCycles(t *testing.T) {
	r := &RoundRobin{}
	for i := 0; i < 9; i++ {
		if got := r.Partition(nil, 3); got != i%3 {
			t.Fatalf("step %d: got %d", i, got)
		}
	}
}

func TestParse(t *testing.T) {
	if _, err := Parse("nope"); err == nil {
		t.Fatal("expected error")
	}
	if p, _ := Parse("jump"); p.Name() != "jump" {
		t.Fatal("wrong partitioner")
	}
}
