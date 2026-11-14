// Package partitioning maps message keys to broker partitions.
//
// Why it matters: a stream processor can only keep per-key state (a window
// accumulator, a dedup cache) in memory if every event for a given key lands on
// the SAME partition, and therefore the same consumer. The partitioner is what
// guarantees that locality.
//
// Three strategies are provided:
//
//	Hash        – FNV-1a modulo N. Simple, fast, what most Kafka clients do
//	              (Kafka's default uses murmur2; the property we need is the same).
//	Jump        – Jump Consistent Hash. When partitions grow from N to N+1 only
//	              ~1/(N+1) of keys move, versus ~N/(N+1) for modulo hashing.
//	RoundRobin  – for key-less messages where spreading load beats locality.
//
// Credits:
//   - FNV hash: Glenn Fowler, Landon Curt Noll, Phong Vo.
//   - Jump Consistent Hash: John Lamping & Eric Veach, "A Fast, Minimal Memory,
//     Consistent Hash Algorithm", Google, 2014 (arXiv:1406.2294).
package partitioning

import (
	"fmt"
	"hash/fnv"
	"sync/atomic"
)

// Partitioner chooses a partition in [0, n) for a key. Implementations must be
// safe for concurrent use and deterministic for a given (key, n) unless
// documented otherwise (RoundRobin).
type Partitioner interface {
	Partition(key []byte, n int) int
	Name() string
}

// Hash is modulo-FNV-1a partitioning.
type Hash struct{}

func (Hash) Name() string { return "hash" }

func (Hash) Partition(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New32a()
	_, _ = h.Write(key) // hash.Hash.Write never returns an error
	return int(h.Sum32() % uint32(n))
}

// Jump is Jump Consistent Hash over an FNV-64a digest of the key.
type Jump struct{}

func (Jump) Name() string { return "jump" }

func (Jump) Partition(key []byte, n int) int {
	if n <= 1 {
		return 0
	}
	h := fnv.New64a()
	_, _ = h.Write(key)
	return int(JumpHash(h.Sum64(), int32(n)))
}

// JumpHash is the reference algorithm from the Lamping/Veach paper, translated
// from the C++ listing. It simulates a sequence of "should this key jump to
// bucket j?" coin flips driven by a linear congruential generator seeded with
// the key, which is why it needs no lookup table and runs in O(ln n).
func JumpHash(key uint64, buckets int32) int32 {
	var b, j int64 = -1, 0
	for j < int64(buckets) {
		b = j
		key = key*2862933555777941757 + 1 // 64-bit LCG step
		j = int64(float64(b+1) * (float64(int64(1)<<31) / float64((key>>33)+1)))
	}
	return int32(b)
}

// RoundRobin ignores the key and cycles through partitions.
type RoundRobin struct{ ctr atomic.Uint64 }

func (*RoundRobin) Name() string { return "round_robin" }

func (r *RoundRobin) Partition(_ []byte, n int) int {
	if n <= 1 {
		return 0
	}
	return int((r.ctr.Add(1) - 1) % uint64(n))
}

// Parse returns the partitioner registered under name.
func Parse(name string) (Partitioner, error) {
	switch name {
	case "", "hash":
		return Hash{}, nil
	case "jump":
		return Jump{}, nil
	case "round_robin":
		return &RoundRobin{}, nil
	}
	return nil, fmt.Errorf("unknown partitioner %q (want hash|jump|round_robin)", name)
}
