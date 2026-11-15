// Package streaming defines the broker-facing abstractions StreamForge is built
// on, and ships a complete in-memory implementation (see membroker.go).
//
// The processing engine never talks to Redpanda directly; it talks to the
// Broker / Producer / Consumer interfaces below, which mirror the subset of
// Kafka semantics the engine relies on (Redpanda speaks the Kafka wire protocol):
//
//   - a topic is split into ordered, append-only partitions;
//   - a record's identity is (topic, partition, offset);
//   - consumers in one GROUP divide the partitions between them, and the group
//     coordinator re-divides them when members join or leave (a "rebalance");
//   - progress is recorded by COMMITTING the next offset to read, which is what a
//     restarted consumer resumes from (at-least-once delivery).
//
// Having an in-memory implementation behind the same interface means the whole
// pipeline — including rebalances and crash recovery — is unit-testable without
// Docker, and the Redpanda adapter (redpanda.go, build tag `redpanda`) is a thin
// translation layer rather than a place where logic hides.
//
// Semantics reference: Kafka consumer group protocol, as described in Kreps,
// Narkhede & Rao, "Kafka: a Distributed Messaging System for Log Processing"
// (NetDB 2011) and the Apache Kafka design documentation.
package streaming

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/BrianObonyo/streamforge/internal/partitioning"
)

// Sentinel errors. Use errors.Is to test for them.
var (
	ErrClosed       = errors.New("streaming: closed")
	ErrUnknownTopic = errors.New("streaming: unknown topic")
	ErrNotAssigned  = errors.New("streaming: partition not assigned to this consumer")
)

// TopicPartition identifies one partition of one topic.
type TopicPartition struct {
	Topic     string
	Partition int
}

func (tp TopicPartition) String() string { return fmt.Sprintf("%s/%d", tp.Topic, tp.Partition) }

// Record is one message as stored by the broker.
type Record struct {
	Topic     string
	Partition int
	Offset    int64
	Key       []byte
	Value     []byte
	Timestamp time.Time // broker append time; used for end-to-end latency
}

// TP returns the record's topic-partition.
func (r Record) TP() TopicPartition { return TopicPartition{r.Topic, r.Partition} }

// Producer appends records to topics.
type Producer interface {
	// Produce appends one record and returns where it landed. A non-empty key
	// is routed by the partitioner so equal keys share a partition.
	Produce(ctx context.Context, topic string, key, value []byte) (partition int, offset int64, err error)
	Close() error
}

// KV is one message in a batch.
type KV struct{ Key, Value []byte }

// BatchProducer is an optional Producer capability: send many messages in one
// call so a network broker can pipeline them instead of paying one round trip per
// message (the difference between ~1 k and ~100 k msgs/s per producer).
type BatchProducer interface {
	ProduceBatch(ctx context.Context, topic string, msgs []KV) error
}

// ProduceBatch sends msgs via the producer's batch path if it has one, else one at
// a time. A returned error means the batch MAY be partially written; callers that
// retry should rely on downstream dedup (the processor does).
func ProduceBatch(ctx context.Context, p Producer, topic string, msgs []KV) error {
	if bp, ok := p.(BatchProducer); ok {
		return bp.ProduceBatch(ctx, topic, msgs)
	}
	for _, m := range msgs {
		if _, _, err := p.Produce(ctx, topic, m.Key, m.Value); err != nil {
			return err
		}
	}
	return nil
}

// Consumer is a member of a consumer group.
type Consumer interface {
	// Poll returns up to max records from assigned, un-paused partitions. It
	// blocks until records exist, the assignment changes (returning an empty
	// slice and nil error so the caller can re-read Assignment), or ctx ends.
	Poll(ctx context.Context, max int) ([]Record, error)

	// Commit records that everything BEFORE each given offset has been fully
	// processed. Offsets are "next offset to read", matching Kafka.
	Commit(ctx context.Context, offsets map[TopicPartition]int64) error

	// Assignment returns the partitions currently owned by this consumer.
	Assignment() []TopicPartition

	// Pause stops Poll from returning records for the partitions (backpressure);
	// Resume undoes it. Both are safe to call from any goroutine.
	Pause(tps ...TopicPartition)
	Resume(tps ...TopicPartition)

	// Close leaves the group, triggering a rebalance for the remaining members.
	Close() error
}

// Broker creates topics, producers and consumers.
type Broker interface {
	CreateTopic(ctx context.Context, name string, partitions int) error
	NewProducer(p partitioning.Partitioner) (Producer, error)
	NewConsumer(group, memberID string, topics ...string) (Consumer, error)
	Close() error
}

// --------------------------------------------------------------------- registry

// Factory builds a Broker from a list of seed addresses.
type Factory func(addrs []string) (Broker, error)

var (
	regMu    sync.RWMutex
	registry = map[string]Factory{}
)

// Register makes a broker implementation selectable by name (e.g. from a CLI
// flag). Implementations call it from init(); build-tagged adapters therefore
// appear only in binaries compiled with their tag.
func Register(name string, f Factory) {
	regMu.Lock()
	defer regMu.Unlock()
	registry[name] = f
}

// Open instantiates the named broker implementation.
func Open(name string, addrs []string) (Broker, error) {
	regMu.RLock()
	f, ok := registry[name]
	regMu.RUnlock()
	if !ok {
		return nil, fmt.Errorf("broker %q is not available in this build (have: %v); "+
			"rebuild with the matching build tag, e.g. -tags redpanda", name, Available())
	}
	return f(addrs)
}

// Available lists the registered broker names, sorted.
func Available() []string {
	regMu.RLock()
	defer regMu.RUnlock()
	out := make([]string, 0, len(registry))
	for n := range registry {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}
