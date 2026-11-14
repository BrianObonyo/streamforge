//go:build redpanda

package streaming

// Redpanda adapter. Redpanda implements the Apache Kafka wire protocol, so this
// file is a thin translation layer from our Broker/Producer/Consumer interfaces
// to franz-go (https://github.com/twmb/franz-go, by Travis Bischel) — a pure-Go,
// high-performance Kafka client. All pipeline logic lives above the interfaces;
// nothing here is Redpanda-specific beyond "talk Kafka to these addresses".
//
// Build with:   go build -tags redpanda ./...
//
// Notes on behaviour that differs from MemBroker:
//
//   - Partitioning: franz-go's default partitioner (sticky for nil keys, murmur2
//     for keyed records) is used so we stay wire-compatible with every other Kafka
//     client producing to the same topic. The `partitioning.Partitioner` argument
//     is therefore ignored here; same-key → same-partition still holds.
//   - Rebalancing: the group protocol is the broker's (cooperative-sticky is the
//     franz-go default). OnPartitionsAssigned/Revoked callbacks feed Assignment().
//   - Offsets: auto-commit is DISABLED. Only the processor's safe offsets are
//     committed, which is the entire basis of its crash-recovery guarantee.

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/kmsg"

	"github.com/BrianObonyo/streamforge/internal/partitioning"
)

type rpBroker struct {
	seeds []string
	admin *kgo.Client
}

func init() {
	Register("redpanda", func(addrs []string) (Broker, error) {
		if len(addrs) == 0 {
			return nil, fmt.Errorf("redpanda: at least one seed broker address is required")
		}
		admin, err := kgo.NewClient(kgo.SeedBrokers(addrs...))
		if err != nil {
			return nil, fmt.Errorf("redpanda: %w", err)
		}
		return &rpBroker{seeds: addrs, admin: admin}, nil
	})
}

func (b *rpBroker) Close() error { b.admin.Close(); return nil }

// CreateTopic is idempotent: "already exists" is success.
func (b *rpBroker) CreateTopic(ctx context.Context, name string, partitions int) error {
	t := kmsg.NewCreateTopicsRequestTopic()
	t.Topic, t.NumPartitions, t.ReplicationFactor = name, int32(partitions), -1 // -1 = broker default
	req := kmsg.NewCreateTopicsRequest()
	req.Topics = append(req.Topics, t)
	resp, err := req.RequestWith(ctx, b.admin)
	if err != nil {
		return fmt.Errorf("create topic %q: %w", name, err)
	}
	for _, rt := range resp.Topics {
		if rt.ErrorCode != 0 && rt.ErrorCode != kerr.TopicAlreadyExists.Code {
			return fmt.Errorf("create topic %q: %w", name, kerr.ErrorForCode(rt.ErrorCode))
		}
	}
	return nil
}

// ---------------------------------------------------------------- producer

type rpProducer struct{ cl *kgo.Client }

func (b *rpBroker) NewProducer(_ partitioning.Partitioner) (Producer, error) {
	cl, err := kgo.NewClient(kgo.SeedBrokers(b.seeds...),
		kgo.RequiredAcks(kgo.AllISRAcks()), // wait for replication: an acked event survives a broker loss
	)
	if err != nil {
		return nil, err
	}
	return &rpProducer{cl: cl}, nil
}

func (p *rpProducer) Produce(ctx context.Context, topic string, key, value []byte) (int, int64, error) {
	r, err := p.cl.ProduceSync(ctx, &kgo.Record{Topic: topic, Key: key, Value: value}).First()
	if err != nil {
		return 0, 0, err
	}
	return int(r.Partition), r.Offset, nil
}

func (p *rpProducer) Close() error { p.cl.Close(); return nil }

// ---------------------------------------------------------------- consumer

type rpConsumer struct {
	cl      *kgo.Client
	mu      sync.Mutex
	owned   map[TopicPartition]struct{}
	changed atomic.Bool // set by rebalance callbacks; consumed by Poll
}

func (b *rpBroker) NewConsumer(group, _ string, topics ...string) (Consumer, error) {
	c := &rpConsumer{owned: map[TopicPartition]struct{}{}}
	cl, err := kgo.NewClient(
		kgo.SeedBrokers(b.seeds...),
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topics...),
		kgo.DisableAutoCommit(),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()), // brand-new group reads from the beginning
		kgo.OnPartitionsAssigned(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { c.update(m, true) }),
		kgo.OnPartitionsRevoked(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { c.update(m, false) }),
		kgo.OnPartitionsLost(func(_ context.Context, _ *kgo.Client, m map[string][]int32) { c.update(m, false) }),
	)
	if err != nil {
		return nil, err
	}
	c.cl = cl
	return c, nil
}

func (c *rpConsumer) update(m map[string][]int32, add bool) {
	c.mu.Lock()
	for t, ps := range m {
		for _, p := range ps {
			tp := TopicPartition{t, int(p)}
			if add {
				c.owned[tp] = struct{}{}
			} else {
				delete(c.owned, tp)
			}
		}
	}
	c.mu.Unlock()
	c.changed.Store(true)
}

func (c *rpConsumer) Poll(ctx context.Context, max int) ([]Record, error) {
	f := c.cl.PollRecords(ctx, max)
	if f.IsClientClosed() {
		return nil, ErrClosed
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	var out []Record
	f.EachRecord(func(r *kgo.Record) {
		out = append(out, Record{Topic: r.Topic, Partition: int(r.Partition), Offset: r.Offset,
			Key: r.Key, Value: r.Value, Timestamp: r.Timestamp})
	})
	if len(out) == 0 {
		if errs := f.Errors(); len(errs) > 0 {
			return nil, fmt.Errorf("fetch %s/%d: %w", errs[0].Topic, errs[0].Partition, errs[0].Err)
		}
		// Surface a rebalance as an empty, error-free poll (the Consumer contract).
		c.changed.Swap(false)
	}
	return out, nil
}

func (c *rpConsumer) Commit(ctx context.Context, offs map[TopicPartition]int64) error {
	m := map[string]map[int32]kgo.EpochOffset{}
	for tp, o := range offs {
		if m[tp.Topic] == nil {
			m[tp.Topic] = map[int32]kgo.EpochOffset{}
		}
		m[tp.Topic][int32(tp.Partition)] = kgo.EpochOffset{Epoch: -1, Offset: o} // -1: don't epoch-fence
	}
	var commitErr error
	c.cl.CommitOffsetsSync(ctx, m, func(_ *kgo.Client, _ *kmsg.OffsetCommitRequest, resp *kmsg.OffsetCommitResponse, err error) {
		if err != nil {
			commitErr = err
			return
		}
		for _, t := range resp.Topics {
			for _, p := range t.Partitions {
				if e := kerr.ErrorForCode(p.ErrorCode); e != nil && commitErr == nil {
					commitErr = e
				}
			}
		}
	})
	if errors.Is(commitErr, kerr.UnknownMemberID) || errors.Is(commitErr, kerr.RebalanceInProgress) || errors.Is(commitErr, kerr.IllegalGeneration) {
		return fmt.Errorf("%w: %v", ErrNotAssigned, commitErr) // fenced: a rebalance raced us
	}
	return commitErr
}

func (c *rpConsumer) Assignment() []TopicPartition {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]TopicPartition, 0, len(c.owned))
	for tp := range c.owned {
		out = append(out, tp)
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].Topic != out[j].Topic {
			return out[i].Topic < out[j].Topic
		}
		return out[i].Partition < out[j].Partition
	})
	return out
}

func toMap(tps []TopicPartition) map[string][]int32 {
	m := map[string][]int32{}
	for _, tp := range tps {
		m[tp.Topic] = append(m[tp.Topic], int32(tp.Partition))
	}
	return m
}

func (c *rpConsumer) Pause(tps ...TopicPartition)  { c.cl.PauseFetchPartitions(toMap(tps)) }
func (c *rpConsumer) Resume(tps ...TopicPartition) { c.cl.ResumeFetchPartitions(toMap(tps)) }
func (c *rpConsumer) Close() error                 { c.cl.Close(); return nil }
