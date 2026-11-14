// Package integration exercises the whole pipeline — generator → broker →
// processors → store — under failure, and checks the one property that matters:
// the stored aggregates are EXACTLY right.
package integration

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/ingestion"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/processor"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

const topic = "events"

type member struct {
	proc     *processor.Processor
	consumer streaming.Consumer
	cancel   context.CancelFunc
	done     chan error
}

func startMember(t *testing.T, b *streaming.MemBroker, id string, sink storage.Sink, dlq reliability.DLQ,
	reg *metrics.Registry, mod func(*processor.Config)) *member {
	t.Helper()
	c, err := b.NewConsumer("g", id, topic)
	if err != nil {
		t.Fatal(err)
	}
	cfg := processor.Config{
		Topic: topic, Group: "g", MemberID: id,
		WindowSize: time.Second, MaxOutOfOrder: 1500 * time.Millisecond, AllowedLateness: 5 * time.Second,
		IdleTimeout: 500 * time.Millisecond, TickInterval: 30 * time.Millisecond, CommitInterval: 30 * time.Millisecond,
		Retry:            reliability.RetryPolicy{MaxAttempts: 3, Base: time.Millisecond, Max: 5 * time.Millisecond},
		BreakerThreshold: 100, // keep the breaker out of the way here; it has its own tests
	}
	if mod != nil {
		mod(&cfg)
	}
	p, err := processor.New(cfg, processor.Deps{Consumer: c, Sink: sink, DLQ: dlq, Metrics: reg})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	m := &member{proc: p, consumer: c, cancel: cancel, done: make(chan error, 1)}
	go func() { m.done <- p.Run(ctx) }()
	t.Cleanup(cancel)
	return m
}

func setup(t *testing.T, parts int) (*streaming.MemBroker, *ingestion.Publisher) {
	t.Helper()
	b := streaming.NewMemBroker()
	if err := b.CreateTopic(context.Background(), topic, parts); err != nil {
		t.Fatal(err)
	}
	prod, _ := b.NewProducer(partitioning.Hash{})
	return b, ingestion.NewPublisher(prod, topic, reliability.DefaultRetry, nil)
}

// produce publishes n generated messages and returns how many UNIQUE valid events
// were among them — the number the pipeline must account for exactly.
func produce(t *testing.T, g *ingestion.Generator, pub *ingestion.Publisher, n int) (unique, poison int) {
	t.Helper()
	for i := 0; i < n; i++ {
		m := g.Next()
		if err := pub.PublishRaw(context.Background(), m.Key, m.Value); err != nil {
			t.Fatal(err)
		}
		switch {
		case m.Poison:
			poison++
		case !m.Dup:
			unique++
		}
	}
	return
}

func totalCount(s *storage.MemStore) (n int64) {
	for _, r := range s.Rows() {
		n += r.Stats.Count
	}
	return
}

func eventually(t *testing.T, d time.Duration, what string, cond func() (bool, string)) {
	t.Helper()
	deadline := time.Now().Add(d)
	var last string
	for time.Now().Before(deadline) {
		ok, msg := cond()
		if ok {
			return
		}
		last = msg
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s: %s", what, last)
}

// THE test: chaos in, exact answers out.
//
//   - 30% of events arrive out of order, 8% are duplicated, 3% are garbage
//   - the sink fails its first several writes
//   - one of two group members is hard-killed mid-stream (no flush, no commit)
//
// Afterwards the sum of stored counts must equal the number of unique events.
func TestExactAggregatesUnderChaos(t *testing.T) {
	b, pub := setup(t, 4)
	store := storage.NewMemStore()
	store.FailNext(5) // sink outage at startup
	dlq := &reliability.MemDLQ{}
	reg := metrics.NewRegistry()

	a := startMember(t, b, "A", store, dlq, reg, nil)
	bm := startMember(t, b, "B", store, dlq, reg, nil)
	_ = bm

	g := ingestion.NewGenerator(ingestion.GenConfig{
		Seed: 2026, DisorderProb: 0.3, MaxDelay: time.Second, DupProb: 0.08, PoisonProb: 0.03,
	})

	u1, p1 := produce(t, g, pub, 2000)
	time.Sleep(150 * time.Millisecond) // let A make real progress and commit some offsets

	// Hard kill: closing the consumer makes Run return ErrClosed. Window state is
	// abandoned exactly as if the process had been SIGKILLed.
	_ = a.consumer.Close()
	select {
	case err := <-a.done:
		if !errors.Is(err, streaming.ErrClosed) {
			t.Fatalf("killed member returned %v, want ErrClosed", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("killed member never stopped")
	}

	u2, p2 := produce(t, g, pub, 2000) // B must now serve all four partitions
	unique, poison := u1+u2, p1+p2

	eventually(t, 20*time.Second, "all unique events to be aggregated exactly once", func() (bool, string) {
		got := totalCount(store)
		return got == int64(unique), fmt.Sprintf("stored %d, want %d", got, unique)
	})
	// Stay put briefly and re-check: nothing may drift after reaching the target
	// (e.g. a late duplicate flush double-counting).
	time.Sleep(300 * time.Millisecond)
	if got := totalCount(store); got != int64(unique) {
		t.Fatalf("count drifted to %d after settling, want %d", got, unique)
	}

	if got := dlq.CountByReason()[reliability.ReasonPoison]; got < poison {
		t.Errorf("DLQ holds %d poison messages, want at least %d (replays may add more)", got, poison)
	}
	if lag := b.Lag("g", topic); sumLag(lag) < 0 {
		t.Errorf("negative lag: %v", lag)
	}
	if st := bm.proc.Stats(); st.Duplicates == 0 || st.Poison == 0 {
		t.Errorf("fault injection never reached B: %+v", st)
	}
	t.Logf("unique=%d poison=%d dupDropsB=%d lateB=%d", unique, poison, bm.proc.Stats().Duplicates, bm.proc.Stats().Late)
}

func sumLag(m map[streaming.TopicPartition]int64) (n int64) {
	for _, v := range m {
		n += v
	}
	return
}

// slowSink makes every write take long enough that the processor must apply
// backpressure rather than buffer without bound.
type slowSink struct {
	storage.Sink
	delay time.Duration
}

func (s slowSink) WriteResults(ctx context.Context, rs []aggregation.Result) error {
	time.Sleep(s.delay)
	return s.Sink.WriteResults(ctx, rs)
}

func TestBackpressureEngagesAndNothingIsLost(t *testing.T) {
	b, pub := setup(t, 1)
	store := storage.NewMemStore()
	reg := metrics.NewRegistry()
	m := startMember(t, b, "solo", slowSink{store, 40 * time.Millisecond}, nil, reg, func(c *processor.Config) {
		c.QueueCapacity, c.HighWater, c.LowWater = 32, 24, 8
		c.TickInterval = 20 * time.Millisecond
	})
	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 5, DisorderProb: 0.2, MaxDelay: 800 * time.Millisecond})
	unique, _ := produce(t, g, pub, 3000)

	var maxDepth float64
	eventually(t, 30*time.Second, "slow pipeline to finish", func() (bool, string) {
		var sb strings.Builder
		_, _ = reg.WriteTo(&sb)
		for _, line := range strings.Split(sb.String(), "\n") {
			if strings.HasPrefix(line, "sf_queue_depth{") {
				var v float64
				fmt.Sscanf(line[strings.LastIndex(line, " ")+1:], "%g", &v)
				maxDepth = max(maxDepth, v)
			}
		}
		got := totalCount(store)
		return got == int64(unique), fmt.Sprintf("stored %d want %d", got, unique)
	})
	if maxDepth > 32 {
		t.Errorf("queue depth reached %v, exceeding its bound of 32", maxDepth)
	}
	if m.proc.Pauses() == 0 {
		t.Error("backpressure never engaged despite a slow sink and tiny queue")
	}
	t.Logf("pauses=%d maxDepth=%v", m.proc.Pauses(), maxDepth)
}

// With periodic commits disabled, ONLY the shutdown path can move offsets. Once
// every window has expired (so nothing needs replaying), a graceful stop must
// commit exactly to the end of each log — and a restart must then do nothing.
func TestGracefulShutdownCommitsToLogEnd(t *testing.T) {
	b, pub := setup(t, 2)
	store := storage.NewMemStore()
	m := startMember(t, b, "solo", store, nil, nil, func(c *processor.Config) {
		c.CommitInterval = time.Hour // periodic commit effectively off
		c.MaxOutOfOrder, c.AllowedLateness, c.IdleTimeout = 200*time.Millisecond, 200*time.Millisecond, 200*time.Millisecond
	})
	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 9})
	unique, _ := produce(t, g, pub, 500)

	eventually(t, 10*time.Second, "events to be aggregated", func() (bool, string) {
		return totalCount(store) == int64(unique), fmt.Sprintf("%d/%d", totalCount(store), unique)
	})
	time.Sleep(time.Second) // let the lateness grace period expire and windows be GC'd

	if c := b.Committed("g"); len(c) != 0 {
		t.Fatalf("offsets committed before shutdown despite CommitInterval=1h: %v", c)
	}
	m.cancel()
	select {
	case err := <-m.done:
		if err != nil {
			t.Fatalf("graceful stop returned %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("graceful stop hung")
	}
	for part, end := range b.EndOffsets(topic) {
		if end == 0 {
			continue
		}
		if got := b.Committed("g")[streaming.TopicPartition{Topic: topic, Partition: part}]; got != end {
			t.Errorf("partition %d committed %d, want log end %d", part, got, end)
		}
	}

	// Restart: nothing left to read, totals unchanged.
	startMember(t, b, "solo2", store, nil, nil, nil)
	time.Sleep(500 * time.Millisecond)
	if got := totalCount(store); got != int64(unique) {
		t.Fatalf("restart changed totals: %d want %d", got, unique)
	}
}

var _ = sync.Mutex{}
