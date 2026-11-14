package streaming

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/partitioning"
)

func newBroker(t *testing.T, parts int) (*MemBroker, Producer) {
	t.Helper()
	b := NewMemBroker()
	if err := b.CreateTopic(context.Background(), "t", parts); err != nil {
		t.Fatal(err)
	}
	p, _ := b.NewProducer(partitioning.Hash{})
	return b, p
}

func produceN(t *testing.T, p Producer, n int) {
	t.Helper()
	for i := 0; i < n; i++ {
		if _, _, err := p.Produce(context.Background(), "t", []byte(fmt.Sprintf("k%d", i)), []byte("v")); err != nil {
			t.Fatal(err)
		}
	}
}

func drain(t *testing.T, c Consumer) []Record {
	t.Helper()
	var all []Record
	for {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
		recs, err := c.Poll(ctx, 100)
		cancel()
		if err != nil {
			return all
		}
		all = append(all, recs...)
	}
}

func TestSameKeySamePartitionAndOrdered(t *testing.T) {
	b, p := newBroker(t, 4)
	_ = b
	var firstPart = -1
	for i := 0; i < 20; i++ {
		part, off, _ := p.Produce(context.Background(), "t", []byte("sensor-7"), []byte("v"))
		if firstPart == -1 {
			firstPart = part
		}
		if part != firstPart || off != int64(i) {
			t.Fatalf("i=%d part=%d off=%d (want part %d off %d)", i, part, off, firstPart, i)
		}
	}
}

func TestSingleConsumerReadsEverything(t *testing.T) {
	b, p := newBroker(t, 3)
	produceN(t, p, 30)
	c, _ := b.NewConsumer("g", "m1", "t")
	if got := len(drain(t, c)); got != 30 {
		t.Fatalf("read %d of 30", got)
	}
}

func TestRebalanceSplitsPartitions(t *testing.T) {
	b, _ := newBroker(t, 4)
	c1, _ := b.NewConsumer("g", "a", "t")
	if n := len(c1.Assignment()); n != 4 {
		t.Fatalf("lone member owns %d, want 4", n)
	}
	c2, _ := b.NewConsumer("g", "b", "t")
	a1, a2 := c1.Assignment(), c2.Assignment()
	if len(a1) != 2 || len(a2) != 2 {
		t.Fatalf("split %d/%d, want 2/2", len(a1), len(a2))
	}
	seen := map[TopicPartition]bool{}
	for _, tp := range append(a1, a2...) {
		if seen[tp] {
			t.Fatalf("%s assigned twice", tp)
		}
		seen[tp] = true
	}
	// c2 leaves → c1 takes everything back.
	_ = c2.Close()
	if n := len(c1.Assignment()); n != 4 {
		t.Fatalf("after leave c1 owns %d, want 4", n)
	}
}

func TestPollSignalsRebalance(t *testing.T) {
	b, _ := newBroker(t, 2)
	c1, _ := b.NewConsumer("g", "a", "t")
	_, _ = c1.Poll(context.Background(), 1) // consume the initial-generation signal
	_, _ = b.NewConsumer("g", "b", "t")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	recs, err := c1.Poll(ctx, 1)
	if err != nil || len(recs) != 0 {
		t.Fatalf("want prompt empty poll after rebalance, got %d recs err=%v", len(recs), err)
	}
}

// The recovery path: a new owner resumes from the COMMITTED offset, so
// uncommitted work is re-delivered (at-least-once) and committed work is not.
func TestCommitAndResumeAfterCrash(t *testing.T) {
	b, p := newBroker(t, 1)
	produceN(t, p, 10)
	c1, _ := b.NewConsumer("g", "a", "t")
	recs := drain(t, c1)
	if len(recs) != 10 {
		t.Fatalf("got %d", len(recs))
	}
	tp := TopicPartition{"t", 0}
	if err := c1.Commit(context.Background(), map[TopicPartition]int64{tp: 6}); err != nil {
		t.Fatal(err)
	}
	_ = c1.Close() // "crash" after committing only 6 of 10

	c2, _ := b.NewConsumer("g", "b", "t")
	again := drain(t, c2)
	if len(again) != 4 || again[0].Offset != 6 {
		t.Fatalf("resumed with %d records starting at %d, want 4 starting at 6", len(again), again[0].Offset)
	}
	if lag := b.Lag("g", "t")[tp]; lag != 4 {
		t.Fatalf("lag=%d want 4", lag)
	}
}

func TestCommitFencedForUnownedPartition(t *testing.T) {
	b, _ := newBroker(t, 2)
	c1, _ := b.NewConsumer("g", "a", "t")
	_, _ = b.NewConsumer("g", "b", "t")
	other := TopicPartition{"t", 1} // owned by "b"
	err := c1.Commit(context.Background(), map[TopicPartition]int64{other: 5})
	if !errors.Is(err, ErrNotAssigned) {
		t.Fatalf("zombie commit allowed: %v", err)
	}
}

func TestCommitNeverMovesBackwards(t *testing.T) {
	b, p := newBroker(t, 1)
	produceN(t, p, 5)
	c, _ := b.NewConsumer("g", "a", "t")
	tp := TopicPartition{"t", 0}
	_ = c.Commit(context.Background(), map[TopicPartition]int64{tp: 4})
	_ = c.Commit(context.Background(), map[TopicPartition]int64{tp: 2})
	if got := b.Committed("g")[tp]; got != 4 {
		t.Fatalf("committed=%d want 4", got)
	}
}

func TestPauseResume(t *testing.T) {
	b, p := newBroker(t, 1)
	produceN(t, p, 5)
	c, _ := b.NewConsumer("g", "a", "t")
	tp := TopicPartition{"t", 0}
	_, _ = c.Poll(context.Background(), 1) // swallow generation signal
	c.Pause(tp)
	ctx, cancel := context.WithTimeout(context.Background(), 40*time.Millisecond)
	if recs, _ := c.Poll(ctx, 10); len(recs) != 0 {
		t.Fatal("paused partition delivered records")
	}
	cancel()
	c.Resume(tp)
	ctx, cancel = context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if recs, err := c.Poll(ctx, 10); err != nil || len(recs) != 5 {
		t.Fatalf("after resume: %d recs err=%v", len(recs), err)
	}
}

func TestBlockedPollWakesOnProduce(t *testing.T) {
	b, p := newBroker(t, 1)
	c, _ := b.NewConsumer("g", "a", "t")
	_, _ = c.Poll(context.Background(), 1)
	go func() { time.Sleep(30 * time.Millisecond); produceN(t, p, 1) }()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	recs, err := c.Poll(ctx, 1)
	if err != nil || len(recs) != 1 {
		t.Fatalf("recs=%d err=%v", len(recs), err)
	}
}

func TestProduceFaultInjection(t *testing.T) {
	b, p := newBroker(t, 1)
	b.InjectProduceFault(func(string) error { return errors.New("leader not available") })
	if _, _, err := p.Produce(context.Background(), "t", []byte("k"), nil); err == nil {
		t.Fatal("fault not injected")
	}
	b.InjectProduceFault(nil)
	if _, _, err := p.Produce(context.Background(), "t", []byte("k"), nil); err != nil {
		t.Fatal(err)
	}
}

func TestUnknownTopic(t *testing.T) {
	b := NewMemBroker()
	if _, err := b.NewConsumer("g", "a", "nope"); !errors.Is(err, ErrUnknownTopic) {
		t.Fatalf("got %v", err)
	}
}
