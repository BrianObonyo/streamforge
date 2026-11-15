//go:build redpanda

package integration

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/BrianObonyo/streamforge/internal/ingestion"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

// The same chaos scenario as TestExactAggregatesUnderChaos, but every byte goes
// through the real franz-go client and the Kafka wire protocol (served by kfake)
// instead of the in-memory broker. It exercises the adapter's group join,
// rebalance, fetch, pause/resume and manual-commit paths under a real crash.
func TestKafkaAdapterExactAggregatesUnderChaos(t *testing.T) {
	cluster, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		t.Fatal(err)
	}
	defer cluster.Close()
	b, err := streaming.Open("redpanda", cluster.ListenAddrs())
	if err != nil {
		t.Fatal(err)
	}
	defer b.Close()
	if err := b.CreateTopic(context.Background(), topic, 4); err != nil {
		t.Fatal(err)
	}
	prod, _ := b.NewProducer(partitioning.Hash{})
	pub := ingestion.NewPublisher(prod, topic, reliability.DefaultRetry, nil)

	store := storage.NewMemStore()
	store.FailNext(3)
	dlq := &reliability.MemDLQ{}
	reg := metrics.NewRegistry()
	a := startMember(t, b, "A", store, dlq, reg, nil)
	bm := startMember(t, b, "B", store, dlq, reg, nil)

	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 77, DisorderProb: 0.3, MaxDelay: time.Second, DupProb: 0.08, PoisonProb: 0.03})
	u1, _ := produce(t, g, pub, 1200)
	time.Sleep(400 * time.Millisecond)

	_ = a.consumer.Close() // hard kill A: its tasks are abandoned mid-flight
	select {
	case err := <-a.done:
		if !errors.Is(err, streaming.ErrClosed) {
			t.Fatalf("killed member returned %v, want ErrClosed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("killed member never stopped")
	}
	u2, _ := produce(t, g, pub, 1200)
	unique := u1 + u2

	eventually(t, 60*time.Second, "exact counts through the Kafka adapter", func() (bool, string) {
		got := totalCount(store)
		return got == int64(unique), fmt.Sprintf("stored %d want %d", got, unique)
	})
	time.Sleep(500 * time.Millisecond)
	if got := totalCount(store); got != int64(unique) {
		t.Fatalf("count drifted to %d, want %d", got, unique)
	}
	t.Logf("unique=%d dupDropsB=%d", unique, bm.proc.Stats().Duplicates)
}
