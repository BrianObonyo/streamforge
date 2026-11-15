//go:build redpanda

package benchmarks

import (
	"context"
	"fmt"
	"testing"

	"github.com/twmb/franz-go/pkg/kfake"

	"github.com/BrianObonyo/streamforge/internal/streaming"
)

// BenchmarkKafkaProduce compares one-record-per-call against one batched call per
// 100 records over the REAL Kafka wire protocol (franz-go client → kfake cluster).
// kfake is in-process on loopback, so absolute numbers are optimistic; the RATIO
// is what matters — it shows the per-message round-trip cost batching removes.
func BenchmarkKafkaProduce(b *testing.B) {
	c, err := kfake.NewCluster(kfake.NumBrokers(1))
	if err != nil {
		b.Fatal(err)
	}
	defer c.Close()
	br, _ := streaming.Open("redpanda", c.ListenAddrs())
	defer br.Close()
	ctx := context.Background()
	_ = br.CreateTopic(ctx, "t", 6)
	p, _ := br.NewProducer(nil)
	val := make([]byte, 180) // roughly one JSON event

	b.Run("sync-per-message", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			_, _, _ = p.Produce(ctx, "t", []byte(fmt.Sprint(i%64)), val)
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
	b.Run("batch-of-100", func(b *testing.B) {
		b.ReportAllocs()
		batch := make([]streaming.KV, 100)
		for i := 0; i < b.N; i += 100 {
			for j := range batch {
				batch[j] = streaming.KV{Key: []byte(fmt.Sprint((i + j) % 64)), Value: val}
			}
			_ = streaming.ProduceBatch(ctx, p, "t", batch)
		}
		b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "msgs/s")
	})
}
