// Package benchmarks holds micro- and pipeline-level benchmarks.
//
//	go test -bench . -benchmem -run '^$' ./benchmarks/
//
// Results are hardware-specific: see benchmarks/README.md for how to record them
// reproducibly (CPU, Go version, GOMAXPROCS, commit) before quoting any number.
package benchmarks

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/ingestion"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/processor"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

var base = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func BenchmarkAggregatorAdd(b *testing.B) {
	a := aggregation.New(aggregation.Config{Assigner: aggregation.Tumbling{Size: time.Second},
		MaxOutOfOrder: 2 * time.Second, AllowedLateness: 5 * time.Second})
	keys := make([]string, 256)
	for i := range keys {
		keys[i] = fmt.Sprintf("sensor-%03d", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		ev := events.Event{Kind: events.KindIoT, Key: keys[i&255], Value: float64(i & 63),
			EventTime: base.Add(time.Duration(i) * time.Millisecond)}
		a.Add(ev, int64(i), base)
		if i&8191 == 8191 { // emit + GC periodically, as the real task does on each tick
			a.Ack(a.Collect(base.Add(time.Duration(i)*time.Millisecond), 0))
		}
	}
}

func BenchmarkSlidingAssign(b *testing.B) {
	a := aggregation.Sliding{Size: time.Minute, Slide: 10 * time.Second}
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_ = a.Assign(base.Add(time.Duration(i) * time.Millisecond))
	}
}

func BenchmarkDedup(b *testing.B) {
	d := reliability.NewDeduper(time.Minute, 100_000)
	ids := make([]string, 200_000)
	for i := range ids {
		ids[i] = fmt.Sprintf("%032x", i)
	}
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		d.Seen(ids[i%len(ids)], base)
	}
}

func BenchmarkPartitioners(b *testing.B) {
	key := []byte("sensor-042")
	for _, p := range []partitioning.Partitioner{partitioning.Hash{}, partitioning.Jump{}} {
		b.Run(p.Name(), func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				_ = p.Partition(key, 64)
			}
		})
	}
}

func BenchmarkEventDecodeValidate(b *testing.B) {
	raw, _ := events.Encode(events.Event{ID: "abc", Kind: events.KindMarket, Key: "BTC-USD", Value: 64000.5, EventTime: base})
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		ev, _ := events.Decode(raw)
		_ = ev.Validate(base)
	}
}

// BenchmarkPipeline measures END-TO-END throughput in-process: pre-generated
// messages → MemBroker → processors → MemStore. The broker and store are
// in-memory, so this isolates the engine's own cost; it is NOT a Redpanda or
// ClickHouse number.
func BenchmarkPipeline(b *testing.B) {
	for _, parts := range []int{1, 4} {
		b.Run(fmt.Sprintf("partitions=%d", parts), func(b *testing.B) {
			ctx := context.Background()
			br := streaming.NewMemBroker()
			_ = br.CreateTopic(ctx, "t", parts)
			prod, _ := br.NewProducer(partitioning.Hash{})
			g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 1, Now: func() time.Time { return time.Now() }})
			msgs := make([]ingestion.Message, b.N)
			for i := range msgs {
				msgs[i] = g.Next()
			}
			cons, _ := br.NewConsumer("g", "m", "t")
			store := storage.NewMemStore()
			p, err := processor.New(processor.Config{Topic: "t", Group: "g", MemberID: "m", WindowSize: time.Second,
				MaxOutOfOrder: time.Second, AllowedLateness: time.Second, IdleTimeout: 200 * time.Millisecond},
				processor.Deps{Consumer: cons, Sink: store})
			if err != nil {
				b.Fatal(err)
			}
			rctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- p.Run(rctx) }()

			b.ResetTimer()
			for _, m := range msgs {
				_, _, _ = prod.Produce(ctx, "t", m.Key, m.Value)
			}
			for { // wait until every message has been handled
				s := p.Stats()
				if int(s.OnTime+s.Late+s.TooLate+s.Duplicates+s.Invalid+s.Poison) >= b.N {
					break
				}
				time.Sleep(time.Millisecond)
			}
			b.StopTimer()
			cancel()
			<-done
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
		})
	}
}

// ---- codec comparison (see ADR-0005) --------------------------------------

func sampleEvent() events.Event {
	return events.Event{ID: "9f2c1d7e8a4b4c0d9e1f2a3b4c5d6e7f", Kind: events.KindMarket, Key: "BTC-USD", Value: 64123.5,
		EventTime: base, IngestTime: base.Add(3 * time.Millisecond), Attrs: map[string]string{"currency": "USD"}}
}

func BenchmarkDecodeJSON(b *testing.B) {
	raw, _ := events.Encode(sampleEvent())
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		_, _ = events.DecodeAny(raw)
	}
}

func BenchmarkDecodeBinary(b *testing.B) {
	raw, _ := events.EncodeBinary(sampleEvent())
	b.ReportAllocs()
	b.SetBytes(int64(len(raw)))
	for i := 0; i < b.N; i++ {
		_, _ = events.DecodeAny(raw)
	}
}

func BenchmarkEncodeJSON(b *testing.B) {
	e := sampleEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = events.Encode(e)
	}
}

func BenchmarkEncodeBinary(b *testing.B) {
	e := sampleEvent()
	b.ReportAllocs()
	for i := 0; i < b.N; i++ {
		_, _ = events.EncodeBinary(e)
	}
}

// BenchmarkPipelineCodec: same engine-only pipeline as BenchmarkPipeline, per codec.
func BenchmarkPipelineCodec(b *testing.B) {
	for _, codec := range []events.Codec{events.CodecJSON, events.CodecBinary} {
		b.Run(string(codec), func(b *testing.B) {
			ctx := context.Background()
			br := streaming.NewMemBroker()
			_ = br.CreateTopic(ctx, "t", 1)
			prod, _ := br.NewProducer(partitioning.Hash{})
			g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 1, Codec: codec})
			kvs := make([]streaming.KV, b.N)
			for i := range kvs {
				m := g.Next()
				kvs[i] = streaming.KV{Key: m.Key, Value: m.Value}
			}
			cons, _ := br.NewConsumer("g", "m", "t")
			p, err := processor.New(processor.Config{Topic: "t", Group: "g", MemberID: "m", WindowSize: time.Second,
				MaxOutOfOrder: time.Second, AllowedLateness: time.Second, IdleTimeout: 200 * time.Millisecond},
				processor.Deps{Consumer: cons, Sink: storage.NewMemStore()})
			if err != nil {
				b.Fatal(err)
			}
			rctx, cancel := context.WithCancel(ctx)
			done := make(chan error, 1)
			go func() { done <- p.Run(rctx) }()
			b.ResetTimer()
			_ = streaming.ProduceBatch(ctx, prod, "t", kvs)
			for {
				s := p.Stats()
				if int(s.OnTime+s.Late+s.TooLate+s.Duplicates+s.Invalid+s.Poison) >= b.N {
					break
				}
				time.Sleep(time.Millisecond)
			}
			b.StopTimer()
			cancel()
			<-done
			b.ReportMetric(float64(b.N)/b.Elapsed().Seconds(), "events/s")
		})
	}
}
