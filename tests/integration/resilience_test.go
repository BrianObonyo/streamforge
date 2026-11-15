package integration

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/ingestion"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/processor"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/tracing"
)

// flakySink fails with a PERMANENT error for its first `permFails` WriteResults
// calls (a rejected schema, say), then behaves.
type flakySink struct {
	storage.Sink
	permFails atomic.Int32
	calls     atomic.Int32
}

func (f *flakySink) WriteResults(ctx context.Context, rs []aggregation.Result) error {
	f.calls.Add(1)
	if f.permFails.Add(-1) >= 0 {
		return reliability.Permanent(errors.New("clickhouse: HTTP 404 (code 60): table missing"))
	}
	return f.Sink.WriteResults(ctx, rs)
}

// A permanent error that an operator fixes in time must lose NOTHING: the batch
// stays pending and is written once the sink recovers.
func TestPermanentSinkErrorRecoversWithoutLoss(t *testing.T) {
	b, pub := setup(t, 2)
	store := storage.NewMemStore()
	sink := &flakySink{Sink: store}
	sink.permFails.Store(3) // fails 3 times, limit is 10 ⇒ must recover
	dlq := &reliability.MemDLQ{}
	startMember(t, b, "m", sink, dlq, nil, func(c *processor.Config) { c.MaxPermanentSinkFailures = 10 })

	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 3})
	unique, _ := produce(t, g, pub, 800)
	eventually(t, 20*time.Second, "recovery after permanent errors", func() (bool, string) {
		return totalCount(store) == int64(unique), fmt.Sprintf("%d/%d", totalCount(store), unique)
	})
	if n := dlq.CountByReason()[reliability.ReasonSinkFail]; n != 0 {
		t.Fatalf("%d rows dead-lettered although the sink recovered in time", n)
	}
}

// If the failure never clears, the partition must not wedge forever: after the
// limit, rows are PRESERVED in the DLQ and processing (and commits) move on.
func TestPersistentPermanentSinkErrorIsDeadLettered(t *testing.T) {
	b, pub := setup(t, 1)
	sink := &flakySink{Sink: storage.NewMemStore()}
	sink.permFails.Store(1 << 30) // never recovers
	dlq := &reliability.MemDLQ{}
	reg := metrics.NewRegistry()
	startMember(t, b, "m", sink, dlq, reg, func(c *processor.Config) {
		c.MaxPermanentSinkFailures = 3
		c.MaxOutOfOrder, c.AllowedLateness, c.IdleTimeout = 100*time.Millisecond, 100*time.Millisecond, 100*time.Millisecond
	})
	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 4})
	produce(t, g, pub, 300)

	eventually(t, 20*time.Second, "rows to be dead-lettered", func() (bool, string) {
		n := dlq.CountByReason()[reliability.ReasonSinkFail]
		return n > 0, fmt.Sprintf("dead-lettered=%d", n)
	})
	// Dead letters must carry a decodable result payload so rows are recoverable.
	d := dlq.Items()[0]
	var r aggregation.Result
	if d.Reason != reliability.ReasonSinkFail || json.Unmarshal(d.Value, &r) != nil || r.Stats.Count == 0 {
		t.Fatalf("dead letter unusable: %+v", d)
	}
	// And the partition is unblocked: offsets advance past the failed windows.
	eventually(t, 15*time.Second, "offsets to advance after dead-lettering", func() (bool, string) {
		return sumOf(b.Committed("g")) > 0, "no commit yet"
	})
}

func sumOf[K comparable](m map[K]int64) (n int64) {
	for _, v := range m {
		n += v
	}
	return
}

func TestBinaryCodecEndToEnd(t *testing.T) {
	b, pub := setup(t, 3)
	pub.Codec = events.CodecBinary
	store := storage.NewMemStore()
	dlq := &reliability.MemDLQ{}
	startMember(t, b, "m", store, dlq, nil, nil)
	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 6, Codec: events.CodecBinary, DisorderProb: .2, MaxDelay: 800 * time.Millisecond, DupProb: .05, PoisonProb: .02})
	unique, poison := produce(t, g, pub, 1500)
	eventually(t, 20*time.Second, "exact counts via binary codec", func() (bool, string) {
		return totalCount(store) == int64(unique), fmt.Sprintf("%d/%d", totalCount(store), unique)
	})
	if got := dlq.CountByReason()[reliability.ReasonPoison]; got < poison {
		t.Errorf("poison dead-lettered=%d want >=%d", got, poison)
	}
}

// One trace must follow an event across the whole pipeline:
// publish (PRODUCER) ─▶ process (CONSUMER, child of publish) — and sink.write too.
func TestTraceFollowsEventThroughPipeline(t *testing.T) {
	var mu sync.Mutex
	var spans []map[string]any
	col := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		var doc struct {
			ResourceSpans []struct {
				ScopeSpans []struct{ Spans []map[string]any } `json:"scopeSpans"`
			} `json:"resourceSpans"`
		}
		if json.Unmarshal(body, &doc) == nil {
			mu.Lock()
			for _, rs := range doc.ResourceSpans {
				for _, ss := range rs.ScopeSpans {
					spans = append(spans, ss.Spans...)
				}
			}
			mu.Unlock()
		}
	}))
	defer col.Close()
	tr := tracing.New(tracing.Config{Endpoint: col.URL, SampleRatio: 1, FlushInterval: 30 * time.Millisecond})

	b, pub := setup(t, 2)
	pub.Tracer = tr
	store := storage.NewMemStore()
	startMember(t, b, "m", store, nil, nil, func(c *processor.Config) { c.Tracer = tr })

	g := ingestion.NewGenerator(ingestion.GenConfig{Seed: 8, Tracer: tr})
	unique, _ := produce(t, g, pub, 120)
	eventually(t, 15*time.Second, "events aggregated", func() (bool, string) {
		return totalCount(store) == int64(unique), "waiting"
	})
	time.Sleep(200 * time.Millisecond)
	_ = tr.Shutdown(context.Background())

	mu.Lock()
	defer mu.Unlock()
	byID := map[string]map[string]any{}
	for _, s := range spans {
		byID[s["spanId"].(string)] = s
	}
	linked, sink := 0, 0
	for _, s := range spans {
		switch s["name"] {
		case "process events":
			parent, ok := byID[fmt.Sprint(s["parentSpanId"])]
			if !ok || parent["name"] != "publish" || parent["traceId"] != s["traceId"] {
				t.Fatalf("process span not a child of a publish span in the same trace: %v / %v", s, parent)
			}
			if s["kind"].(float64) != float64(tracing.KindConsumer) || parent["kind"].(float64) != float64(tracing.KindProducer) {
				t.Fatalf("wrong span kinds: %v %v", s["kind"], parent["kind"])
			}
			linked++
		case "sink.write":
			sink++
		}
	}
	if linked < 100 || sink == 0 {
		t.Fatalf("publish→process links=%d (want ≥100 of 120 unique), sink spans=%d", linked, sink)
	}
	t.Logf("%d spans exported; %d publish→process links; %d sink.write", len(spans), linked, sink)
}
