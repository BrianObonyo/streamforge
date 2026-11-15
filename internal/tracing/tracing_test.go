package tracing

import (
	"context"
	"encoding/json"
	"errors"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestTraceparentRoundTripAndValidation(t *testing.T) {
	const good = "00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01"
	c, ok := ParseTraceparent(good)
	if !ok || !c.Sampled || c.String() != good {
		t.Fatalf("%+v ok=%v str=%s", c, ok, c.String())
	}
	if c, ok := ParseTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-00"); !ok || c.Sampled {
		t.Fatal("flag 00 must parse as unsampled")
	}
	for _, bad := range []string{
		"", "garbage", good[:54], good + "0",
		"01-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", // unsupported version
		"00-00000000000000000000000000000000-b7ad6b7169203331-01", // all-zero trace id is invalid
		"00-0af7651916cd43dd8448eb211c80319c-0000000000000000-01", // all-zero span id is invalid
		"00-0af7651916cd43dd8448eb211c80319z-b7ad6b7169203331-01", // non-hex
		"00_0af7651916cd43dd8448eb211c80319c_b7ad6b7169203331_01",
	} {
		if _, ok := ParseTraceparent(bad); ok {
			t.Errorf("accepted %q", bad)
		}
	}
}

func TestNilTracerAndSpanAreNoOps(t *testing.T) {
	var tr *Tracer
	s := tr.StartRoot("x", KindInternal)
	s.SetAttr("a", 1)
	s.RecordError(errors.New("e"))
	s.End()
	if tr.StartFromTraceparent("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01", "y", KindConsumer) != nil {
		t.Fatal("nil tracer produced a span")
	}
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}
	if New(Config{}) != nil {
		t.Fatal("empty endpoint must disable tracing")
	}
}

func TestSamplingIsInheritedNotRedecided(t *testing.T) {
	tr := New(Config{Endpoint: "http://127.0.0.1:1", SampleRatio: 1})
	defer tr.Shutdown(context.Background())
	root := tr.StartRoot("r", KindProducer)
	child := tr.StartChild(root.Context(), "c", KindConsumer)
	if child == nil || child.Context().TraceID != root.Context().TraceID || child.parent != root.Context().SpanID {
		t.Fatal("child must share trace and reference parent")
	}
	unsampled := root.Context()
	unsampled.Sampled = false
	if tr.StartChild(unsampled, "c", KindConsumer) != nil {
		t.Fatal("unsampled parent must not create a child")
	}
	zero := New(Config{Endpoint: "http://127.0.0.1:1", SampleRatio: 0})
	defer zero.Shutdown(context.Background())
	if zero.StartRoot("r", KindInternal) != nil {
		t.Fatal("ratio 0 must never sample")
	}
}

// Captures what a real OTLP collector would receive.
type collector struct {
	mu     sync.Mutex
	bodies [][]byte
	status atomic.Int32
	hits   atomic.Int32
}

func (c *collector) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	c.hits.Add(1)
	b, _ := io.ReadAll(r.Body)
	if st := int(c.status.Load()); st != 0 && st != 200 {
		w.WriteHeader(st)
		return
	}
	c.mu.Lock()
	c.bodies = append(c.bodies, b)
	c.mu.Unlock()
	if r.URL.Path != "/v1/traces" || r.Header.Get("Content-Type") != "application/json" {
		w.WriteHeader(http.StatusBadRequest)
	}
}

func TestExportOTLPJSONShapeAndParentage(t *testing.T) {
	col := &collector{}
	srv := httptest.NewServer(col)
	defer srv.Close()
	tr := New(Config{Endpoint: srv.URL, ServiceName: "svc-test", SampleRatio: 1, FlushInterval: 20 * time.Millisecond})

	root := tr.StartRoot("publish", KindProducer)
	root.SetAttr("messaging.destination", "events")
	root.SetAttr("count", 3)
	root.SetAttr("ratio", 0.5)
	root.SetAttr("ok", true)
	child := tr.StartChild(root.Context(), "process", KindConsumer)
	child.RecordError(errors.New("boom"))
	child.End()
	root.End()
	root.End() // idempotent: must export once
	if err := tr.Shutdown(context.Background()); err != nil {
		t.Fatal(err)
	}

	col.mu.Lock()
	defer col.mu.Unlock()
	if len(col.bodies) == 0 {
		t.Fatal("nothing exported")
	}
	var doc struct {
		ResourceSpans []struct {
			Resource   struct{ Attributes []map[string]any } `json:"resource"`
			ScopeSpans []struct {
				Spans []map[string]any `json:"spans"`
			} `json:"scopeSpans"`
		} `json:"resourceSpans"`
	}
	if err := json.Unmarshal(col.bodies[0], &doc); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	spans := doc.ResourceSpans[0].ScopeSpans[0].Spans
	if len(spans) != 2 {
		t.Fatalf("exported %d spans, want 2 (End must be idempotent)", len(spans))
	}
	byName := map[string]map[string]any{}
	for _, s := range spans {
		byName[s["name"].(string)] = s
	}
	p, c := byName["publish"], byName["process"]
	if c["parentSpanId"] != p["spanId"] || c["traceId"] != p["traceId"] {
		t.Fatalf("parentage broken: parent=%v child=%v", p, c)
	}
	if _, has := p["parentSpanId"]; has {
		t.Error("root must omit parentSpanId")
	}
	if len(p["traceId"].(string)) != 32 || len(p["spanId"].(string)) != 16 {
		t.Error("ids must be lowercase hex of 32/16 chars")
	}
	if p["kind"].(float64) != 4 || c["kind"].(float64) != 5 {
		t.Errorf("kinds: %v %v", p["kind"], c["kind"])
	}
	if st := c["status"].(map[string]any); st["code"].(float64) != 2 || st["message"] != "boom" {
		t.Errorf("status: %v", st)
	}
	if got := doc.ResourceSpans[0].Resource.Attributes[0]["key"]; got != "service.name" {
		t.Errorf("resource attr: %v", got)
	}
	// int attrs are strings in OTLP/JSON
	found := false
	for _, a := range p["attributes"].([]any) {
		m := a.(map[string]any)
		if m["key"] == "count" {
			found = m["value"].(map[string]any)["intValue"] == "3"
		}
	}
	if !found {
		t.Error("int attribute must be encoded as intValue string")
	}
}

func TestExportRetriesTransientButNotClientErrors(t *testing.T) {
	col := &collector{}
	col.status.Store(503)
	srv := httptest.NewServer(col)
	defer srv.Close()
	tr := New(Config{Endpoint: srv.URL, SampleRatio: 1, FlushInterval: time.Hour})
	tr.StartRoot("a", KindInternal).End()
	_ = tr.Shutdown(context.Background())
	if col.hits.Load() != 2 || tr.ExportErrors.Load() != 1 {
		t.Fatalf("503: hits=%d errors=%d, want 2 attempts and 1 error", col.hits.Load(), tr.ExportErrors.Load())
	}

	col2 := &collector{}
	col2.status.Store(400)
	srv2 := httptest.NewServer(col2)
	defer srv2.Close()
	tr2 := New(Config{Endpoint: srv2.URL, SampleRatio: 1, FlushInterval: time.Hour})
	tr2.StartRoot("a", KindInternal).End()
	_ = tr2.Shutdown(context.Background())
	if col2.hits.Load() != 1 {
		t.Fatalf("400 must not be retried, hits=%d", col2.hits.Load())
	}
}

func TestFullQueueDropsInsteadOfBlocking(t *testing.T) {
	block := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { <-block }))
	defer srv.Close()
	defer close(block)
	tr := New(Config{Endpoint: srv.URL, SampleRatio: 1, QueueSize: 4, MaxBatch: 1, Timeout: time.Minute})
	start := time.Now()
	for i := 0; i < 1000; i++ {
		tr.StartRoot("x", KindInternal).End()
	}
	if time.Since(start) > 2*time.Second {
		t.Fatal("End() blocked on a stalled collector")
	}
	if tr.Dropped.Load() == 0 {
		t.Fatal("expected drops with a stalled collector and tiny queue")
	}
}

func FuzzParseTraceparent(f *testing.F) {
	f.Add("00-0af7651916cd43dd8448eb211c80319c-b7ad6b7169203331-01")
	f.Fuzz(func(t *testing.T, s string) {
		if c, ok := ParseTraceparent(s); ok && !c.IsValid() { // must never report ok for an invalid context
			t.Fatalf("ok but invalid: %q", s)
		}
	})
}

func TestRegisterMetricsExposesExporterHealth(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(500) }))
	defer srv.Close()
	tr := New(Config{Endpoint: srv.URL, SampleRatio: 1, FlushInterval: time.Hour})
	reg := metrics.NewRegistry()
	tr.RegisterMetrics(reg)
	(*Tracer)(nil).RegisterMetrics(reg) // nil-safe
	tr.StartRoot("a", KindInternal).End()
	_ = tr.Shutdown(context.Background())
	var sb strings.Builder
	_, _ = reg.WriteTo(&sb)
	if !strings.Contains(sb.String(), "sf_trace_export_errors_total 1") || !strings.Contains(sb.String(), "# TYPE sf_trace_spans_dropped_total counter") {
		t.Fatalf("exporter health not exposed:\n%s", sb.String())
	}
}
