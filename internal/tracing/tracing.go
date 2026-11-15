// Package tracing is a small, dependency-free distributed-tracing layer that
// speaks the OpenTelemetry wire standards:
//
//   - W3C Trace Context ("traceparent") for propagation, and
//   - OTLP/HTTP with JSON encoding (POST {endpoint}/v1/traces) for export,
//
// so traces from StreamForge appear in any OTLP backend — the OpenTelemetry
// Collector, Jaeger, Tempo, Grafana Cloud — with no vendor SDK.
//
// WHY NOT THE OFFICIAL otel-go SDK? Keeping the core stdlib-only (ADR-0003) is a
// project goal, and the surface we need is small: create spans, propagate context
// through the broker, export in batches. This package is deliberately shaped like
// the OTel API (Tracer / Span / SetAttr / End) so swapping in the SDK later is a
// mechanical change. See ADR-0006 for the trade-offs.
//
// DESIGN RULES
//
//  1. Tracing must NEVER slow or break the pipeline. Span.End is a non-blocking
//     channel send; if the export queue is full the span is DROPPED and counted.
//     Export failures are logged and counted, never returned to hot paths.
//  2. A nil *Tracer and nil *Span are valid no-ops, so call sites never branch on
//     "is tracing enabled".
//  3. Sampling is decided ONCE, at the trace root, and honoured downstream via the
//     traceparent sampled flag — so a trace is complete or absent, never partial.
//
// Specs: https://www.w3.org/TR/trace-context/ ·
// https://opentelemetry.io/docs/specs/otlp/#otlphttp
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package tracing

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	mrand "math/rand/v2"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/BrianObonyo/streamforge/internal/metrics"
)

// SpanContext identifies a span within a trace.
type SpanContext struct {
	TraceID [16]byte
	SpanID  [8]byte
	Sampled bool
}

// IsValid reports whether both IDs are non-zero (the W3C validity rule).
func (c SpanContext) IsValid() bool { return c.TraceID != [16]byte{} && c.SpanID != [8]byte{} }

// String renders the W3C traceparent header value.
func (c SpanContext) String() string {
	flags := "00"
	if c.Sampled {
		flags = "01"
	}
	return "00-" + hex.EncodeToString(c.TraceID[:]) + "-" + hex.EncodeToString(c.SpanID[:]) + "-" + flags
}

// ParseTraceparent parses a W3C traceparent value. It is strict about the
// version-00 format and rejects all-zero IDs, as the spec requires.
func ParseTraceparent(s string) (SpanContext, bool) {
	// 00-<32 hex>-<16 hex>-<2 hex> = 55 chars
	if len(s) != 55 || s[0:3] != "00-" || s[35] != '-' || s[52] != '-' {
		return SpanContext{}, false
	}
	var c SpanContext
	if _, err := hex.Decode(c.TraceID[:], []byte(s[3:35])); err != nil {
		return SpanContext{}, false
	}
	if _, err := hex.Decode(c.SpanID[:], []byte(s[36:52])); err != nil {
		return SpanContext{}, false
	}
	var fl [1]byte
	if _, err := hex.Decode(fl[:], []byte(s[53:55])); err != nil {
		return SpanContext{}, false
	}
	c.Sampled = fl[0]&1 == 1
	return c, c.IsValid()
}

// Kind is the OTLP span kind.
type Kind int

const (
	KindInternal Kind = 1
	KindServer   Kind = 2
	KindClient   Kind = 3
	KindProducer Kind = 4
	KindConsumer Kind = 5
)

type attr struct {
	key string
	val any // string | int64 | float64 | bool
}

// Span is one timed operation. Safe to call on nil.
type Span struct {
	t      *Tracer
	sc     SpanContext
	parent [8]byte
	name   string
	kind   Kind
	start  time.Time
	attrs  []attr
	errMsg string
	ended  atomic.Bool
}

// Context returns the span's identity (zero value for a nil span).
func (s *Span) Context() SpanContext {
	if s == nil {
		return SpanContext{}
	}
	return s.sc
}

// SetAttr attaches an attribute (string, int, int64, float64 or bool).
func (s *Span) SetAttr(key string, v any) {
	if s == nil {
		return
	}
	switch x := v.(type) {
	case int:
		v = int64(x)
	case uint64:
		v = int64(x)
	case time.Duration:
		v = x.Milliseconds()
	}
	s.attrs = append(s.attrs, attr{key, v})
}

// RecordError marks the span failed.
func (s *Span) RecordError(err error) {
	if s != nil && err != nil {
		s.errMsg = err.Error()
	}
}

// End finishes the span and queues it for export. Idempotent and non-blocking.
func (s *Span) End() {
	if s == nil || !s.ended.CompareAndSwap(false, true) {
		return
	}
	s.t.enqueue(finished{s, time.Now()})
}

type finished struct {
	s   *Span
	end time.Time
}

// Config configures a Tracer.
type Config struct {
	ServiceName   string        // resource service.name (default "streamforge")
	Endpoint      string        // OTLP/HTTP base URL, e.g. http://localhost:4318 ("" ⇒ tracing disabled)
	SampleRatio   float64       // probability a NEW trace is sampled, 0..1
	FlushInterval time.Duration // default 1s
	MaxBatch      int           // spans per export request (default 512)
	QueueSize     int           // buffered spans before dropping (default 8192)
	Timeout       time.Duration // per-export HTTP timeout (default 5s)
	Headers       map[string]string
	Logger        *slog.Logger
}

// Tracer creates spans and exports them in the background.
type Tracer struct {
	cfg   Config
	hc    *http.Client
	queue chan finished
	done  chan struct{}
	wg    sync.WaitGroup
	url   string

	Exported, Dropped, ExportErrors atomic.Uint64
}

// New returns a started Tracer, or nil (a valid no-op tracer) if cfg.Endpoint is empty.
func New(cfg Config) *Tracer {
	if cfg.Endpoint == "" {
		return nil
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "streamforge"
	}
	if cfg.FlushInterval <= 0 {
		cfg.FlushInterval = time.Second
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 512
	}
	if cfg.QueueSize <= 0 {
		cfg.QueueSize = 8192
	}
	if cfg.Timeout <= 0 {
		cfg.Timeout = 5 * time.Second
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	t := &Tracer{
		cfg: cfg, hc: &http.Client{Timeout: cfg.Timeout},
		queue: make(chan finished, cfg.QueueSize), done: make(chan struct{}),
		url: strings.TrimRight(cfg.Endpoint, "/") + "/v1/traces",
	}
	t.wg.Add(1)
	go t.loop()
	return t
}

func randomIDs() (tid [16]byte, sid [8]byte) {
	_, _ = rand.Read(tid[:]) // crypto/rand: ids must be unguessable-ish and collision-free across hosts
	_, _ = rand.Read(sid[:])
	return
}

func newSpanID() (sid [8]byte) {
	for sid == [8]byte{} { // all-zero is invalid per W3C
		_, _ = rand.Read(sid[:])
	}
	return
}

func (t *Tracer) sample() bool {
	r := t.cfg.SampleRatio
	return r >= 1 || (r > 0 && mrand.Float64() < r)
}

// StartRoot begins a NEW trace; the sampling decision is made here. An
// unsampled root returns nil, so the whole subtree costs nothing.
func (t *Tracer) StartRoot(name string, kind Kind) *Span {
	if t == nil || !t.sample() {
		return nil
	}
	tid, sid := randomIDs()
	return &Span{t: t, sc: SpanContext{tid, sid, true}, name: name, kind: kind, start: time.Now()}
}

// StartChild continues a trace from a remote/parent context. If the parent is
// invalid or NOT sampled, no span is created (sampling is inherited).
func (t *Tracer) StartChild(parent SpanContext, name string, kind Kind) *Span {
	if t == nil || !parent.IsValid() || !parent.Sampled {
		return nil
	}
	return &Span{t: t, sc: SpanContext{parent.TraceID, newSpanID(), true}, parent: parent.SpanID,
		name: name, kind: kind, start: time.Now()}
}

// StartFromTraceparent is StartChild for a raw header/payload string. An absent or
// malformed value starts nothing (never fails the caller).
func (t *Tracer) StartFromTraceparent(tp, name string, kind Kind) *Span {
	pc, ok := ParseTraceparent(tp)
	if !ok {
		return nil
	}
	return t.StartChild(pc, name, kind)
}

func (t *Tracer) enqueue(f finished) {
	select {
	case t.queue <- f:
	default:
		t.Dropped.Add(1) // never block the pipeline for telemetry
	}
}

func (t *Tracer) loop() {
	defer t.wg.Done()
	tick := time.NewTicker(t.cfg.FlushInterval)
	defer tick.Stop()
	batch := make([]finished, 0, t.cfg.MaxBatch)
	flush := func() {
		if len(batch) > 0 {
			t.export(batch)
			batch = batch[:0]
		}
	}
	for {
		select {
		case f := <-t.queue:
			if batch = append(batch, f); len(batch) >= t.cfg.MaxBatch {
				flush()
			}
		case <-tick.C:
			flush()
		case <-t.done:
			for { // drain whatever is already queued, then final flush
				select {
				case f := <-t.queue:
					if batch = append(batch, f); len(batch) >= t.cfg.MaxBatch {
						flush()
					}
				default:
					flush()
					return
				}
			}
		}
	}
}

// RegisterMetrics exposes the exporter's health as Prometheus counters, so a dead
// collector or an undersized queue is visible instead of silently losing traces.
// Safe on a nil tracer (registers nothing).
func (t *Tracer) RegisterMetrics(reg *metrics.Registry) {
	if t == nil || reg == nil {
		return
	}
	reg.CounterFunc("sf_trace_spans_exported_total", "Spans delivered to the OTLP collector.", nil, func() float64 { return float64(t.Exported.Load()) })
	reg.CounterFunc("sf_trace_spans_dropped_total", "Spans dropped because the export queue was full.", nil, func() float64 { return float64(t.Dropped.Load()) })
	reg.CounterFunc("sf_trace_export_errors_total", "Span batches that failed to export.", nil, func() float64 { return float64(t.ExportErrors.Load()) })
}

// Shutdown flushes pending spans (bounded by ctx) and stops the exporter.
func (t *Tracer) Shutdown(ctx context.Context) error {
	if t == nil {
		return nil
	}
	select {
	case <-t.done:
		return nil // already shut down
	default:
		close(t.done)
	}
	fin := make(chan struct{})
	go func() { t.wg.Wait(); close(fin) }()
	select {
	case <-fin:
		return nil
	case <-ctx.Done():
		return fmt.Errorf("tracing shutdown: %w", ctx.Err())
	}
}

// ------------------------------------------------------------------ OTLP JSON

type kv struct {
	Key   string `json:"key"`
	Value anyVal `json:"value"`
}
type anyVal struct {
	S *string  `json:"stringValue,omitempty"`
	I *string  `json:"intValue,omitempty"` // OTLP/JSON encodes int64 as a decimal string
	D *float64 `json:"doubleValue,omitempty"`
	B *bool    `json:"boolValue,omitempty"`
}
type otlpSpan struct {
	TraceID string `json:"traceId"`
	SpanID  string `json:"spanId"`
	Parent  string `json:"parentSpanId,omitempty"`
	Name    string `json:"name"`
	Kind    int    `json:"kind"`
	Start   string `json:"startTimeUnixNano"`
	End     string `json:"endTimeUnixNano"`
	Attrs   []kv   `json:"attributes,omitempty"`
	Status  *struct {
		Code    int    `json:"code"`
		Message string `json:"message,omitempty"`
	} `json:"status,omitempty"`
}

func toKV(a attr) kv {
	switch v := a.val.(type) {
	case string:
		return kv{a.key, anyVal{S: &v}}
	case int64:
		s := fmt.Sprint(v)
		return kv{a.key, anyVal{I: &s}}
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) { // not representable in JSON
			s := fmt.Sprint(v)
			return kv{a.key, anyVal{S: &s}}
		}
		return kv{a.key, anyVal{D: &v}}
	case bool:
		return kv{a.key, anyVal{B: &v}}
	}
	s := fmt.Sprint(a.val)
	return kv{a.key, anyVal{S: &s}}
}

func (t *Tracer) payload(batch []finished) ([]byte, error) {
	spans := make([]otlpSpan, 0, len(batch))
	for _, f := range batch {
		s := f.s
		o := otlpSpan{
			TraceID: hex.EncodeToString(s.sc.TraceID[:]), SpanID: hex.EncodeToString(s.sc.SpanID[:]),
			Name: s.name, Kind: int(s.kind),
			Start: fmt.Sprint(s.start.UnixNano()), End: fmt.Sprint(f.end.UnixNano()),
		}
		if s.parent != [8]byte{} {
			o.Parent = hex.EncodeToString(s.parent[:])
		}
		for _, a := range s.attrs {
			o.Attrs = append(o.Attrs, toKV(a))
		}
		if s.errMsg != "" {
			o.Status = &struct {
				Code    int    `json:"code"`
				Message string `json:"message,omitempty"`
			}{Code: 2, Message: s.errMsg} // STATUS_CODE_ERROR
		}
		spans = append(spans, o)
	}
	svc := t.cfg.ServiceName
	doc := map[string]any{"resourceSpans": []any{map[string]any{
		"resource":   map[string]any{"attributes": []kv{{"service.name", anyVal{S: &svc}}}},
		"scopeSpans": []any{map[string]any{"scope": map[string]any{"name": "github.com/BrianObonyo/streamforge"}, "spans": spans}},
	}}}
	return json.Marshal(doc)
}

// export sends one batch, retrying once on a transient failure. Errors are
// counted and logged, never propagated.
func (t *Tracer) export(batch []finished) {
	body, err := t.payload(batch)
	if err != nil {
		t.ExportErrors.Add(1)
		t.cfg.Logger.Warn("trace encode failed", "err", err)
		return
	}
	for attempt := 0; attempt < 2; attempt++ {
		if attempt > 0 {
			time.Sleep(200 * time.Millisecond)
		}
		req, _ := http.NewRequest(http.MethodPost, t.url, bytes.NewReader(body))
		req.Header.Set("Content-Type", "application/json")
		for k, v := range t.cfg.Headers {
			req.Header.Set(k, v)
		}
		resp, err := t.hc.Do(req)
		if err == nil {
			_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
			resp.Body.Close()
			switch {
			case resp.StatusCode/100 == 2:
				t.Exported.Add(uint64(len(batch)))
				return
			case resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500:
				err = fmt.Errorf("collector HTTP %d", resp.StatusCode) // transient: retry
			default:
				t.ExportErrors.Add(1) // 4xx: our payload is wrong; retrying won't help
				t.cfg.Logger.Warn("trace export rejected", "status", resp.StatusCode)
				return
			}
		}
		if attempt == 1 {
			t.ExportErrors.Add(1)
			t.cfg.Logger.Warn("trace export failed", "spans", len(batch), "err", err)
		}
	}
}

// RegisterFlags binds the standard tracing options to fs and returns a builder to
// call after fs.Parse. Defaults honour the standard OpenTelemetry environment
// variables, so existing OTel deployment conventions just work:
//
//	OTEL_EXPORTER_OTLP_ENDPOINT   base URL of an OTLP/HTTP collector (unset = tracing off)
//	OTEL_SERVICE_NAME             resource service.name
//	SF_TRACE_SAMPLE               fraction of new traces to sample (default 0.01)
func RegisterFlags(fs *flag.FlagSet) func(defaultService string, log *slog.Logger) *Tracer {
	env := func(k, d string) string {
		if v := os.Getenv(k); v != "" {
			return v
		}
		return d
	}
	ratio := 0.01
	if v, err := strconv.ParseFloat(os.Getenv("SF_TRACE_SAMPLE"), 64); err == nil {
		ratio = v
	}
	endpoint := fs.String("otlp-endpoint", env("OTEL_EXPORTER_OTLP_ENDPOINT", ""), "OTLP/HTTP collector base URL, e.g. http://localhost:4318 (empty = tracing off)")
	service := fs.String("service-name", env("OTEL_SERVICE_NAME", ""), "trace resource service.name")
	sample := fs.Float64("trace-sample", ratio, "fraction of new traces to sample, 0..1")
	return func(def string, log *slog.Logger) *Tracer {
		name := *service
		if name == "" {
			name = def
		}
		return New(Config{ServiceName: name, Endpoint: *endpoint, SampleRatio: *sample, Logger: log})
	}
}
