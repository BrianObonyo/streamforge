package ingestion

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

var fixedNow = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func gen(cfg GenConfig) *Generator {
	cfg.Now = func() time.Time { return fixedNow }
	return NewGenerator(cfg)
}

func TestGeneratorIsDeterministic(t *testing.T) {
	a, b := gen(GenConfig{Seed: 42, DisorderProb: .3, DupProb: .1}), gen(GenConfig{Seed: 42, DisorderProb: .3, DupProb: .1})
	for i := 0; i < 500; i++ {
		x, y := a.Next(), b.Next()
		if !bytes.Equal(x.Value, y.Value) {
			t.Fatalf("message %d differs between identical seeds", i)
		}
	}
	c := gen(GenConfig{Seed: 43})
	if bytes.Equal(gen(GenConfig{Seed: 42}).Next().Value, c.Next().Value) {
		t.Fatal("different seeds produced identical output")
	}
}

func TestGeneratedEventsAreValidAndCoverAllKinds(t *testing.T) {
	g := gen(GenConfig{Seed: 1})
	kinds := map[events.Kind]int{}
	for i := 0; i < 600; i++ {
		m := g.Next()
		ev, err := events.Decode(m.Value)
		if err != nil || ev.Validate(fixedNow) != nil {
			t.Fatalf("msg %d invalid: %v / %v", i, err, ev.Validate(fixedNow))
		}
		kinds[ev.Kind]++
	}
	for _, k := range events.AllKinds {
		if kinds[k] < 100 {
			t.Errorf("kind %s only %d/600", k, kinds[k])
		}
	}
}

func TestFaultInjectionRates(t *testing.T) {
	g := gen(GenConfig{Seed: 7, DisorderProb: 0.2, MaxDelay: 2 * time.Second, DupProb: 0.1, PoisonProb: 0.05})
	var dup, poison, late, n int
	for ; n < 20000; n++ {
		m := g.Next()
		switch {
		case m.Poison:
			poison++
			if _, err := events.Decode(m.Value); err == nil {
				t.Fatalf("poison message decoded cleanly: %q", m.Value)
			}
		case m.Dup:
			dup++
		case m.Event.EventTime.Before(fixedNow):
			late++
			if fixedNow.Sub(m.Event.EventTime) > 2*time.Second {
				t.Fatal("delay exceeded MaxDelay")
			}
		}
	}
	within := func(name string, got int, want float64) {
		if f := float64(got) / float64(n); f < want*0.7 || f > want*1.3 {
			t.Errorf("%s rate %.3f, want ≈%.3f", name, f, want)
		}
	}
	within("poison", poison, 0.05)
	within("dup", dup, 0.1*0.95) // dups are drawn only from non-poison draws
}

func newPub(t *testing.T) (*Publisher, *streaming.MemBroker, streaming.Consumer) {
	t.Helper()
	b := streaming.NewMemBroker()
	_ = b.CreateTopic(context.Background(), "events", 2)
	p, _ := b.NewProducer(partitioning.Hash{})
	c, _ := b.NewConsumer("g", "m", "events")
	return NewPublisher(p, "events", reliability.RetryPolicy{MaxAttempts: 4, Base: time.Millisecond, Max: 2 * time.Millisecond}, nil), b, c
}

func TestPublisherRetriesTransientErrors(t *testing.T) {
	pub, b, c := newPub(t)
	var fails atomic.Int32
	b.InjectProduceFault(func(string) error {
		if fails.Add(1) <= 2 {
			return errors.New("not leader for partition")
		}
		return nil
	})
	ev := events.Event{ID: "1", Kind: events.KindIoT, Key: "s", Value: 1, EventTime: fixedNow}
	if err := pub.Publish(context.Background(), ev); err != nil {
		t.Fatalf("publish should have recovered: %v", err)
	}
	if pub.retries.Value() != 2 {
		t.Errorf("retries=%d want 2", pub.retries.Value())
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	_, _ = c.Poll(ctx, 1)
	recs, _ := c.Poll(ctx, 10)
	if len(recs) != 1 {
		t.Fatalf("got %d records", len(recs))
	}
	got, _ := events.Decode(recs[0].Value)
	if got.IngestTime.IsZero() {
		t.Error("IngestTime not stamped")
	}
}

func TestPublisherGivesUp(t *testing.T) {
	pub, b, _ := newPub(t)
	b.InjectProduceFault(func(string) error { return errors.New("down") })
	err := pub.Publish(context.Background(), events.Event{ID: "1", Kind: events.KindIoT, Key: "s", Value: 1, EventTime: fixedNow})
	if err == nil || pub.failed.Value() != 1 {
		t.Fatalf("err=%v failed=%d", err, pub.failed.Value())
	}
}

func post(h http.Handler, body string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/events", strings.NewReader(body)))
	return rr
}

func TestHTTPStatusCodes(t *testing.T) {
	pub, _, _ := newPub(t)
	h := &Handler{Pub: pub, Now: func() time.Time { return fixedNow }}
	ts := fixedNow.Format(time.RFC3339)

	if rr := post(h, `{"kind":"iot","key":"s1","value":21.5,"event_time":"`+ts+`"}`); rr.Code != http.StatusAccepted {
		t.Errorf("single valid: %d %s", rr.Code, rr.Body)
	}
	if rr := post(h, `[{"kind":"iot","key":"s1","value":1,"event_time":"`+ts+`"},{"kind":"bogus","key":"s2","value":1,"event_time":"`+ts+`"}]`); rr.Code != http.StatusMultiStatus {
		t.Errorf("mixed batch: %d %s", rr.Code, rr.Body)
	} else {
		var resp response
		_ = json.Unmarshal(rr.Body.Bytes(), &resp)
		if resp.Accepted != 1 || len(resp.Rejected) != 1 || resp.Rejected[0].Index != 1 {
			t.Errorf("resp=%+v", resp)
		}
	}
	if rr := post(h, `[{"kind":"bogus"}]`); rr.Code != http.StatusBadRequest {
		t.Errorf("all invalid: %d", rr.Code)
	}
	if rr := post(h, `not json`); rr.Code != http.StatusBadRequest {
		t.Errorf("garbage: %d", rr.Code)
	}
	if rr := post(h, `[]`); rr.Code != http.StatusBadRequest {
		t.Errorf("empty batch: %d", rr.Code)
	}
	// Future-dated events are rejected (watermark-poisoning protection).
	future := fixedNow.Add(time.Hour).Format(time.RFC3339)
	if rr := post(h, `{"kind":"iot","key":"s","value":1,"event_time":"`+future+`"}`); rr.Code != http.StatusBadRequest {
		t.Errorf("future event: %d", rr.Code)
	}
	if rr := post(h, `{"kind":"iot","key":"s","value":1}`); rr.Code != http.StatusBadRequest {
		t.Errorf("missing event_time: %d", rr.Code)
	}
}

func TestHTTPMethodAndSizeLimits(t *testing.T) {
	pub, _, _ := newPub(t)
	h := &Handler{Pub: pub}
	rr := httptest.NewRecorder()
	h.ServeHTTP(rr, httptest.NewRequest(http.MethodGet, "/v1/events", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET: %d", rr.Code)
	}
	if rr := post(h, `{"x":"`+strings.Repeat("a", maxBodyBytes+10)+`"}`); rr.Code != http.StatusRequestEntityTooLarge {
		t.Errorf("oversize: %d", rr.Code)
	}
}

func TestHTTPRateLimit429(t *testing.T) {
	pub, _, _ := newPub(t)
	h := &Handler{Pub: pub, Limiter: backpressure.NewTokenBucket(1, 1), Now: func() time.Time { return fixedNow }}
	body := `{"kind":"iot","key":"s","value":1,"event_time":"` + fixedNow.Format(time.RFC3339) + `"}`
	if rr := post(h, body); rr.Code != http.StatusAccepted {
		t.Fatalf("first: %d", rr.Code)
	}
	rr := post(h, body)
	if rr.Code != http.StatusTooManyRequests || rr.Header().Get("Retry-After") == "" {
		t.Fatalf("second: %d retry-after=%q", rr.Code, rr.Header().Get("Retry-After"))
	}
}

func TestHTTPBrokerDown502(t *testing.T) {
	pub, b, _ := newPub(t)
	b.InjectProduceFault(func(string) error { return errors.New("down") })
	h := &Handler{Pub: pub, Now: func() time.Time { return fixedNow }}
	rr := post(h, `{"kind":"iot","key":"s","value":1,"event_time":"`+fixedNow.Format(time.RFC3339)+`"}`)
	if rr.Code != http.StatusBadGateway {
		t.Fatalf("got %d", rr.Code)
	}
}

func TestRunStopsAtLimit(t *testing.T) {
	pub, b, _ := newPub(t)
	n, err := gen(GenConfig{Seed: 1}).Run(context.Background(), pub, 250)
	if err != nil || n != 250 {
		t.Fatalf("n=%d err=%v", n, err)
	}
	var total int64
	for _, e := range b.EndOffsets("events") {
		total += e
	}
	if total != 250 {
		t.Fatalf("broker holds %d records", total)
	}
}
