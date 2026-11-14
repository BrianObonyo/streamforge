package api

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/storage"
)

var now = time.Date(2026, 11, 14, 10, 0, 0, 0, time.UTC)

func seeded() *Server {
	st := storage.NewMemStore()
	w := aggregation.Window{Start: now.Add(-30 * time.Second), End: now.Add(-20 * time.Second)}
	_ = st.WriteResults(context.Background(), []aggregation.Result{
		{Window: w, Kind: events.KindIoT, Key: "s1", Stats: aggregation.Stats{Count: 10, Sum: 220, Min: 20, Max: 25}, Version: 10},
		{Window: w, Kind: events.KindMarket, Key: "BTC", Stats: aggregation.Stats{Count: 5, Sum: 500, Min: 99, Max: 101}, Version: 5},
	})
	_ = st.WriteLateStats(context.Background(), []storage.LateStatRow{{TS: now.Add(-time.Minute), OnTime: 90, Late: 8, TooLate: 2, MaxLatenessMs: 1500}})
	reg := metrics.NewRegistry()
	reg.Counter("sf_x_total", "x", nil).Inc()
	return &Server{Store: st, Metrics: reg, Now: func() time.Time { return now }}
}

func get(s *Server, path string) *httptest.ResponseRecorder {
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodGet, path, nil))
	return rr
}

func decode(t *testing.T, rr *httptest.ResponseRecorder) map[string]any {
	t.Helper()
	var m map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &m); err != nil {
		t.Fatalf("bad JSON %q: %v", rr.Body, err)
	}
	return m
}

func TestSummary(t *testing.T) {
	rr := get(seeded(), "/v1/summary?since=10m")
	if rr.Code != 200 {
		t.Fatalf("%d %s", rr.Code, rr.Body)
	}
	m := decode(t, rr)
	if m["total_events"].(float64) != 15 || len(m["categories"].([]any)) != 2 {
		t.Fatalf("%v", m)
	}
	one := decode(t, get(seeded(), "/v1/summary?kind=iot&since=10m"))
	if one["total_events"].(float64) != 10 {
		t.Fatalf("kind filter: %v", one)
	}
}

func TestSeriesAndLate(t *testing.T) {
	m := decode(t, get(seeded(), "/v1/series?kind=market&key=BTC&since=1h"))
	if pts := m["points"].([]any); len(pts) != 1 {
		t.Fatalf("%v", m)
	}
	l := decode(t, get(seeded(), "/v1/late?since=1h"))
	if l["total"].(float64) != 100 || l["late_fraction"].(float64) != 0.08 || l["max_lateness_ms"].(float64) != 1500 {
		t.Fatalf("%v", l)
	}
}

func TestThroughput(t *testing.T) {
	m := decode(t, get(seeded(), "/v1/throughput?window=60s"))
	if m["events"].(float64) != 15 || m["events_per_second"].(float64) != 0.25 {
		t.Fatalf("%v", m)
	}
}

func TestValidationRejectsBadInput(t *testing.T) {
	s := seeded()
	for _, p := range []string{
		"/v1/summary?kind=bogus", "/v1/summary?since=abc", "/v1/summary?since=-5m",
		"/v1/summary?from=yesterday", "/v1/series?limit=0", "/v1/series?limit=999999",
		"/v1/summary?from=2026-11-14T10:00:00Z&to=2026-11-14T09:00:00Z", "/v1/throughput?window=1ms",
		"/v1/summary?key=" + strings.Repeat("a", 300),
	} {
		if rr := get(s, p); rr.Code != http.StatusBadRequest {
			t.Errorf("%s -> %d, want 400", p, rr.Code)
		}
	}
}

func TestMethodsAndOps(t *testing.T) {
	s := seeded()
	rr := httptest.NewRecorder()
	s.Handler().ServeHTTP(rr, httptest.NewRequest(http.MethodPost, "/v1/summary", nil))
	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("POST: %d", rr.Code)
	}
	if get(s, "/healthz").Code != 200 || get(s, "/readyz").Code != 200 {
		t.Error("health endpoints")
	}
	if m := get(s, "/metrics"); !strings.Contains(m.Body.String(), "sf_x_total 1") {
		t.Errorf("metrics: %s", m.Body)
	}
}

type brokenStore struct{ storage.Store }

func (brokenStore) Summary(context.Context, storage.Query) ([]storage.SummaryRow, error) {
	return nil, errors.New(`Code: 60. DB::Exception: Table streamforge.window_results doesn't exist`)
}
func (brokenStore) LateSummary(context.Context, storage.Query) (storage.LateSummary, error) {
	return storage.LateSummary{}, errors.New("down")
}

func TestStoreErrorsAreNotLeaked(t *testing.T) {
	s := &Server{Store: brokenStore{storage.NewMemStore()}, Now: func() time.Time { return now }}
	rr := get(s, "/v1/summary")
	if rr.Code != http.StatusBadGateway || strings.Contains(rr.Body.String(), "streamforge.window_results") {
		t.Fatalf("%d %s", rr.Code, rr.Body)
	}
	if get(s, "/readyz").Code != http.StatusServiceUnavailable {
		t.Error("readyz should fail when the store is down")
	}
}
