// Package api serves the read-side analytics HTTP API, plus health and metrics.
//
//	GET /v1/summary     events / avg / min / max per category (kind)
//	GET /v1/series      per-window time series (events per interval, avg, max)
//	GET /v1/throughput  recent processing throughput, derived from the store
//	GET /v1/late        late-arrival statistics
//	GET /healthz        liveness   (always 200 if the process is up)
//	GET /readyz         readiness  (200 only if the store answers)
//	GET /metrics        Prometheus exposition
//
// Common query parameters: kind, key, from/to (RFC 3339) or since (e.g. 15m),
// limit. Every value is validated here and then passed to the store as a typed
// parameter — never spliced into SQL.
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/storage"
)

// Server wires the HTTP handlers.
type Server struct {
	Store   storage.Store
	Metrics *metrics.Registry
	Log     *slog.Logger
	Now     func() time.Time
}

const (
	defaultSince = time.Hour
	maxLimit     = 10000
	reqTimeout   = 10 * time.Second
)

// Handler returns the routed, middleware-wrapped handler.
func (s *Server) Handler() http.Handler {
	if s.Now == nil {
		s.Now = time.Now
	}
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
	mux.HandleFunc("/readyz", s.ready)
	mux.HandleFunc("/metrics", s.metrics)
	mux.HandleFunc("/v1/summary", s.summary)
	mux.HandleFunc("/v1/series", s.series)
	mux.HandleFunc("/v1/throughput", s.throughput)
	mux.HandleFunc("/v1/late", s.late)
	return s.wrap(mux)
}

// wrap adds: GET/HEAD only, a per-request timeout, panic recovery, and defensive
// headers. A panic in one handler must not take down the server.
func (s *Server) wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if s.Log != nil {
					s.Log.Error("handler panic", "path", r.URL.Path, "panic", v)
				}
				http.Error(w, "internal error", http.StatusInternalServerError)
			}
		}()
		if r.Method != http.MethodGet && r.Method != http.MethodHead {
			w.Header().Set("Allow", "GET, HEAD")
			http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
			return
		}
		ctx, cancel := context.WithTimeout(r.Context(), reqTimeout)
		defer cancel()
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Cache-Control", "no-store")
		next.ServeHTTP(w, r.WithContext(ctx))
	})
}

type apiError struct {
	Error string `json:"error"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func badRequest(w http.ResponseWriter, format string, a ...any) {
	writeJSON(w, http.StatusBadRequest, apiError{fmt.Sprintf(format, a...)})
}

func (s *Server) storeError(w http.ResponseWriter, r *http.Request, err error) {
	if s.Log != nil {
		s.Log.Error("store query failed", "path", r.URL.Path, "err", err)
	}
	// Don't leak backend error text (table names, SQL) to clients.
	writeJSON(w, http.StatusBadGateway, apiError{"analytics store unavailable"})
}

// parseQuery validates the shared query parameters.
func (s *Server) parseQuery(r *http.Request) (storage.Query, error) {
	v := r.URL.Query()
	q := storage.Query{Key: v.Get("key")}
	if k := v.Get("kind"); k != "" {
		if !events.Kind(k).Valid() {
			return q, fmt.Errorf("unknown kind %q (want iot|clickstream|market)", k)
		}
		q.Kind = events.Kind(k)
	}
	if len(q.Key) > 256 {
		return q, fmt.Errorf("key too long")
	}
	now := s.Now()
	var err error
	if f := v.Get("from"); f != "" {
		if q.From, err = time.Parse(time.RFC3339, f); err != nil {
			return q, fmt.Errorf("from: want RFC 3339 timestamp")
		}
	} else {
		since := defaultSince
		if sv := v.Get("since"); sv != "" {
			if since, err = time.ParseDuration(sv); err != nil || since <= 0 {
				return q, fmt.Errorf("since: want a positive duration like 15m")
			}
		}
		q.From = now.Add(-since)
	}
	if t := v.Get("to"); t != "" {
		if q.To, err = time.Parse(time.RFC3339, t); err != nil {
			return q, fmt.Errorf("to: want RFC 3339 timestamp")
		}
		if !q.To.After(q.From) {
			return q, fmt.Errorf("to must be after from")
		}
	}
	if l := v.Get("limit"); l != "" {
		if q.Limit, err = strconv.Atoi(l); err != nil || q.Limit < 1 || q.Limit > maxLimit {
			return q, fmt.Errorf("limit: want 1..%d", maxLimit)
		}
	}
	return q, nil
}

func (s *Server) summary(w http.ResponseWriter, r *http.Request) {
	q, err := s.parseQuery(r)
	if err != nil {
		badRequest(w, "%v", err)
		return
	}
	rows, err := s.Store.Summary(r.Context(), q)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	var total int64
	for _, x := range rows {
		total += x.Events
	}
	if rows == nil {
		rows = []storage.SummaryRow{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"from": q.From, "to": q.To, "total_events": total, "categories": rows})
}

func (s *Server) series(w http.ResponseWriter, r *http.Request) {
	q, err := s.parseQuery(r)
	if err != nil {
		badRequest(w, "%v", err)
		return
	}
	pts, err := s.Store.Series(r.Context(), q)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	if pts == nil {
		pts = []storage.SeriesPoint{}
	}
	writeJSON(w, http.StatusOK, map[string]any{"points": pts})
}

// throughput estimates events/second over the last `window` (default 60s) from
// the store itself, so it works in a separate process from the processors.
//
// It is an UNDERESTIMATE of instantaneous rate: a window only appears in the
// store once the watermark closes it, so the newest MaxOutOfOrder+window seconds
// are not yet counted. For live rates scrape sf_events_processed_total and use
// rate() in Prometheus.
func (s *Server) throughput(w http.ResponseWriter, r *http.Request) {
	win := time.Minute
	if v := r.URL.Query().Get("window"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil || d < time.Second || d > 24*time.Hour {
			badRequest(w, "window: want a duration between 1s and 24h")
			return
		}
		win = d
	}
	now := s.Now()
	rows, err := s.Store.Summary(r.Context(), storage.Query{From: now.Add(-win), To: now})
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	var total int64
	for _, x := range rows {
		total += x.Events
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"window_seconds": win.Seconds(), "events": total,
		"events_per_second": float64(total) / win.Seconds(),
		"note":              "counts only closed windows; see /metrics for live rates",
	})
}

func (s *Server) late(w http.ResponseWriter, r *http.Request) {
	q, err := s.parseQuery(r)
	if err != nil {
		badRequest(w, "%v", err)
		return
	}
	ls, err := s.Store.LateSummary(r.Context(), q)
	if err != nil {
		s.storeError(w, r, err)
		return
	}
	total := ls.OnTime + ls.Late + ls.TooLate
	frac := func(n int64) float64 {
		if total == 0 {
			return 0
		}
		return float64(n) / float64(total)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"total": total, "on_time": ls.OnTime, "late_accepted": ls.Late, "dropped_too_late": ls.TooLate,
		"late_fraction": frac(ls.Late), "dropped_fraction": frac(ls.TooLate),
		"max_lateness_ms": ls.MaxLatenessMs,
	})
}

func (s *Server) ready(w http.ResponseWriter, r *http.Request) {
	// A cheap real query proves the store is reachable, not merely configured.
	if _, err := s.Store.LateSummary(r.Context(), storage.Query{From: s.Now().Add(-time.Minute)}); err != nil {
		http.Error(w, "store unavailable", http.StatusServiceUnavailable)
		return
	}
	_, _ = w.Write([]byte("ready\n"))
}

func (s *Server) metrics(w http.ResponseWriter, _ *http.Request) {
	if s.Metrics == nil {
		http.NotFound(w, nil)
		return
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
	_, _ = s.Metrics.WriteTo(w)
}
