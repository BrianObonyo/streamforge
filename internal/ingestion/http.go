package ingestion

import (
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"time"

	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/tracing"
)

const (
	maxBodyBytes = 1 << 20 // 1 MiB: stops a single request from exhausting memory
	maxBatch     = 1000    // events per request
)

// Handler is the HTTP ingestion API:
//
//	POST /v1/events   body: one Event object OR a JSON array of them
//
// Responses: 202 all accepted · 207 partially accepted (per-item errors listed)
// · 400 nothing valid · 413 too large · 429 over rate limit (with Retry-After) ·
// 502 broker unavailable.
//
// 429 is how backpressure reaches EXTERNAL clients: rather than queueing without
// bound, we tell fast producers to slow down.
type Handler struct {
	Pub     *Publisher
	Limiter *backpressure.TokenBucket // optional global rate limit (events/sec)
	Now     func() time.Time
}

type rejection struct {
	Index int    `json:"index"`
	Error string `json:"error"`
}

type response struct {
	Accepted int         `json:"accepted"`
	Rejected []rejection `json:"rejected,omitempty"`
}

func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	now := time.Now
	if h.Now != nil {
		now = h.Now
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		var mbe *http.MaxBytesError
		if errors.As(err, &mbe) {
			http.Error(w, "request body too large", http.StatusRequestEntityTooLarge)
			return
		}
		http.Error(w, "cannot read body", http.StatusBadRequest)
		return
	}

	// Accept either a single object or an array.
	var batch []events.Event
	if trimmed := firstNonSpace(body); trimmed == '[' {
		if err := json.Unmarshal(body, &batch); err != nil {
			http.Error(w, "invalid JSON array: "+err.Error(), http.StatusBadRequest)
			return
		}
	} else {
		var one events.Event
		if err := json.Unmarshal(body, &one); err != nil {
			http.Error(w, "invalid JSON: "+err.Error(), http.StatusBadRequest)
			return
		}
		batch = []events.Event{one}
	}
	if len(batch) == 0 || len(batch) > maxBatch {
		http.Error(w, "batch must contain 1..1000 events", http.StatusBadRequest)
		return
	}

	// Admission control BEFORE doing any work for the batch.
	if !h.Limiter.Allow(float64(len(batch))) {
		w.Header().Set("Retry-After", "1")
		http.Error(w, "rate limit exceeded", http.StatusTooManyRequests)
		return
	}

	// Trace propagation: a W3C `traceparent` header applies to events that don't
	// carry their own. Malformed values are silently dropped (never a 400): a bad
	// tracing header must not cost a client its data.
	hdrTP := r.Header.Get("traceparent")

	var resp response
	for i := range batch {
		ev := batch[i]
		if ev.TraceParent == "" {
			ev.TraceParent = hdrTP
		}
		if _, ok := tracing.ParseTraceparent(ev.TraceParent); !ok {
			ev.TraceParent = ""
		}
		if ev.ID == "" {
			ev.ID = events.NewID() // server-assigned ids are fine, but clients should send their own for dedup
		}
		ev.IngestTime = now().UTC()
		if err := ev.Validate(now()); err != nil {
			resp.Rejected = append(resp.Rejected, rejection{i, err.Error()})
			continue
		}
		if err := h.Pub.Publish(r.Context(), ev); err != nil {
			http.Error(w, "broker unavailable: "+err.Error(), http.StatusBadGateway)
			return
		}
		resp.Accepted++
	}

	status := http.StatusAccepted
	switch {
	case resp.Accepted == 0:
		status = http.StatusBadRequest
	case len(resp.Rejected) > 0:
		status = http.StatusMultiStatus
	}
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(resp)
}

func firstNonSpace(b []byte) byte {
	for _, c := range b {
		if c != ' ' && c != '\n' && c != '\t' && c != '\r' {
			return c
		}
	}
	return 0
}
