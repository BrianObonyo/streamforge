// Package events defines the canonical event model that flows through
// StreamForge, plus its wire encoding and validation rules.
//
// Design note: every event carries TWO timestamps.
//
//   - EventTime  – when the thing actually happened at the source
//     (sensor reading taken, button clicked, trade executed).
//   - IngestTime – when StreamForge first saw it.
//
// The gap between them is exactly what makes stream processing hard: networks
// delay, batch and reorder data. All windowing in StreamForge is driven by
// EventTime; IngestTime exists so we can measure how late data really is.
// (The event-time / processing-time distinction is from Akidau et al.,
// "The Dataflow Model", VLDB 2015.)
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package events

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"time"
)

// Kind identifies one of the three simulated workload categories.
type Kind string

const (
	KindIoT    Kind = "iot"         // sensor telemetry
	KindClick  Kind = "clickstream" // application click events
	KindMarket Kind = "market"      // financial tick data
)

// AllKinds lists every supported kind in a stable order.
var AllKinds = []Kind{KindIoT, KindClick, KindMarket}

// Valid reports whether k is a supported kind.
func (k Kind) Valid() bool {
	switch k {
	case KindIoT, KindClick, KindMarket:
		return true
	}
	return false
}

// MaxFutureSkew is how far ahead of the wall clock an EventTime may be before
// we treat it as a producer clock bug and reject it. Future-dated events are
// dangerous: a single one can drag a watermark far ahead and cause every
// legitimate event after it to be classified as "late".
const MaxFutureSkew = 5 * time.Minute

// Event is the unit of data moving through the pipeline.
type Event struct {
	ID         string            `json:"id"`                    // globally unique; basis for dedup
	Kind       Kind              `json:"kind"`                  // workload category
	Key        string            `json:"key"`                   // partition key: sensor id / user id / ticker
	Value      float64           `json:"value"`                 // numeric measure aggregated per window
	EventTime  time.Time         `json:"event_time"`            // source-side timestamp
	IngestTime time.Time         `json:"ingest_time,omitempty"` // set by the ingestion layer
	Attrs      map[string]string `json:"attrs,omitempty"`       // free-form dimensions
}

// ErrInvalid wraps every validation failure so callers can use errors.Is.
var ErrInvalid = errors.New("invalid event")

// Validate checks structural invariants. now is passed in (rather than read
// from the clock) so validation stays deterministic under test.
func (e *Event) Validate(now time.Time) error {
	switch {
	case e.ID == "":
		return fmt.Errorf("%w: missing id", ErrInvalid)
	case !e.Kind.Valid():
		return fmt.Errorf("%w: unknown kind %q", ErrInvalid, e.Kind)
	case e.Key == "":
		return fmt.Errorf("%w: missing key", ErrInvalid)
	case math.IsNaN(e.Value) || math.IsInf(e.Value, 0):
		// NaN poisons Sum/Min/Max permanently, so it must never enter a window.
		return fmt.Errorf("%w: non-finite value", ErrInvalid)
	case e.EventTime.IsZero():
		return fmt.Errorf("%w: missing event_time", ErrInvalid)
	case e.EventTime.After(now.Add(MaxFutureSkew)):
		return fmt.Errorf("%w: event_time %s is too far in the future", ErrInvalid, e.EventTime.Format(time.RFC3339))
	}
	return nil
}

// Encode serialises an event to its JSON wire format.
func Encode(e Event) ([]byte, error) { return json.Marshal(e) }

// Decode parses the wire format. It does NOT validate; callers decide whether a
// decodable-but-invalid event is a poison pill (processors) or a 400 (HTTP).
func Decode(b []byte) (Event, error) {
	var e Event
	if err := json.Unmarshal(b, &e); err != nil {
		return Event{}, fmt.Errorf("decode event: %w", err)
	}
	return e, nil
}

// NewID returns a random 128-bit identifier as 32 hex characters.
// crypto/rand is used (not math/rand) so IDs are collision-safe across many
// independent producers without coordination.
func NewID() string {
	var b [16]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic("events: crypto/rand unavailable: " + err.Error()) // cannot happen on supported OSes
	}
	return hex.EncodeToString(b[:])
}
