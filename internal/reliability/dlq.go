package reliability

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"github.com/BrianObonyo/streamforge/internal/streaming"
)

// Dead-letter reasons. Kept as constants so dashboards can group by them.
const (
	ReasonPoison   = "poison_undecodable" // bytes were not valid JSON
	ReasonInvalid  = "invalid_event"      // decoded but failed validation
	ReasonTooLate  = "too_late"           // beyond allowed lateness
	ReasonSinkFail = "sink_failure"       // retries exhausted
)

// DeadLetter is what lands in the DLQ: the original bytes untouched, plus enough
// context to understand and replay the failure later.
type DeadLetter struct {
	Reason    string    `json:"reason"`
	Error     string    `json:"error,omitempty"`
	Topic     string    `json:"topic"`
	Partition int       `json:"partition"`
	Offset    int64     `json:"offset"`
	Key       []byte    `json:"key,omitempty"`   // base64 in JSON
	Value     []byte    `json:"value,omitempty"` // original payload, byte-for-byte
	FailedAt  time.Time `json:"failed_at"`
}

// DLQ receives messages the pipeline cannot or should not process. Parking them
// (rather than crashing, or silently dropping) keeps one poison message from
// blocking a partition while still making every loss auditable.
type DLQ interface {
	Send(ctx context.Context, d DeadLetter) error
}

// BrokerDLQ publishes dead letters as JSON to a broker topic.
type BrokerDLQ struct {
	P     streaming.Producer
	Topic string
}

// Send implements DLQ. The partition key is the source partition so dead letters
// from one partition stay ordered relative to each other.
func (b *BrokerDLQ) Send(ctx context.Context, d DeadLetter) error {
	payload, err := json.Marshal(d)
	if err != nil {
		return err
	}
	_, _, err = b.P.Produce(ctx, b.Topic, []byte(d.Topic+"/"+itoa(d.Partition)), payload)
	return err
}

// MemDLQ collects dead letters in memory (tests and the demo).
type MemDLQ struct {
	mu    sync.Mutex
	items []DeadLetter
}

func (m *MemDLQ) Send(_ context.Context, d DeadLetter) error {
	m.mu.Lock()
	m.items = append(m.items, d)
	m.mu.Unlock()
	return nil
}

// Items returns a snapshot.
func (m *MemDLQ) Items() []DeadLetter {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]DeadLetter(nil), m.items...)
}

// CountByReason tallies the DLQ contents.
func (m *MemDLQ) CountByReason() map[string]int {
	m.mu.Lock()
	defer m.mu.Unlock()
	out := map[string]int{}
	for _, d := range m.items {
		out[d.Reason]++
	}
	return out
}

// NopDLQ discards everything; used when no DLQ is configured.
type NopDLQ struct{}

func (NopDLQ) Send(context.Context, DeadLetter) error { return nil }

func itoa(n int) string {
	b, _ := json.Marshal(n)
	return string(b)
}
