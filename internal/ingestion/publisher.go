// Package ingestion is the front door of StreamForge: it turns external input
// (a simulation, or HTTP requests) into records on the broker.
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package ingestion

import (
	"context"
	"fmt"
	"time"

	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

// Publisher encodes events and produces them to one topic, retrying transient
// broker errors with jittered exponential backoff.
//
// The event's Key is used as the record key, so all events for one sensor / user /
// ticker land on one partition and are therefore processed in order by one
// consumer — the locality that per-key windowing depends on.
type Publisher struct {
	P     streaming.Producer
	Topic string
	Retry reliability.RetryPolicy

	sent, failed *metrics.Counter
	retries      *metrics.Counter
}

// NewPublisher builds a Publisher; reg may be nil.
func NewPublisher(p streaming.Producer, topic string, retry reliability.RetryPolicy, reg *metrics.Registry) *Publisher {
	if reg == nil {
		reg = metrics.NewRegistry()
	}
	return &Publisher{
		P: p, Topic: topic, Retry: retry,
		sent:    reg.Counter("sf_ingest_published_total", "Events published to the broker.", nil),
		failed:  reg.Counter("sf_ingest_failed_total", "Events that failed to publish after retries.", nil),
		retries: reg.Counter("sf_ingest_retries_total", "Publish attempts beyond the first.", nil),
	}
}

// Publish stamps IngestTime (if unset), encodes and produces one event.
func (p *Publisher) Publish(ctx context.Context, ev events.Event) error {
	if ev.IngestTime.IsZero() {
		ev.IngestTime = time.Now().UTC()
	}
	b, err := events.Encode(ev)
	if err != nil {
		return reliability.Permanent(fmt.Errorf("encode: %w", err))
	}
	return p.PublishRaw(ctx, []byte(ev.Key), b)
}

// PublishRaw produces pre-encoded bytes. The generator uses it to inject
// duplicates (identical bytes) and poison pills (garbage bytes).
func (p *Publisher) PublishRaw(ctx context.Context, key, value []byte) error {
	attempt := 0
	err := reliability.Do(ctx, p.Retry, func() error {
		if attempt++; attempt > 1 {
			p.retries.Inc()
		}
		_, _, err := p.P.Produce(ctx, p.Topic, key, value)
		return err
	})
	if err != nil {
		p.failed.Inc()
		return err
	}
	p.sent.Inc()
	return nil
}
