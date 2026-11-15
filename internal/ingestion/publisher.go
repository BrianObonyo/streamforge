// Package ingestion is the front door of StreamForge: it turns external input
// (a simulation, or HTTP requests) into records on the broker.
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package ingestion

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/streaming"
	"github.com/BrianObonyo/streamforge/internal/tracing"
)

// Publisher encodes events and produces them to one topic, retrying transient
// broker errors with jittered exponential backoff.
//
// The event's Key is used as the record key, so all events for one sensor / user /
// ticker land on one partition and are therefore processed in order by one
// consumer — the locality that per-key windowing depends on.
type Publisher struct {
	P      streaming.Producer
	Topic  string
	Retry  reliability.RetryPolicy
	Codec  events.Codec    // wire format; zero value = JSON
	Tracer *tracing.Tracer // optional; nil disables tracing

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

// Publish stamps IngestTime, continues (or starts) a trace, encodes and produces
// one event.
//
// Trace handling: if the event already carries a sampled traceparent (a client
// propagating its own trace) we become a child of it; if it carries none we may
// start a new sampled trace at the configured ratio; if it carries an UNSAMPLED
// one we honour that and create nothing. Either way the traceparent written into
// the event points at OUR span, so the processor's span becomes its child.
func (p *Publisher) Publish(ctx context.Context, ev events.Event) error {
	if ev.IngestTime.IsZero() {
		ev.IngestTime = time.Now().UTC()
	}
	span := p.Tracer.StartFromTraceparent(ev.TraceParent, "publish "+p.Topic, tracing.KindProducer)
	if span == nil && ev.TraceParent == "" {
		span = p.Tracer.StartRoot("publish "+p.Topic, tracing.KindProducer)
	}
	if span != nil {
		ev.TraceParent = span.Context().String()
		span.SetAttr("messaging.destination.name", p.Topic)
		span.SetAttr("event.kind", string(ev.Kind))
		defer span.End()
	}
	b, err := events.EncodeWith(p.Codec, ev)
	if err != nil {
		span.RecordError(err)
		return reliability.Permanent(fmt.Errorf("encode: %w", err))
	}
	err = p.PublishRaw(ctx, []byte(ev.Key), b)
	span.RecordError(err)
	return err
}

// permanentBrokerErr reports errors no retry can fix.
func permanentBrokerErr(err error) bool {
	return errors.Is(err, streaming.ErrUnknownTopic) || errors.Is(err, streaming.ErrClosed)
}

// PublishRaw produces pre-encoded bytes. The generator uses it to inject
// duplicates (identical bytes) and poison pills (garbage bytes).
func (p *Publisher) PublishRaw(ctx context.Context, key, value []byte) error {
	return p.PublishBatch(ctx, []streaming.KV{{Key: key, Value: value}})
}

// PublishBatch produces several messages with ONE broker call where the broker
// supports it, retrying transient errors with jittered backoff. A retried batch can
// duplicate messages that had already been written before the failure; that is
// safe because the processor deduplicates by event ID (at-least-once publishing).
func (p *Publisher) PublishBatch(ctx context.Context, msgs []streaming.KV) error {
	if len(msgs) == 0 {
		return nil
	}
	attempt := 0
	err := reliability.Do(ctx, p.Retry, func() error {
		if attempt++; attempt > 1 {
			p.retries.Inc()
		}
		if err := streaming.ProduceBatch(ctx, p.P, p.Topic, msgs); err != nil {
			if permanentBrokerErr(err) {
				return reliability.Permanent(err)
			}
			return err
		}
		return nil
	})
	if err != nil {
		p.failed.Add(uint64(len(msgs)))
		return fmt.Errorf("publish to %q: %w", p.Topic, err)
	}
	p.sent.Add(uint64(len(msgs)))
	return nil
}
