package ingestion

import (
	"context"
	"encoding/json"
	"fmt"
	"math"
	"math/rand"
	"time"

	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/streaming"
	"github.com/BrianObonyo/streamforge/internal/tracing"
)

// GenConfig controls the synthetic workload.
type GenConfig struct {
	Rate float64 // events per second; 0 = as fast as possible
	Seed int64   // RNG seed; equal seeds give identical streams (reproducible benchmarks)

	IoTKeys, ClickKeys, MarketKeys int // cardinality per kind

	// Fault injection — each is a probability in [0,1].
	DisorderProb float64       // event arrives with an OLD event time (out-of-order)
	MaxDelay     time.Duration // upper bound of that delay
	DupProb      float64       // re-send a recently emitted message byte-for-byte
	PoisonProb   float64       // emit undecodable garbage

	Codec  events.Codec    // wire format (zero = JSON)
	Tracer *tracing.Tracer // optional: sampled events get a publish span and a traceparent

	Now func() time.Time // injectable clock; defaults to time.Now
}

func (c *GenConfig) defaults() {
	if c.IoTKeys <= 0 {
		c.IoTKeys = 50
	}
	if c.ClickKeys <= 0 {
		c.ClickKeys = 200
	}
	if c.MarketKeys <= 0 {
		c.MarketKeys = 10
	}
	if c.MaxDelay <= 0 {
		c.MaxDelay = 3 * time.Second
	}
	if c.Now == nil {
		c.Now = time.Now
	}
}

// Message is one generated record, ready to publish.
type Message struct {
	Key, Value  []byte
	Event       *events.Event // nil for poison pills
	Dup, Poison bool
	Span        *tracing.Span // non-nil when this message is part of a sampled trace; End it after publishing
}

var tickers = []string{"BTC-USD", "ETH-USD", "SOL-USD", "AAPL", "MSFT", "NVDA", "EUR-USD", "XAU-USD", "KES-USD", "TSLA"}
var pages = []string{"/home", "/search", "/product", "/cart", "/checkout", "/account"}

// Generator produces a deterministic (given Seed) mix of IoT, clickstream and
// market events, with configurable disorder, duplicates and corruption.
//
// Each source has realistic dynamics rather than uniform noise, so aggregates are
// interesting to look at:
//
//	IoT     – temperature follows a mean-reverting random walk per sensor
//	          (Ornstein–Uhlenbeck-style drift back toward a set-point).
//	Market  – price follows geometric Brownian motion, the textbook model behind
//	          Black–Scholes: dS = μS dt + σS dW, discretised per tick.
//	Click   – dwell time is log-normal, which matches real-world page-view data
//	          (a few very long sessions, many short ones).
type Generator struct {
	cfg    GenConfig
	rng    *rand.Rand
	iot    map[int]float64
	price  map[int]float64
	recent []Message // ring of recent messages to replay as duplicates
	ri     int
}

const recentRing = 256

// NewGenerator creates a generator.
func NewGenerator(cfg GenConfig) *Generator {
	cfg.defaults()
	return &Generator{
		cfg: cfg, rng: rand.New(rand.NewSource(cfg.Seed)),
		iot: map[int]float64{}, price: map[int]float64{}, recent: make([]Message, 0, recentRing),
	}
}

// Next returns the next message. Not safe for concurrent use.
func (g *Generator) Next() Message {
	// Corruption first: it replaces the event entirely.
	if g.cfg.PoisonProb > 0 && g.rng.Float64() < g.cfg.PoisonProb {
		junk := []string{`{"id": "broken", "kind":`, `not json at all`, `{"id":"x","kind":"iot","key":"k","value":"NaN-as-string"}`, ``}
		return Message{Key: []byte("poison"), Value: []byte(junk[g.rng.Intn(len(junk))]), Poison: true}
	}
	// Duplicate: replay an earlier message verbatim (same ID ⇒ dedup target).
	if g.cfg.DupProb > 0 && len(g.recent) > 0 && g.rng.Float64() < g.cfg.DupProb {
		m := g.recent[g.rng.Intn(len(g.recent))]
		m.Dup, m.Span = true, nil
		return m
	}

	now := g.cfg.Now().UTC()
	eventTime := now
	if g.cfg.DisorderProb > 0 && g.rng.Float64() < g.cfg.DisorderProb {
		// Delay is skewed toward small values (u²) with a long tail, like real networks.
		u := g.rng.Float64()
		eventTime = now.Add(-time.Duration(u * u * float64(g.cfg.MaxDelay)))
	}

	ev := events.Event{ID: g.id(), EventTime: eventTime, IngestTime: now, Attrs: map[string]string{}}
	switch g.rng.Intn(3) {
	case 0:
		i := g.rng.Intn(g.cfg.IoTKeys)
		t, ok := g.iot[i]
		if !ok {
			t = 18 + g.rng.Float64()*8 // initial 18–26 °C
		}
		t += 0.1*(22-t) + g.rng.NormFloat64()*0.4 // revert toward 22 °C + noise
		g.iot[i] = t
		ev.Kind, ev.Key, ev.Value = events.KindIoT, fmt.Sprintf("sensor-%03d", i), round(t, 3)
		ev.Attrs["unit"] = "celsius"
	case 1:
		i := g.rng.Intn(g.cfg.ClickKeys)
		ev.Kind, ev.Key = events.KindClick, fmt.Sprintf("user-%04d", i)
		ev.Value = round(math.Exp(g.rng.NormFloat64()*0.8+7.5), 1) // median ≈ 1.8 s, in ms
		ev.Attrs["page"] = pages[g.rng.Intn(len(pages))]
	default:
		i := g.rng.Intn(min(g.cfg.MarketKeys, len(tickers)))
		p, ok := g.price[i]
		if !ok {
			p = 50 + g.rng.Float64()*950
		}
		const mu, sigma, dt = 0.0001, 0.02, 1.0
		p *= math.Exp((mu-0.5*sigma*sigma)*dt + sigma*math.Sqrt(dt)*g.rng.NormFloat64())
		g.price[i] = p
		ev.Kind, ev.Key, ev.Value = events.KindMarket, tickers[i], round(p, 4)
		ev.Attrs["currency"] = "USD"
	}

	// Sampled events carry a traceparent pointing at their publish span, so the
	// processor's span becomes its child and one trace spans the whole pipeline.
	span := g.cfg.Tracer.StartRoot("publish", tracing.KindProducer)
	if span != nil {
		ev.TraceParent = span.Context().String()
		span.SetAttr("event.kind", string(ev.Kind))
		span.SetAttr("event.key", ev.Key)
	}
	b, err := events.EncodeWith(g.cfg.Codec, ev)
	if err != nil { // only an oversized field can fail; fall back so the stream never stalls
		b, _ = json.Marshal(ev)
	}
	m := Message{Key: []byte(ev.Key), Value: b, Event: &ev, Span: span}
	stored := m
	stored.Span = nil // a replayed duplicate must not reuse (or re-End) the original's span
	if len(g.recent) < recentRing {
		g.recent = append(g.recent, stored)
	} else {
		g.recent[g.ri%recentRing] = stored
		g.ri++
	}
	return m
}

// id draws from the seeded RNG so the whole stream — IDs included — is
// reproducible (events.NewID uses crypto/rand and is for production paths).
func (g *Generator) id() string { return fmt.Sprintf("%016x%016x", g.rng.Uint64(), g.rng.Uint64()) }

func round(v float64, places int) float64 {
	p := math.Pow10(places)
	return math.Round(v*p) / p
}

// Run publishes messages at cfg.Rate until ctx ends or limit messages have been
// sent (limit <= 0 means unlimited). It returns the number published.
//
// Messages are sent in batches sized to about 5 ms of traffic (1 at low rates, up
// to 500 when unthrottled), so a network broker is not hit with one round trip per
// message. The rate limiter is still applied per message, so pacing is unchanged.
func (g *Generator) Run(ctx context.Context, pub *Publisher, limit int) (int, error) {
	var limiter *backpressure.TokenBucket
	batchSize := 500
	if g.cfg.Rate > 0 {
		limiter = backpressure.NewTokenBucket(g.cfg.Rate, max(g.cfg.Rate/10, 1))
		batchSize = int(min(500, max(1, g.cfg.Rate/200)))
	}
	n := 0
	batch := make([]streaming.KV, 0, batchSize)
	spans := make([]*tracing.Span, 0, batchSize)
	flush := func() error {
		err := pub.PublishBatch(ctx, batch)
		for _, s := range spans {
			s.RecordError(err)
			s.End()
		}
		if err == nil {
			n += len(batch)
		}
		batch, spans = batch[:0], spans[:0]
		return err
	}
	for limit <= 0 || n+len(batch) < limit {
		if err := limiter.Wait(ctx, 1); err != nil {
			_ = flush()   // best effort: send what we already generated
			return n, nil // normal shutdown
		}
		m := g.Next()
		batch = append(batch, streaming.KV{Key: m.Key, Value: m.Value})
		if m.Span != nil {
			spans = append(spans, m.Span)
		}
		if len(batch) >= batchSize {
			if err := flush(); err != nil {
				if ctx.Err() != nil {
					return n, nil
				}
				return n, err
			}
		}
	}
	if err := flush(); err != nil && ctx.Err() == nil {
		return n, err
	}
	return n, nil
}
