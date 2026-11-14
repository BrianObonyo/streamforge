package processor

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/BrianObonyo/streamforge/internal/aggregation"
	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/events"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

// partitionTask owns everything about ONE partition: its queue, watermark,
// windows and dedup cache. Exactly one goroutine (run) touches agg, dedup and the
// plain fields below, which is why none of them need a mutex.
type partitionTask struct {
	p  *Processor
	tp streaming.TopicPartition

	queue  *backpressure.Queue[streaming.Record]
	agg    *aggregation.Aggregator
	dedup  *reliability.Deduper
	cancel context.CancelFunc
	done   chan struct{}

	next       int64 // goroutine-local: one past the last handled offset (-1 = none yet)
	lastLate   aggregation.LateStats
	lastLateAt time.Time

	// Published for other goroutines (commit loop, metrics scrape).
	safe     atomic.Int64 // highest safely committable offset; -1 = unknown
	open     atomic.Int64
	consumed atomic.Int64
}

func newTask(p *Processor, tp streaming.TopicPartition) *partitionTask {
	t := &partitionTask{
		p: p, tp: tp, done: make(chan struct{}), next: -1,
		agg: aggregation.New(aggregation.Config{
			Assigner: p.cfg.Assigner, MaxOutOfOrder: p.cfg.MaxOutOfOrder,
			AllowedLateness: p.cfg.AllowedLateness, IdleTimeout: p.cfg.IdleTimeout,
		}),
		dedup: reliability.NewDeduper(p.cfg.DedupTTL, p.cfg.DedupMax),
	}
	t.safe.Store(-1)

	// Backpressure wiring: at high-water, tell the BROKER to stop sending this
	// partition (so records wait safely on disk, not in our heap) and tell AIMD
	// we're congested; at low-water, reverse both.
	t.queue = backpressure.NewQueue[streaming.Record](p.cfg.QueueCapacity, p.cfg.HighWater, p.cfg.LowWater,
		func() {
			p.d.Consumer.Pause(tp)
			if p.aimd != nil {
				p.aimd.OnCongestion()
			}
			p.log.Debug("backpressure: paused", "tp", tp.String(), "depth", t.queue.Len())
		},
		func() {
			p.d.Consumer.Resume(tp)
			if p.aimd != nil {
				p.aimd.OnHealthy()
			}
			p.log.Debug("backpressure: resumed", "tp", tp.String())
		})
	return t
}

// run is the task's single goroutine: process records as they arrive, and on each
// tick emit whichever windows the watermark has closed.
func (t *partitionTask) run(ctx context.Context) {
	defer close(t.done)
	tick := time.NewTicker(t.p.cfg.TickInterval)
	defer tick.Stop()
	for {
		rec, ok, err := t.queue.Pop(ctx, tick.C)
		switch {
		case err == nil && ok:
			t.handle(ctx, rec)
		case err == nil: // tick
			t.advance(ctx)
		case errors.Is(err, backpressure.ErrClosed):
			// Graceful stop: queue fully drained. One last emit + stats flush.
			t.advance(ctx)
			t.flushLate(ctx, true)
			return
		default: // ctx cancelled: revoked or crashed — abandon without side effects
			return
		}
	}
}

// handle runs one record through the pipeline:
//
//	decode ─▶ validate ─▶ dedup ─▶ window ─▶ (outcome metrics / DLQ)
//
// Every early exit still advances t.next, so bad records never block offset
// progress — a poison pill must cost one DLQ entry, not a stuck partition.
func (t *partitionTask) handle(ctx context.Context, rec streaming.Record) {
	p := t.p
	now := p.cfg.Now()
	t.next = rec.Offset + 1
	t.consumed.Store(t.next)
	if !rec.Timestamp.IsZero() {
		p.m.latency.Observe(now.Sub(rec.Timestamp).Seconds())
	}

	ev, err := events.Decode(rec.Value)
	if err != nil {
		p.m.poison.Inc()
		t.deadLetter(ctx, rec, reliability.ReasonPoison, err)
		return
	}
	// Validation happens BEFORE the watermark sees the event. An invalid
	// (e.g. far-future) timestamp must never be allowed to drag event time forward.
	if err := ev.Validate(now); err != nil {
		p.m.invalid.Inc()
		t.deadLetter(ctx, rec, reliability.ReasonInvalid, err)
		return
	}
	if t.dedup.Seen(ev.ID, now) {
		p.m.duplicate.Inc()
		return
	}

	switch t.agg.Add(ev, rec.Offset, now) {
	case aggregation.OnTime:
		p.m.onTime.Inc()
	case aggregation.Late:
		p.m.late.Inc()
	case aggregation.TooLate:
		p.m.tooLate.Inc()
		if p.cfg.DLQTooLate {
			t.deadLetter(ctx, rec, reliability.ReasonTooLate, errors.New("event time behind watermark beyond allowed lateness"))
		}
	}
}

// advance is the emit cycle:
//
//	Collect (read-only) ─▶ write to sink with retry + circuit breaker
//	   ├─ success ─▶ Ack (mark clean, GC expired windows) ─▶ publish safe offset
//	   └─ failure ─▶ leave windows dirty; the same results are due next tick
//
// Because Collect doesn't mutate state, a failed write can never lose data; the
// cost is that the task keeps retrying every tick while the sink is down, and the
// queue behind it fills until backpressure pauses the broker.
func (t *partitionTask) advance(ctx context.Context) {
	p := t.p
	now := p.cfg.Now()
	batch := t.agg.Collect(now, t.tp.Partition)

	if len(batch.Results) > 0 {
		start := time.Now()
		err := reliability.Do(ctx, p.cfg.Retry, func() error {
			return p.breaker.Do(func() error { return p.d.Sink.WriteResults(ctx, batch.Results) })
		})
		p.m.sinkLatency.Observe(time.Since(start).Seconds())
		if err != nil {
			if ctx.Err() == nil {
				p.m.sinkErrors.Inc()
				p.log.Warn("sink write failed; will retry next tick", "tp", t.tp.String(),
					"rows", len(batch.Results), "breaker", p.breaker.State(), "err", err)
			}
			return
		}
		for _, r := range batch.Results {
			if r.Update {
				p.m.emittedUpdates.Inc()
			} else {
				p.m.emitted.Inc()
			}
		}
	}

	t.agg.Ack(batch)
	t.open.Store(int64(t.agg.OpenWindows()))
	if t.next >= 0 {
		t.safe.Store(t.agg.SafeOffset(t.next))
	}
	t.flushLate(ctx, false)
}

// flushLate persists the DELTA of late-arrival counters since the last flush, at
// most once a second. Deltas (not totals) make the rows summable across
// partitions, restarts and time ranges in SQL.
func (t *partitionTask) flushLate(ctx context.Context, force bool) {
	now := t.p.cfg.Now()
	if !force && now.Sub(t.lastLateAt) < time.Second {
		return
	}
	cur := t.agg.LateStats()
	d := storage.LateStatRow{
		TS: now, Partition: t.tp.Partition,
		OnTime: cur.OnTime - t.lastLate.OnTime, Late: cur.Late - t.lastLate.Late,
		TooLate: cur.TooLate - t.lastLate.TooLate, MaxLatenessMs: cur.MaxLateness.Milliseconds(),
	}
	t.lastLateAt = now
	if d.OnTime == 0 && d.Late == 0 && d.TooLate == 0 {
		return
	}
	if err := t.p.d.Sink.WriteLateStats(ctx, []storage.LateStatRow{d}); err != nil {
		return // keep lastLate unchanged: the delta is retried (and grows) next time
	}
	t.lastLate = cur
}

// deadLetter parks an unprocessable record in the DLQ. A DLQ failure is logged and
// counted but never blocks the partition: losing a dead letter is preferable to
// halting the pipeline behind it.
func (t *partitionTask) deadLetter(ctx context.Context, rec streaming.Record, reason string, cause error) {
	p := t.p
	d := reliability.DeadLetter{
		Reason: reason, Error: cause.Error(), Topic: rec.Topic, Partition: rec.Partition,
		Offset: rec.Offset, Key: rec.Key, Value: rec.Value, FailedAt: p.cfg.Now().UTC(),
	}
	if err := p.d.DLQ.Send(ctx, d); err != nil {
		p.m.dlqErrors.Inc()
		p.log.Warn("dead-letter send failed", "reason", reason, "err", err)
		return
	}
	p.m.dlqSent.Inc()
}
