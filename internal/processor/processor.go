package processor

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

// Deps are the collaborators a Processor needs.
type Deps struct {
	Consumer streaming.Consumer
	Sink     storage.Sink
	DLQ      reliability.DLQ   // optional (defaults to discarding)
	Metrics  *metrics.Registry // optional
	Logger   *slog.Logger      // optional
}

// Processor is one consumer-group member. Create with New, run with Run.
type Processor struct {
	cfg Config
	d   Deps
	log *slog.Logger
	m   *pm

	// Touched ONLY by the Run goroutine — hence no lock.
	tasks      map[streaming.TopicPartition]*partitionTask
	committed  map[streaming.TopicPartition]int64
	lastCommit time.Time

	// Shared with task goroutines (all internally synchronised).
	queues   sync.Map // tp -> *backpressure.Queue[streaming.Record], read by scrape-time gauges
	throttle *backpressure.TokenBucket
	aimd     *backpressure.AIMD
	breaker  *reliability.CircuitBreaker

	taskCtx    context.Context
	taskCancel context.CancelFunc
}

// New validates cfg and wires up a Processor.
func New(cfg Config, d Deps) (*Processor, error) {
	if err := cfg.ApplyDefaults(); err != nil {
		return nil, err
	}
	if d.Consumer == nil || d.Sink == nil {
		return nil, fmt.Errorf("processor: Consumer and Sink are required")
	}
	if d.DLQ == nil {
		d.DLQ = reliability.NopDLQ{}
	}
	if d.Logger == nil {
		d.Logger = slog.New(slog.NewTextHandler(discard{}, nil))
	}
	p := &Processor{
		cfg: cfg, d: d, log: d.Logger.With("member", cfg.MemberID, "group", cfg.Group),
		m:         newMetrics(d.Metrics),
		tasks:     map[streaming.TopicPartition]*partitionTask{},
		committed: map[streaming.TopicPartition]int64{},
		breaker:   reliability.NewCircuitBreaker(cfg.BreakerThreshold, cfg.BreakerOpenFor),
	}
	if cfg.ThrottleCeiling > 0 {
		p.throttle = backpressure.NewTokenBucket(cfg.ThrottleCeiling, cfg.ThrottleCeiling/10)
		p.aimd = backpressure.NewAIMD(p.throttle, cfg.ThrottleCeiling/100, cfg.ThrottleCeiling)
	}
	lbl := metrics.Labels{"member": cfg.MemberID}
	p.m.reg.GaugeFunc("sf_paused_partitions", "Partitions currently paused by backpressure.", lbl, func() float64 {
		n := 0
		p.queues.Range(func(_, v any) bool {
			if v.(*backpressure.Queue[streaming.Record]).Saturated() {
				n++
			}
			return true
		})
		return float64(n)
	})
	p.m.reg.GaugeFunc("sf_throttle_rate", "Current AIMD-controlled consume rate (events/sec; 0 = unlimited).", lbl,
		func() float64 { return p.throttle.Rate() })
	return p, nil
}

type discard struct{}

func (discard) Write(b []byte) (int, error) { return len(b), nil }

// Run consumes until ctx is cancelled (graceful: drains queues, flushes, commits)
// or the consumer is closed underneath it (crash: abandon work immediately, as a
// killed process would). It returns nil on graceful stop and the error otherwise.
func (p *Processor) Run(ctx context.Context) (err error) {
	p.taskCtx, p.taskCancel = context.WithCancel(context.Background())
	defer p.taskCancel()

	graceful := true
	defer func() { p.shutdown(graceful) }()

	p.log.Info("processor started", "topic", p.cfg.Topic, "window", p.cfg.Assigner.Name(),
		"max_ooo", p.cfg.MaxOutOfOrder, "lateness", p.cfg.AllowedLateness)

	for {
		if ctx.Err() != nil {
			return nil
		}
		p.reconcile()

		pollCtx, cancel := context.WithTimeout(ctx, p.cfg.PollTimeout)
		recs, perr := p.d.Consumer.Poll(pollCtx, p.cfg.PollBatch)
		cancel()
		switch {
		case perr == nil:
		case ctx.Err() != nil:
			return nil
		case errors.Is(perr, context.DeadlineExceeded):
			// Idle poll: fall through to housekeeping.
		case errors.Is(perr, streaming.ErrClosed):
			graceful = false
			return perr
		default:
			p.log.Warn("poll failed", "err", perr)
			select {
			case <-time.After(100 * time.Millisecond):
			case <-ctx.Done():
				return nil
			}
		}

		if len(recs) > 0 {
			// Re-reconcile before dispatching: a rebalance may have handed us a
			// partition DURING Poll. Dropping its first records because no task
			// existed yet would silently skip them (the broker already advanced
			// our read position).
			p.reconcile()
			for _, r := range recs {
				if !p.dispatch(ctx, r) {
					return nil
				}
			}
		}
		p.maybeCommit(ctx)
	}
}

// dispatch hands one record to its partition's queue. It returns false only when
// ctx has ended.
func (p *Processor) dispatch(ctx context.Context, r streaming.Record) bool {
	t := p.tasks[r.TP()]
	if t == nil {
		// Partition was revoked between fetch and dispatch. Its new owner will
		// re-read from the committed offset, so dropping here loses nothing.
		return true
	}
	if err := p.throttle.Wait(ctx, 1); err != nil {
		return false
	}
	// Put blocks when the queue is full — that stall IS the backpressure.
	// (Pause normally engages at high-water long before the queue is full.)
	return t.queue.Put(ctx, r) == nil
}

// reconcile aligns running tasks with the consumer's current assignment.
func (p *Processor) reconcile() {
	want := map[streaming.TopicPartition]struct{}{}
	for _, tp := range p.d.Consumer.Assignment() {
		if tp.Topic == p.cfg.Topic {
			want[tp] = struct{}{}
		}
	}
	changed := false
	for tp, t := range p.tasks {
		if _, ok := want[tp]; !ok {
			p.revoke(tp, t)
			changed = true
		}
	}
	for tp := range want {
		if p.tasks[tp] == nil {
			p.assign(tp)
			changed = true
		}
	}
	if changed {
		p.m.rebalances.Inc()
		p.log.Info("assignment changed", "partitions", len(p.tasks))
	}
}

func (p *Processor) assign(tp streaming.TopicPartition) {
	t := newTask(p, tp)
	p.tasks[tp] = t
	p.queues.Store(tp, t.queue)

	lbl := metrics.Labels{"member": p.cfg.MemberID, "partition": strconv.Itoa(tp.Partition)}
	p.m.reg.GaugeFunc("sf_queue_depth", "Records waiting in a partition queue.", lbl, func() float64 { return float64(t.queue.Len()) })
	p.m.reg.GaugeFunc("sf_open_windows", "Windows held in memory for a partition.", lbl, func() float64 { return float64(t.open.Load()) })
	p.m.reg.GaugeFunc("sf_consumed_offset", "Next offset to be handled for a partition.", lbl, func() float64 { return float64(t.consumed.Load()) })

	ctx, cancel := context.WithCancel(p.taskCtx)
	t.cancel = cancel
	go t.run(ctx)
	p.log.Debug("partition assigned", "tp", tp.String())
}

// revoke stops a task whose partition now belongs to someone else.
//
// We deliberately do NOT try to flush or commit: the broker has already
// reassigned the partition, so a commit would be fenced (and would race the new
// owner's progress). The new owner resumes from the last committed offset and
// rebuilds the window state by replay — the design the safe-offset rule exists for.
func (p *Processor) revoke(tp streaming.TopicPartition, t *partitionTask) {
	t.cancel()
	<-t.done
	delete(p.tasks, tp)
	delete(p.committed, tp)
	p.queues.Delete(tp)
	lbl := metrics.Labels{"member": p.cfg.MemberID, "partition": strconv.Itoa(tp.Partition)}
	for _, n := range []string{"sf_queue_depth", "sf_open_windows", "sf_consumed_offset"} {
		p.m.reg.Unregister(n, lbl)
	}
	p.log.Debug("partition revoked", "tp", tp.String())
}

// maybeCommit commits each partition's safe offset if it advanced.
func (p *Processor) maybeCommit(ctx context.Context) {
	now := p.cfg.Now()
	if now.Sub(p.lastCommit) < p.cfg.CommitInterval {
		return
	}
	p.lastCommit = now
	p.commit(ctx)
}

func (p *Processor) commit(ctx context.Context) {
	offs := map[streaming.TopicPartition]int64{}
	for tp, t := range p.tasks {
		if s := t.safe.Load(); s >= 0 {
			if prev, ok := p.committed[tp]; !ok || s > prev {
				offs[tp] = s
			}
		}
	}
	if len(offs) == 0 {
		return
	}
	if err := p.d.Consumer.Commit(ctx, offs); err != nil {
		// ErrNotAssigned just means a rebalance raced us; next loop reconciles.
		if !errors.Is(err, streaming.ErrClosed) && ctx.Err() == nil {
			p.m.commitErrors.Inc()
			p.log.Warn("commit failed", "err", err)
		}
		return
	}
	for tp, o := range offs {
		p.committed[tp] = o
	}
	p.m.commits.Inc()
}

// shutdown stops every task. Graceful: close queues so tasks drain what is
// already buffered, give them a bounded time, then commit. Crash: cancel at once.
func (p *Processor) shutdown(graceful bool) {
	if graceful {
		for _, t := range p.tasks {
			t.queue.Close() // safe: the Run goroutine (the only Put-er) has returned
		}
		deadline := time.After(5 * time.Second)
	wait:
		for _, t := range p.tasks {
			select {
			case <-t.done:
			case <-deadline:
				break wait
			}
		}
	}
	p.taskCancel()
	for _, t := range p.tasks {
		<-t.done
	}
	if graceful {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		p.commit(ctx)
		cancel()
	}
	p.log.Info("processor stopped", "graceful", graceful)
}

// Pauses returns how many times backpressure has paused a (still-owned)
// partition since it was assigned. Useful for asserting that overload handling
// actually engaged.
func (p *Processor) Pauses() uint64 {
	var n uint64
	p.queues.Range(func(_, v any) bool {
		n += v.(*backpressure.Queue[streaming.Record]).Pauses()
		return true
	})
	return n
}

// Snapshot is a point-in-time copy of the headline counters.
type Snapshot struct {
	OnTime, Late, TooLate, Duplicates, Invalid, Poison uint64
	WindowRows, LateUpdateRows, SinkErrors, Commits    uint64
	P50, P95, P99                                      float64 // seconds
}

// Stats returns a Snapshot.
func (p *Processor) Stats() Snapshot {
	return Snapshot{
		OnTime: p.m.onTime.Value(), Late: p.m.late.Value(), TooLate: p.m.tooLate.Value(),
		Duplicates: p.m.duplicate.Value(), Invalid: p.m.invalid.Value(), Poison: p.m.poison.Value(),
		WindowRows: p.m.emitted.Value(), LateUpdateRows: p.m.emittedUpdates.Value(),
		SinkErrors: p.m.sinkErrors.Value(), Commits: p.m.commits.Value(),
		P50: p.m.latency.Quantile(0.5), P95: p.m.latency.Quantile(0.95), P99: p.m.latency.Quantile(0.99),
	}
}
