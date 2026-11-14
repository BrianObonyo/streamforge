// Command demo runs the ENTIRE StreamForge pipeline in one process — generator,
// broker, several processors, store and analytics API — and proves, live, that
// the results survive chaos.
//
//	go run ./cmd/demo                 # 20 s run, kills a member mid-stream
//	go run ./cmd/demo -duration 60s -rate 20000 -members 3
//
// It injects out-of-order events, duplicates and garbage, optionally hard-kills a
// processor partway through (no flush, no commit — like SIGKILL) and later starts
// a replacement. At the end it compares the number of unique events produced with
// the number counted in the store. They must match exactly.
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"sync"
	"syscall"
	"time"

	"github.com/BrianObonyo/streamforge/internal/api"
	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/config"
	"github.com/BrianObonyo/streamforge/internal/ingestion"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/processor"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

const topic = "events"

func main() {
	var cfg processor.Config
	processor.RegisterFlags(flag.CommandLine, &cfg)
	var (
		duration   = flag.Duration("duration", 20*time.Second, "how long to generate events")
		rate       = flag.Float64("rate", 3000, "events/sec")
		partitions = flag.Int("partitions", 6, "topic partitions")
		members    = flag.Int("members", 2, "initial processors in the group")
		killAt     = flag.Duration("kill-at", 8*time.Second, "hard-kill one member at this offset (0 = never)")
		reviveAt   = flag.Duration("revive-at", 13*time.Second, "start a replacement member at this offset (0 = never)")
		disorder   = flag.Float64("disorder", 0.25, "out-of-order probability")
		maxDelay   = flag.Duration("max-delay", 1500*time.Millisecond, "max out-of-order delay")
		dup        = flag.Float64("dup", 0.05, "duplicate probability")
		poison     = flag.Float64("poison", 0.02, "garbage-message probability")
		seed       = flag.Int64("seed", 42, "RNG seed")
		httpAddr   = flag.String("http", ":8080", "analytics API + /metrics address (empty = off)")
		storeKind  = flag.String("store", "memory", "memory|clickhouse")
		chURL      = flag.String("clickhouse-url", "http://localhost:8123", "ClickHouse URL (when -store clickhouse)")
		logLevel   = flag.String("log-level", "warn", "debug|info|warn|error")
	)
	flag.Parse()
	cfg.Topic, cfg.Group = topic, "demo"
	if cfg.WindowSize == 10*time.Second { // the production default is too coarse for a short demo
		cfg.WindowSize = time.Second
	}
	cfg.MaxOutOfOrder = *maxDelay + 500*time.Millisecond // slack just above the worst injected delay
	cfg.CommitInterval = 200 * time.Millisecond
	log := config.Logger(*logLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	die := func(err error) {
		if err != nil {
			fmt.Fprintln(os.Stderr, "fatal:", err)
			os.Exit(1)
		}
	}

	// ---- infrastructure
	broker := streaming.NewMemBroker()
	die(broker.CreateTopic(ctx, topic, *partitions))
	store, err := storage.Open(ctx, *storeKind, storage.CHConfig{URL: *chURL})
	die(err)
	reg := metrics.NewRegistry()
	reg.GaugeFunc("sf_consumer_lag", "Records the group has yet to commit past.", nil, func() float64 {
		var n int64
		for _, v := range broker.Lag(cfg.Group, topic) {
			n += v
		}
		return float64(n)
	})
	dlq := &reliability.MemDLQ{}

	// ---- processors
	type member struct {
		id string
		c  streaming.Consumer
		ch chan error
	}
	var (
		mu    sync.Mutex
		alive = map[string]*member{}
		first *processor.Processor
	)
	runCtx, stopMembers := context.WithCancel(context.Background())
	start := func(id string) {
		c, err := broker.NewConsumer(cfg.Group, id, topic)
		die(err)
		mc := cfg
		mc.MemberID = id
		p, err := processor.New(mc, processor.Deps{Consumer: c, Sink: store, DLQ: dlq, Metrics: reg, Logger: log})
		die(err)
		m := &member{id: id, c: c, ch: make(chan error, 1)}
		mu.Lock()
		alive[id] = m
		if first == nil {
			first = p // counters live in the shared registry, so any member reports the totals
		}
		mu.Unlock()
		go func() { m.ch <- p.Run(runCtx) }()
	}
	for i := 1; i <= *members; i++ {
		start(fmt.Sprintf("proc-%d", i))
	}

	// ---- analytics API
	if *httpAddr != "" {
		srv := &http.Server{Addr: *httpAddr, ReadHeaderTimeout: 5 * time.Second,
			Handler: (&api.Server{Store: store, Metrics: reg, Log: log}).Handler()}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				fmt.Fprintln(os.Stderr, "http:", err)
			}
		}()
		defer srv.Close()
		fmt.Printf("analytics API:  http://localhost%s/v1/summary   metrics: http://localhost%s/metrics\n", *httpAddr, *httpAddr)
	}

	fmt.Printf("window=%s max-ooo=%s lateness=%s partitions=%d members=%d rate=%.0f/s\n\n",
		cfg.WindowSize, cfg.MaxOutOfOrder, cfg.AllowedLateness, *partitions, *members, *rate)

	// ---- producer
	prod, _ := broker.NewProducer(partitioning.Hash{})
	pub := ingestion.NewPublisher(prod, topic, reliability.DefaultRetry, reg)
	gen := ingestion.NewGenerator(ingestion.GenConfig{Rate: *rate, Seed: *seed,
		DisorderProb: *disorder, MaxDelay: *maxDelay, DupProb: *dup, PoisonProb: *poison})
	limiter := backpressure.NewTokenBucket(*rate, max(*rate/10, 1))

	var produced, unique, dups, poisoned int64
	var pmu sync.Mutex
	prodDone := make(chan struct{})
	go func() {
		defer close(prodDone)
		deadline := time.Now().Add(*duration)
		for time.Now().Before(deadline) && ctx.Err() == nil {
			if limiter.Wait(ctx, 1) != nil {
				return
			}
			m := gen.Next()
			if pub.PublishRaw(ctx, m.Key, m.Value) != nil {
				return
			}
			pmu.Lock()
			produced++
			switch {
			case m.Poison:
				poisoned++
			case m.Dup:
				dups++
			default:
				unique++
			}
			pmu.Unlock()
		}
	}()

	// ---- chaos timeline + live stats
	t0 := time.Now()
	killed, revived := *killAt == 0, *reviveAt == 0
	tick := time.NewTicker(time.Second)
	defer tick.Stop()
	fmt.Printf("%-5s %9s %9s %7s %7s %9s %8s %9s  %s\n", "t", "produced", "handled", "dupes", "poison", "windows", "lag", "p95", "members")
loop:
	for {
		select {
		case <-prodDone:
			break loop
		case <-ctx.Done():
			break loop
		case <-tick.C:
			el := time.Since(t0)
			if !killed && el >= *killAt {
				killed = true
				mu.Lock()
				victim := alive["proc-1"]
				delete(alive, "proc-1")
				mu.Unlock()
				_ = victim.c.Close() // hard kill: no flush, no commit
				fmt.Println("      >>> KILLED proc-1 (hard crash: in-memory windows lost; survivors resume from committed offsets)")
			}
			if !revived && el >= *reviveAt {
				revived = true
				start("proc-replacement")
				fmt.Println("      >>> STARTED proc-replacement (group rebalances; partitions move again)")
			}
			mu.Lock()
			st, n := first.Stats(), len(alive)
			mu.Unlock()
			pmu.Lock()
			pr := produced
			pmu.Unlock()
			var lag int64
			for _, v := range broker.Lag(cfg.Group, topic) {
				lag += v
			}
			fmt.Printf("%-5s %9d %9d %7d %7d %9d %8d %8.0fms  %d\n", fmt.Sprintf("%ds", int(el.Seconds())), pr,
				st.OnTime+st.Late+st.TooLate+st.Duplicates+st.Invalid+st.Poison, st.Duplicates, st.Poison,
				st.WindowRows+st.LateUpdateRows, lag, st.P95*1000, n)
		}
	}

	// ---- settle, then verify
	fmt.Println("\nproducer finished; letting watermarks advance so the last windows close...")
	settle := cfg.MaxOutOfOrder + cfg.IdleTimeout + cfg.AllowedLateness + time.Second
	var stored int64
	deadline := time.Now().Add(settle + 10*time.Second)
	for time.Now().Before(deadline) {
		time.Sleep(250 * time.Millisecond)
		stored = total(store)
		pmu.Lock()
		u := unique
		pmu.Unlock()
		if stored == u && time.Since(t0) > *duration+settle/2 {
			break
		}
	}
	stopMembers() // graceful: drain, flush, commit
	mu.Lock()
	for _, m := range alive {
		<-m.ch
	}
	mu.Unlock()
	stored = total(store)

	pmu.Lock()
	defer pmu.Unlock()
	fmt.Printf("\n──────────────── RESULT ────────────────\n")
	fmt.Printf("messages produced:           %d\n", produced)
	fmt.Printf("  unique valid events:       %d\n", unique)
	fmt.Printf("  injected duplicates:       %d\n", dups)
	fmt.Printf("  injected garbage:          %d\n", poisoned)
	reasons := dlq.CountByReason()
	keys := make([]string, 0, len(reasons))
	for k := range reasons {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf("dead-lettered [%s]: %d\n", k, reasons[k])
	}
	st := first.Stats()
	fmt.Printf("late events accepted:        %d   dropped too-late: %d\n", st.Late, st.TooLate)
	fmt.Printf("events counted in store:     %d\n", stored)
	if stored == unique {
		fmt.Println("\n✓ EXACT: every unique event was counted exactly once — through disorder, duplicates, garbage and a crash.")
	} else {
		fmt.Printf("\n✗ MISMATCH: store has %d, expected %d (diff %+d)\n", stored, unique, stored-unique)
		os.Exit(2)
	}
}

func total(s storage.Store) int64 {
	rows, err := s.Summary(context.Background(), storage.Query{})
	if err != nil {
		return -1
	}
	var n int64
	for _, r := range rows {
		n += r.Events
	}
	return n
}
