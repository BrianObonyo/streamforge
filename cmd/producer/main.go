// Command producer is the ingestion tier: it can generate a synthetic workload,
// expose an HTTP ingestion API, or both.
//
//	producer -broker redpanda -brokers localhost:9092 -rate 5000 -serve :8081
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
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/BrianObonyo/streamforge/internal/backpressure"
	"github.com/BrianObonyo/streamforge/internal/config"
	"github.com/BrianObonyo/streamforge/internal/ingestion"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/streaming"
)

func main() {
	var (
		brokerKind  = flag.String("broker", config.String("SF_BROKER", "redpanda"), "broker implementation: memory|redpanda")
		brokers     = flag.String("brokers", config.String("SF_BROKERS", "localhost:9092"), "comma-separated seed brokers")
		topic       = flag.String("topic", config.String("SF_TOPIC", "events"), "topic to publish to")
		partitions  = flag.Int("partitions", config.Int("SF_PARTITIONS", 6), "partitions to create the topic with")
		partitioner = flag.String("partitioner", "hash", "hash|jump|round_robin")
		generate    = flag.Bool("generate", true, "run the synthetic workload generator")
		rate        = flag.Float64("rate", 1000, "generator events/sec (0 = unthrottled)")
		count       = flag.Int("count", 0, "stop the generator after N events (0 = until interrupted)")
		duration    = flag.Duration("duration", 0, "stop after this long (0 = until interrupted)")
		seed        = flag.Int64("seed", time.Now().UnixNano(), "RNG seed (fix for reproducible streams)")
		disorder    = flag.Float64("disorder", 0.1, "probability an event arrives out of order")
		maxDelay    = flag.Duration("max-delay", 3*time.Second, "max out-of-order delay")
		dup         = flag.Float64("dup", 0.02, "probability of a duplicate message")
		poison      = flag.Float64("poison", 0.005, "probability of an undecodable message")
		serve       = flag.String("serve", "", "HTTP ingestion/metrics listen address, e.g. :8081 (empty = off)")
		httpLimit   = flag.Float64("http-rate-limit", 0, "HTTP ingestion limit in events/sec (0 = unlimited)")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()
	log := config.Logger(*logLevel)

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	if *duration > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, *duration)
		defer cancel()
	}

	if *brokerKind == "memory" {
		log.Warn("the in-memory broker is private to this process; nothing else can consume what is produced. Use cmd/demo for a self-contained run.")
	}
	broker, err := streaming.Open(*brokerKind, strings.Split(*brokers, ","))
	fatal(log, err)
	defer broker.Close()
	fatal(log, broker.CreateTopic(ctx, *topic, *partitions))

	part, err := partitioning.Parse(*partitioner)
	fatal(log, err)
	prod, err := broker.NewProducer(part)
	fatal(log, err)
	defer prod.Close()

	reg := metrics.NewRegistry()
	pub := ingestion.NewPublisher(prod, *topic, reliability.DefaultRetry, reg)

	var wg sync.WaitGroup
	if *serve != "" {
		mux := http.NewServeMux()
		mux.Handle("/v1/events", &ingestion.Handler{Pub: pub, Limiter: limiter(*httpLimit)})
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = reg.WriteTo(w)
		})
		srv := &http.Server{Addr: *serve, Handler: mux, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 15 * time.Second, WriteTimeout: 15 * time.Second}
		wg.Add(1)
		go func() {
			defer wg.Done()
			log.Info("ingestion API listening", "addr", *serve)
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("http server", "err", err)
			}
		}()
		go func() {
			<-ctx.Done()
			sctx, c := context.WithTimeout(context.Background(), 5*time.Second)
			defer c()
			_ = srv.Shutdown(sctx)
		}()
	}

	if *generate {
		g := ingestion.NewGenerator(ingestion.GenConfig{
			Rate: *rate, Seed: *seed, DisorderProb: *disorder, MaxDelay: *maxDelay, DupProb: *dup, PoisonProb: *poison,
		})
		log.Info("generating", "rate", *rate, "seed", *seed, "disorder", *disorder, "dup", *dup, "poison", *poison)
		n, err := g.Run(ctx, pub, *count)
		log.Info("generator finished", "published", n, "err", err)
		if *serve == "" || *count > 0 {
			stop()
		}
	}
	<-ctx.Done()
	wg.Wait()
}

func limiter(rate float64) *backpressure.TokenBucket {
	if rate <= 0 {
		return nil
	}
	return backpressure.NewTokenBucket(rate, rate)
}

func fatal(log interface{ Error(string, ...any) }, err error) {
	if err != nil {
		log.Error("fatal", "err", err)
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
