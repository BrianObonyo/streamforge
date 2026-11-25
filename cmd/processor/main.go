// Command processor runs one stream-processing consumer-group member.
// Run several (any host) to scale out; the broker splits partitions between them.
//
// Project: StreamForge — Brian Obonyo (github.com/BrianObonyo/streamforge)
package main

import (
	"context"
	"errors"
	"flag"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/BrianObonyo/streamforge/internal/config"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/partitioning"
	"github.com/BrianObonyo/streamforge/internal/processor"
	"github.com/BrianObonyo/streamforge/internal/reliability"
	"github.com/BrianObonyo/streamforge/internal/storage"
	"github.com/BrianObonyo/streamforge/internal/streaming"
	"github.com/BrianObonyo/streamforge/internal/tracing"
)

func main() {
	var cfg processor.Config
	processor.RegisterFlags(flag.CommandLine, &cfg)
	var (
		brokerKind  = flag.String("broker", config.String("SF_BROKER", "redpanda"), "memory|redpanda")
		brokers     = flag.String("brokers", config.String("SF_BROKERS", "localhost:9092"), "seed brokers")
		partitions  = flag.Int("partitions", config.Int("SF_PARTITIONS", 6), "partitions if the topic must be created")
		member      = flag.String("member", config.String("HOSTNAME", ""), "consumer member id (defaults to hostname)")
		storeKind   = flag.String("store", config.String("SF_STORE", "clickhouse"), "memory|clickhouse")
		chURL       = flag.String("clickhouse-url", config.String("SF_CLICKHOUSE_URL", "http://localhost:8123"), "ClickHouse HTTP URL")
		chDB        = flag.String("clickhouse-db", config.String("SF_CLICKHOUSE_DB", "streamforge"), "ClickHouse database")
		chUser      = flag.String("clickhouse-user", config.String("SF_CLICKHOUSE_USER", ""), "ClickHouse user")
		chPass      = flag.String("clickhouse-password", config.String("SF_CLICKHOUSE_PASSWORD", ""), "ClickHouse password")
		metricsAddr = flag.String("metrics-addr", ":9100", "metrics/health listen address (empty = off)")
		logLevel    = flag.String("log-level", "info", "debug|info|warn|error")
	)
	newTracer := tracing.RegisterFlags(flag.CommandLine)
	flag.Parse()
	log := config.Logger(*logLevel)
	cfg.MemberID = *member
	cfg.Tracer = newTracer("streamforge-processor", log)
	defer func() { // flush buffered spans on the way out
		c, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = cfg.Tracer.Shutdown(c)
	}()

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	die := func(err error) {
		if err != nil {
			log.Error("fatal", "err", err)
			os.Exit(1)
		}
	}

	broker, err := streaming.Open(*brokerKind, strings.Split(*brokers, ","))
	die(err)
	defer broker.Close()
	die(broker.CreateTopic(ctx, cfg.Topic, *partitions))
	die(broker.CreateTopic(ctx, cfg.Topic+".dlq", 1))

	store, err := storage.Open(ctx, *storeKind, storage.CHConfig{URL: *chURL, Database: *chDB, User: *chUser, Password: *chPass})
	die(err)
	checkpoints, ok := store.(storage.CheckpointStore)
	if !ok {
		die(errors.New("configured store does not support processor checkpoints"))
	}

	dlqProd, err := broker.NewProducer(partitioning.Hash{})
	die(err)
	consumer, err := broker.NewConsumer(cfg.Group, cfg.MemberID, cfg.Topic)
	die(err)
	defer consumer.Close()

	reg := metrics.NewRegistry()
	cfg.Tracer.RegisterMetrics(reg)
	p, err := processor.New(cfg, processor.Deps{
		Consumer: consumer, Sink: store, Checkpoints: checkpoints, Metrics: reg, Logger: log,
		DLQ: &reliability.BrokerDLQ{P: dlqProd, Topic: cfg.Topic + ".dlq"},
	})
	die(err)

	if *metricsAddr != "" {
		mux := http.NewServeMux()
		mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok\n")) })
		mux.HandleFunc("/metrics", func(w http.ResponseWriter, _ *http.Request) {
			w.Header().Set("Content-Type", "text/plain; version=0.0.4")
			_, _ = reg.WriteTo(w)
		})
		srv := &http.Server{Addr: *metricsAddr, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
		go func() {
			if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
				log.Error("metrics server", "err", err)
			}
		}()
		defer srv.Close()
	}

	if err := p.Run(ctx); err != nil {
		log.Error("processor exited", "err", err)
		c, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		_ = cfg.Tracer.Shutdown(c) // os.Exit skips defers, so flush explicitly
		cancel()
		os.Exit(1)
	}
}
