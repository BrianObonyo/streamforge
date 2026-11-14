// Command api serves the analytics HTTP API over the results store.
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
	"syscall"
	"time"

	"github.com/BrianObonyo/streamforge/internal/api"
	"github.com/BrianObonyo/streamforge/internal/config"
	"github.com/BrianObonyo/streamforge/internal/metrics"
	"github.com/BrianObonyo/streamforge/internal/storage"
)

func main() {
	var (
		addr      = flag.String("addr", config.String("SF_API_ADDR", ":8080"), "listen address")
		storeKind = flag.String("store", config.String("SF_STORE", "clickhouse"), "memory|clickhouse")
		chURL     = flag.String("clickhouse-url", config.String("SF_CLICKHOUSE_URL", "http://localhost:8123"), "ClickHouse HTTP URL")
		chDB      = flag.String("clickhouse-db", config.String("SF_CLICKHOUSE_DB", "streamforge"), "database")
		chUser    = flag.String("clickhouse-user", config.String("SF_CLICKHOUSE_USER", ""), "user")
		chPass    = flag.String("clickhouse-password", config.String("SF_CLICKHOUSE_PASSWORD", ""), "password")
		logLevel  = flag.String("log-level", "info", "debug|info|warn|error")
	)
	flag.Parse()
	log := config.Logger(*logLevel)
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	store, err := storage.Open(ctx, *storeKind, storage.CHConfig{URL: *chURL, Database: *chDB, User: *chUser, Password: *chPass})
	if err != nil {
		log.Error("store", "err", err)
		os.Exit(1)
	}
	s := &api.Server{Store: store, Metrics: metrics.NewRegistry(), Log: log}
	srv := &http.Server{Addr: *addr, Handler: s.Handler(), ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout: 15 * time.Second, WriteTimeout: 30 * time.Second, IdleTimeout: 60 * time.Second}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 10*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	}()
	log.Info("analytics API listening", "addr", *addr, "store", *storeKind)
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server", "err", err)
		os.Exit(1)
	}
}
