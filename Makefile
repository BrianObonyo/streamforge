.PHONY: all build test race integration bench demo simulator lint up down logs redpanda-build redpanda-test e2e verify-live fuzz check-compose

GO ?= go

all: lint test build

build:               ## compile all binaries into ./bin (stdlib-only core)
	$(GO) build -trimpath -o bin/ ./cmd/...

redpanda-build:      ## same, with the Redpanda/Kafka adapter linked in
	$(GO) build -tags redpanda -trimpath -o bin/ ./cmd/...

test:                ## unit tests
	$(GO) test -count=1 ./internal/...

race:                ## everything, under the race detector
	$(GO) test -race -count=1 ./...

integration:         ## end-to-end chaos tests (crash, duplicates, sink outage, backpressure)
	$(GO) test -race -count=1 -v ./tests/integration/

bench:               ## micro + pipeline benchmarks
	$(GO) test -bench . -benchmem -run '^$$' -benchtime=2s ./benchmarks/

demo:                ## self-contained run that kills a processor mid-stream and verifies exact counts
	$(GO) run ./cmd/demo

simulator:           ## replay caller-provided JSONL events; pass INPUT=path/to/events.jsonl
	@test -n "$(INPUT)" || (echo 'set INPUT=path/to/events.jsonl'; exit 2)
	$(GO) run ./cmd/simulator -input "$(INPUT)"

redpanda-test:      ## adapter + chaos pipeline over the real Kafka protocol (franz-go kfake)
	$(GO) test -tags redpanda -race -count=1 ./internal/streaming/ ./tests/integration/

verify-live:         ## live-service tests; set SF_TEST_CLICKHOUSE_URL and/or SF_TEST_BROKERS
	@test -z "$$SF_TEST_CLICKHOUSE_URL" || $(GO) test -count=1 -v -run Live ./internal/storage/
	@test -z "$$SF_TEST_BROKERS" || $(GO) test -tags redpanda -count=1 -v -run LiveBroker ./internal/streaming/

e2e: redpanda-build  ## full multi-process verification (needs clickhouse, k6, prometheus, otelcol-contrib)
	$(GO) build -tags redpanda -o bin/ ./tests/tools/...
	BIN=./bin scripts/e2e-local.sh

check-compose: redpanda-build  ## static compose checks (no Docker needed)
	python3 scripts/check-compose.py ./bin

fuzz:                ## 30 s of fuzzing per target
	$(GO) test -run xxx -fuzz FuzzDecodeBinary -fuzztime 30s ./internal/events/
	$(GO) test -run xxx -fuzz FuzzParseTraceparent -fuzztime 30s ./internal/tracing/

lint:                ## gofmt + vet (CI also runs staticcheck)
	@test -z "$$(gofmt -l .)" || (echo 'gofmt needed:'; gofmt -l .; exit 1)
	$(GO) vet ./...
	$(GO) vet -tags redpanda ./...

up:                  ## full stack: Redpanda, ClickHouse, Prometheus, Grafana, apps
	docker compose -f deployments/docker-compose.yml up -d --build

down:
	docker compose -f deployments/docker-compose.yml down -v

logs:
	docker compose -f deployments/docker-compose.yml logs -f producer processor api
