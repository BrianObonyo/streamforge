.PHONY: all build test race integration bench demo lint up down logs redpanda-build

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
