SHELL := /bin/bash

COMPOSE       := docker compose -f deploy/docker-compose.yml
CHAOS_COMPOSE := docker compose -f deploy/docker-compose.yml -f deploy/docker-compose.chaos.yml
MIGRATE_DSN   := postgres://ledger:ledger@postgres:5432/ledger?sslmode=disable
MIGRATE       := $(COMPOSE) run --rm migrate -path=/migrations -database='$(MIGRATE_DSN)'

# Number of migrations `make migrate-down` reverses. One by default: rolling the
# whole schema back should be something you type on purpose (make migrate-down N=8).
N ?= 1

.DEFAULT_GOAL := help
.PHONY: help up down logs migrate-up migrate-down seed rebuild test test-race lint loadtest loadtest-smoke build fmt tidy psql gateway-behaviour sagas-stuck chaos-up chaos-down chaos-test chaos-fault

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Start the whole stack (postgres, redpanda, connect, redis, api, outbox-publisher, projector, saga-orchestrator, reconciler, mock-gateway, prometheus, grafana) and apply migrations
	$(COMPOSE) up --build -d
	@echo "api               http://localhost:8080/healthz"
	@echo "api metrics       http://localhost:9090/metrics"
	@echo "outbox-publisher  http://localhost:9091/healthz  (health+metrics share one port; no separate app port)"
	@echo "projector         http://localhost:9093/healthz  (health+metrics share one port; no separate app port)"
	@echo "saga-orchestrator http://localhost:9094/healthz  (health+metrics share one port; no separate app port)"
	@echo "reconciler        http://localhost:9095/healthz  (health+metrics share one port; consistency checks always run, PSP match disabled until LEDGER_RECONCILER_PSP_CSV_PATH is set)"
	@echo "mock-gateway      http://localhost:8090/healthz  (LOCAL ONLY; holds payments in memory)"
	@echo "connect           http://localhost:8083/connectors"
	@echo "prometheus        http://localhost:9099  (9090 is taken by the api container's own metrics port)"
	@echo "grafana           http://localhost:3001  (anonymous viewer access; admin/admin)"

rebuild: ## Recompute balances from journal_entries and diff against the live projection
	# No leading /usr/local/bin/service here: the image's ENTRYPOINT already is
	# that binary, so this becomes its argv. Repeating the binary path would
	# make -rebuild the entrypoint's SECOND positional argument rather than its
	# first flag, and Go's flag package stops looking for flags at the first
	# non-flag argument -- silently running the long-lived consumer instead of
	# a one-shot rebuild, exactly as this comment exists to prevent.
	$(COMPOSE) run --rm projector -rebuild

down: ## Stop the stack and remove its volumes
	$(COMPOSE) down --volumes --remove-orphans

logs: ## Follow logs for every service
	$(COMPOSE) logs -f

migrate-up: ## Apply all pending migrations
	$(MIGRATE) up

migrate-down: ## Reverse the last N migrations (default 1)
	$(MIGRATE) down $(N)

seed: ## Load the development chart of accounts (re-runnable)
	$(COMPOSE) exec -T postgres \
		psql -v ON_ERROR_STOP=1 -U ledger -d ledger < deploy/seed/seed.sql

psql: ## Open a psql shell against the local database
	$(COMPOSE) exec postgres psql -U ledger -d ledger

build: ## Compile every binary
	go build ./...

fmt: ## Format the tree
	gofmt -s -w .

tidy: ## Sync go.mod and go.sum
	go mod tidy

test: ## Run the test suite (starts Testcontainers; Docker must be running)
	go test -count=1 ./...

test-race: ## Run the test suite under the race detector
	go test -race -count=1 ./...

lint: ## Run golangci-lint
	golangci-lint run ./...

loadtest: ## Spin up the stack fresh, run every k6 scenario, and write docs/BENCHMARKS.md + docs/benchmarks.json
	$(COMPOSE) down --volumes --remove-orphans
	$(COMPOSE) up --build -d
	@echo "waiting for the api to report healthy..."
	@until curl -sf http://localhost:8080/healthz > /dev/null 2>&1; do sleep 1; done
	$(COMPOSE) exec -T postgres \
		psql -v ON_ERROR_STOP=1 -U ledger -d ledger < deploy/seed/seed.sql
	@echo "settling for 15s before the timed run: --build's own tail CPU usage (docker layer writes, builder cache bookkeeping) measurably inflates the first scenario's p99 otherwise"
	@sleep 15
	go run ./cmd/loadtest-harness -scenario=all

loadtest-smoke: ## Run the original probes-only k6 smoke profile against a running stack
	k6 run test/load/smoke.js

gateway-behaviour: ## Set the mock gateway's behaviour, e.g. make gateway-behaviour BEHAVIOUR='{"outcome":"decline"}'
	@curl -sS -X POST http://localhost:8090/control/behaviour \
		-H 'Content-Type: application/json' \
		-d '$(or $(BEHAVIOUR),{"outcome":"succeed"})' \
		&& echo "gateway behaviour set to $(or $(BEHAVIOUR),{\"outcome\":\"succeed\"})"

sagas-stuck: ## List sagas awaiting manual review
	@curl -sS 'http://localhost:8080/v1/sagas?status=NEEDS_MANUAL_REVIEW'

chaos-up: ## Start the stack with fault injection enabled (mounts the Docker socket into a new chaos-harness container -- see deploy/docker-compose.chaos.yml)
	$(CHAOS_COMPOSE) up --build -d
	@echo "chaos-harness     http://localhost:9199/healthz  (holds Docker-socket access; POST /faults/* to inject)"

chaos-down: ## Stop the chaos-enabled stack and remove its volumes
	$(CHAOS_COMPOSE) down --volumes --remove-orphans

chaos-fault: ## Inject one fault by hand, e.g. make chaos-fault FAULT=slow-query BODY='{"duration_seconds":10}'
	@curl -sS -X POST "http://localhost:9199/faults/$(FAULT)" \
		-H 'Content-Type: application/json' \
		-d '$(or $(BODY),{"duration_seconds":10})'

chaos-test: ## Run the chaos test against the already-running chaos stack (start it first with make chaos-up)
	LEDGER_CHAOS_HARNESS_URL=http://localhost:9199 \
	LEDGER_CHAOS_API_URL=http://localhost:8080 \
	go test -count=1 -run TestChaos -timeout 10m ./test/...
