SHELL := /bin/bash

COMPOSE      := docker compose -f deploy/docker-compose.yml
MIGRATE_DSN  := postgres://ledger:ledger@postgres:5432/ledger?sslmode=disable
MIGRATE      := $(COMPOSE) run --rm migrate -path=/migrations -database='$(MIGRATE_DSN)'

# Number of migrations `make migrate-down` reverses. One by default: rolling the
# whole schema back should be something you type on purpose (make migrate-down N=8).
N ?= 1

.DEFAULT_GOAL := help
.PHONY: help up down logs migrate-up migrate-down seed test test-race lint loadtest build fmt tidy psql

help: ## List available targets
	@grep -hE '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-14s\033[0m %s\n", $$1, $$2}'

up: ## Start the whole stack (postgres, redpanda, connect, redis, api) and apply migrations
	$(COMPOSE) up --build -d
	@echo "api      http://localhost:8080/healthz"
	@echo "metrics  http://localhost:9090/metrics"
	@echo "connect  http://localhost:8083/connectors"

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

loadtest: ## Run the k6 load profile against a running stack
	k6 run test/load/smoke.js
