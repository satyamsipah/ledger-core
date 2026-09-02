// Package config loads and validates process configuration from the
// environment.
//
// Configuration is loaded once at startup and passed into constructors
// explicitly, rather than read at the point of use. That is what lets a test
// construct any component with values it chose without mutating process-wide
// state, which is the whole reason CLAUDE.md forbids globals here.
package config

import (
	"fmt"
	"log/slog"
	"os"
	"strconv"
	"strings"
	"time"
)

// envPrefix namespaces every variable this service reads, so that a shared
// container environment cannot collide with, say, a sidecar's HTTP_ADDR.
const envPrefix = "LEDGER_"

// Config is the fully resolved configuration for one process.
type Config struct {
	Env           string
	Service       string
	HTTP          HTTP
	Postgres      Postgres
	Ledger        Ledger
	Kafka         Kafka
	Outbox        Outbox
	Saga          Saga
	Gateway       Gateway
	Redis         Redis
	Reconciler    Reconciler
	Observability Observability
}

// Ledger configures the write path's concurrency behaviour and the idempotency
// lifetimes.
type Ledger struct {
	// AdvisoryLocks turns on per-account pg_advisory_xact_lock before the row
	// locks. Off by default, and it is a PROCESS-WIDE switch rather than a
	// per-request one: advisory locks are a separate lock space, so a
	// deployment where only some write paths take them has two lock orderings
	// instead of the single global one that makes deadlock unconstructible.
	AdvisoryLocks bool

	// MaxTxAttempts caps retries of transactions PostgreSQL aborted with 40001
	// or 40P01. Those two alone: every other error is either ambiguous about
	// whether it committed, or deterministic. See internal/db/retry.go.
	MaxTxAttempts    int
	RetryBaseBackoff time.Duration
	RetryMaxBackoff  time.Duration

	// IdempotencyTTL is how long a replay record survives. It bounds storage
	// only -- the key itself stays reserved permanently by
	// transactions_idempotency_key_key, so expiry can never permit a second
	// transaction, only refuse a replay.
	IdempotencyTTL time.Duration

	// IdempotencyLease is how long one in-flight request may hold a key before
	// another may reclaim it. Sized above the Postgres query timeout so a live
	// request is never reclaimed out from under itself.
	IdempotencyLease time.Duration

	SweepInterval time.Duration
	SweepBatch    int
}

// Saga configures the orchestrator's claim loop, its leases, and how patient it
// is before giving up on a step.
type Saga struct {
	// WorkerID identifies this replica in saga_instances.lease_owner. Defaults
	// to the hostname, which in a container is the container id -- exactly what
	// an operator needs to find the process that was driving a stuck saga.
	WorkerID string

	// ClaimInterval is how often the claim loop looks for runnable sagas.
	ClaimInterval time.Duration
	ClaimBatch    int

	// Lease is how long a claim holds a saga before another replica may take
	// it. Sized above StepTimeout so a replica that is merely slow is not
	// overtaken by one that assumes it died.
	Lease time.Duration

	// StepTimeout is how long one step may take before the sweeper treats it as
	// stuck. It bounds a step, not the saga: a saga that has been alive for an
	// hour is fine as long as each step finished.
	StepTimeout time.Duration

	// MaxStepAttempts caps retries of a FORWARD step.
	MaxStepAttempts int

	// MaxCompensationAttempts caps retries of a COMPENSATION, and is
	// deliberately larger than MaxStepAttempts.
	//
	// The asymmetry is the point. Giving up on a forward step is cheap -- the
	// saga compensates and the customer is untouched. Giving up on a
	// compensation strands real money in a suspense account and pages a human.
	// The two failures are not comparable, so their budgets are not equal.
	MaxCompensationAttempts int

	// SweepInterval is how often stuck sagas are looked for. Below Lease, so a
	// saga abandoned by a dead replica is found promptly once its lease lapses.
	SweepInterval time.Duration
}

// Gateway configures the external payment gateway client.
type Gateway struct {
	URL string

	// Timeout bounds a payment submission. Generous, because this call can
	// create a charge and a timeout here leaves an ambiguity a human may have
	// to resolve.
	Timeout time.Duration

	// ProbeTimeout bounds the resolution query. Short, because a probe cannot
	// create anything, so giving up early costs nothing but another attempt.
	ProbeTimeout time.Duration

	// MaxProbes is how many inconclusive probes a saga endures before it is
	// escalated to NEEDS_MANUAL_REVIEW rather than guessed at.
	MaxProbes int
}

// HTTP configures the public API listener. Every timeout is explicit because a
// server with no write timeout will happily hold a connection open forever
// under load, which is how a slow client turns into an outage.
type HTTP struct {
	Addr            string
	ReadTimeout     time.Duration
	WriteTimeout    time.Duration
	IdleTimeout     time.Duration
	ShutdownTimeout time.Duration

	// TrustedProxyHops is how many rightmost X-Forwarded-For entries clientIP
	// (D19) treats as written by infrastructure this deployment vouches for.
	// Zero -- the default -- means nothing is trusted and the header is
	// ignored outright, which is correct for today's actual deployment
	// (nothing sits in front of this service) and safe for any deployment this
	// value has not been explicitly set for.
	TrustedProxyHops int
}

// Postgres configures the connection pool. QueryTimeout is the default budget
// applied per statement; CLAUDE.md requires every DB call to have one, and
// carrying it in config means no call site has to invent a number.
type Postgres struct {
	DSN             string
	MaxConns        int32
	MinConns        int32
	MaxConnLifetime time.Duration
	MaxConnIdleTime time.Duration
	ConnectTimeout  time.Duration
	QueryTimeout    time.Duration
}

// Kafka configures broker connectivity. Locally these point at Redpanda, which
// speaks the same protocol.
type Kafka struct {
	Brokers       []string
	ConsumerGroup string
}

// Outbox configures how committed outbox rows reach Kafka. See
// docs/DECISIONS.md D31 for the comparison between the two publishers this
// selects between.
type Outbox struct {
	// Publisher selects which implementation runs: "polling" or "debezium".
	// Debezium is the default -- see D31 for why LSN ordering outweighs
	// polling's lower operational footprint for a ledger specifically.
	Publisher string

	// PollInterval and BatchSize configure the polling publisher only; the
	// Debezium publisher ignores both, since it does not poll anything itself.
	PollInterval time.Duration
	BatchSize    int

	// ConnectURL and ConnectorName configure the Debezium publisher's
	// connector-status monitor: where Kafka Connect's REST API lives, and
	// which connector (registered separately, by deploy/docker-compose.yml's
	// connect-init service) to report the health of.
	ConnectURL    string
	ConnectorName string
}

// Redis configures the idempotency fast path. Redis is a latency optimisation
// only: correctness never depends on it being reachable.
type Redis struct {
	Addr string
	DB   int
}

// Reconciler configures the three-way-match job cmd/reconciler runs on a
// schedule.
type Reconciler struct {
	// PSPStatementPath is where the mock settlement CSV is read from. Empty
	// means the job is disabled -- a process with nothing to reconcile
	// against should not run a ticker that fails on every tick.
	PSPStatementPath string

	// Interval is how often a new run starts. Daily in production, per the
	// phase's own framing ("daily reconciliation report"); short in local
	// development, per docker-compose.yml, so `docker compose up` produces a
	// visible run instead of a day-long wait.
	Interval time.Duration

	// TimingWindow is classify's auto-resolve threshold -- see
	// internal/reconciliation.DefaultTimingWindow for the default and the
	// reasoning.
	TimingWindow time.Duration

	// Lookback bounds how far back Match considers ledger transactions when
	// looking for MISSING_IN_PSP references. See
	// internal/reconciliation.DefaultLookback.
	Lookback time.Duration
}

// Observability configures the three signals: logs, metrics, traces.
type Observability struct {
	LogLevel         slog.Level
	MetricsAddr      string
	OTLPEndpoint     string
	TraceSampleRatio float64
}

// Load reads configuration for the named service from the environment,
// applying development-friendly defaults so that `go run ./cmd/api` works
// against a `make up` stack with no exported variables at all.
//
// It returns an error rather than exiting so that main can log the failure
// through the same logger it uses for everything else.
func Load(service string) (Config, error) {
	var errs []string
	fail := func(format string, args ...any) {
		errs = append(errs, fmt.Sprintf(format, args...))
	}

	cfg := Config{
		Env:     env("ENV", "local"),
		Service: service,
		HTTP: HTTP{
			Addr:             env("HTTP_ADDR", ":8080"),
			ReadTimeout:      envDuration("HTTP_READ_TIMEOUT", 5*time.Second, fail),
			WriteTimeout:     envDuration("HTTP_WRITE_TIMEOUT", 10*time.Second, fail),
			IdleTimeout:      envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second, fail),
			ShutdownTimeout:  envDuration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second, fail),
			TrustedProxyHops: envInt("TRUSTED_PROXY_HOPS", 0, fail),
		},
		Postgres: Postgres{
			DSN:             env("POSTGRES_DSN", "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"),
			MaxConns:        int32(envInt("POSTGRES_MAX_CONNS", 20, fail)),
			MinConns:        int32(envInt("POSTGRES_MIN_CONNS", 2, fail)),
			MaxConnLifetime: envDuration("POSTGRES_MAX_CONN_LIFETIME", time.Hour, fail),
			MaxConnIdleTime: envDuration("POSTGRES_MAX_CONN_IDLE_TIME", 30*time.Minute, fail),
			ConnectTimeout:  envDuration("POSTGRES_CONNECT_TIMEOUT", 5*time.Second, fail),
			QueryTimeout:    envDuration("POSTGRES_QUERY_TIMEOUT", 3*time.Second, fail),
		},
		Ledger: Ledger{
			AdvisoryLocks:    envBool("LEDGER_ADVISORY_LOCKS", false, fail),
			MaxTxAttempts:    envInt("LEDGER_MAX_TX_ATTEMPTS", 5, fail),
			RetryBaseBackoff: envDuration("LEDGER_RETRY_BASE_BACKOFF", 5*time.Millisecond, fail),
			RetryMaxBackoff:  envDuration("LEDGER_RETRY_MAX_BACKOFF", 250*time.Millisecond, fail),
			IdempotencyTTL:   envDuration("IDEMPOTENCY_TTL", 24*time.Hour, fail),
			IdempotencyLease: envDuration("IDEMPOTENCY_LEASE", 30*time.Second, fail),
			SweepInterval:    envDuration("IDEMPOTENCY_SWEEP_INTERVAL", 5*time.Minute, fail),
			SweepBatch:       envInt("IDEMPOTENCY_SWEEP_BATCH", 1000, fail),
		},
		Kafka: Kafka{
			Brokers:       envList("KAFKA_BROKERS", []string{"localhost:9092"}),
			ConsumerGroup: env("KAFKA_CONSUMER_GROUP", "ledger-"+service),
		},
		Outbox: Outbox{
			Publisher:     env("OUTBOX_PUBLISHER", "debezium"),
			PollInterval:  envDuration("OUTBOX_POLL_INTERVAL", 500*time.Millisecond, fail),
			BatchSize:     envInt("OUTBOX_BATCH_SIZE", 100, fail),
			ConnectURL:    env("OUTBOX_CONNECT_URL", "http://localhost:8083"),
			ConnectorName: env("OUTBOX_CONNECTOR_NAME", "ledger-outbox"),
		},
		Saga: Saga{
			WorkerID:                env("SAGA_WORKER_ID", defaultWorkerID()),
			ClaimInterval:           envDuration("SAGA_CLAIM_INTERVAL", 250*time.Millisecond, fail),
			ClaimBatch:              envInt("SAGA_CLAIM_BATCH", 50, fail),
			Lease:                   envDuration("SAGA_LEASE", 60*time.Second, fail),
			StepTimeout:             envDuration("SAGA_STEP_TIMEOUT", 30*time.Second, fail),
			MaxStepAttempts:         envInt("SAGA_MAX_STEP_ATTEMPTS", 5, fail),
			MaxCompensationAttempts: envInt("SAGA_MAX_COMPENSATION_ATTEMPTS", 8, fail),
			SweepInterval:           envDuration("SAGA_SWEEP_INTERVAL", 10*time.Second, fail),
		},
		Gateway: Gateway{
			URL:          env("GATEWAY_URL", "http://localhost:8090"),
			Timeout:      envDuration("GATEWAY_TIMEOUT", 10*time.Second, fail),
			ProbeTimeout: envDuration("GATEWAY_PROBE_TIMEOUT", 5*time.Second, fail),
			MaxProbes:    envInt("GATEWAY_MAX_PROBES", 6, fail),
		},
		Redis: Redis{
			Addr: env("REDIS_ADDR", "localhost:6379"),
			DB:   envInt("REDIS_DB", 0, fail),
		},
		Reconciler: Reconciler{
			PSPStatementPath: env("RECONCILER_PSP_CSV_PATH", ""),
			Interval:         envDuration("RECONCILER_INTERVAL", 24*time.Hour, fail),
			TimingWindow:     envDuration("RECONCILER_TIMING_WINDOW", 2*time.Hour, fail),
			Lookback:         envDuration("RECONCILER_LOOKBACK", 7*24*time.Hour, fail),
		},
		Observability: Observability{
			LogLevel:         envLogLevel("LOG_LEVEL", slog.LevelInfo, fail),
			MetricsAddr:      env("METRICS_ADDR", ":9090"),
			OTLPEndpoint:     env("OTLP_ENDPOINT", ""),
			TraceSampleRatio: envFloat("TRACE_SAMPLE_RATIO", 1.0, fail),
		},
	}

	if cfg.Postgres.DSN == "" {
		fail("%sPOSTGRES_DSN must not be empty", envPrefix)
	}
	if cfg.Postgres.MinConns > cfg.Postgres.MaxConns {
		fail("%sPOSTGRES_MIN_CONNS (%d) exceeds %sPOSTGRES_MAX_CONNS (%d)",
			envPrefix, cfg.Postgres.MinConns, envPrefix, cfg.Postgres.MaxConns)
	}
	if cfg.Observability.TraceSampleRatio < 0 || cfg.Observability.TraceSampleRatio > 1 {
		fail("%sTRACE_SAMPLE_RATIO must be between 0 and 1, got %v",
			envPrefix, cfg.Observability.TraceSampleRatio)
	}
	if cfg.HTTP.Addr == cfg.Observability.MetricsAddr {
		fail("%sHTTP_ADDR and %sMETRICS_ADDR must differ; metrics are not exposed publicly",
			envPrefix, envPrefix)
	}
	if cfg.Ledger.MaxTxAttempts < 1 {
		fail("%sLEDGER_MAX_TX_ATTEMPTS must be at least 1, got %d", envPrefix, cfg.Ledger.MaxTxAttempts)
	}
	if cfg.HTTP.TrustedProxyHops < 0 {
		fail("%sTRUSTED_PROXY_HOPS must not be negative, got %d", envPrefix, cfg.HTTP.TrustedProxyHops)
	}
	// A lease shorter than the transaction budget would let a request still
	// holding account row locks be declared abandoned and reclaimed by its own
	// retry. The two would then contend for the same accounts, and the loser
	// would abort on ErrLeaseLost -- correct, but a self-inflicted failure that
	// looks exactly like a real one in the logs.
	// A step deadline shorter than the gateway's own timeout would declare a
	// call stuck while it is still legitimately outstanding, and the sweeper
	// would start probing a payment whose original response is about to arrive.
	// Harmless but wasteful, and it makes every slow-but-healthy gateway look
	// like an ambiguity in the metrics.
	if cfg.Saga.StepTimeout <= cfg.Gateway.Timeout {
		fail("%sSAGA_STEP_TIMEOUT (%s) must exceed %sGATEWAY_TIMEOUT (%s)",
			envPrefix, cfg.Saga.StepTimeout, envPrefix, cfg.Gateway.Timeout)
	}
	// A lease shorter than a step would let a replica still working a step be
	// overtaken by one that assumed it died. Both would then drive the same
	// saga, and the slower one's guarded transition would fail -- correct, but
	// a self-inflicted race that looks like a real one.
	if cfg.Saga.Lease <= cfg.Saga.StepTimeout {
		fail("%sSAGA_LEASE (%s) must exceed %sSAGA_STEP_TIMEOUT (%s)",
			envPrefix, cfg.Saga.Lease, envPrefix, cfg.Saga.StepTimeout)
	}
	// The sweeper must run more often than a lease lasts, or a saga abandoned
	// by a dead replica waits for the sweep rather than for the lease.
	if cfg.Saga.SweepInterval >= cfg.Saga.Lease {
		fail("%sSAGA_SWEEP_INTERVAL (%s) must be shorter than %sSAGA_LEASE (%s)",
			envPrefix, cfg.Saga.SweepInterval, envPrefix, cfg.Saga.Lease)
	}
	if cfg.Saga.MaxCompensationAttempts < cfg.Saga.MaxStepAttempts {
		fail("%sSAGA_MAX_COMPENSATION_ATTEMPTS (%d) must be at least %sSAGA_MAX_STEP_ATTEMPTS (%d): "+
			"abandoning a compensation strands money, abandoning a forward step does not",
			envPrefix, cfg.Saga.MaxCompensationAttempts, envPrefix, cfg.Saga.MaxStepAttempts)
	}
	if cfg.Gateway.MaxProbes < 1 {
		fail("%sGATEWAY_MAX_PROBES must be at least 1, got %d", envPrefix, cfg.Gateway.MaxProbes)
	}
	if cfg.Ledger.IdempotencyLease <= cfg.Postgres.QueryTimeout {
		fail("%sIDEMPOTENCY_LEASE (%s) must exceed %sPOSTGRES_QUERY_TIMEOUT (%s)",
			envPrefix, cfg.Ledger.IdempotencyLease, envPrefix, cfg.Postgres.QueryTimeout)
	}
	if cfg.Ledger.IdempotencyLease > cfg.Ledger.IdempotencyTTL {
		fail("%sIDEMPOTENCY_LEASE (%s) must not exceed %sIDEMPOTENCY_TTL (%s); "+
			"idempotency_keys_lease_within_ttl_check would reject the reservation",
			envPrefix, cfg.Ledger.IdempotencyLease, envPrefix, cfg.Ledger.IdempotencyTTL)
	}
	switch cfg.Outbox.Publisher {
	case "polling", "debezium":
	default:
		fail("%sOUTBOX_PUBLISHER must be \"polling\" or \"debezium\", got %q",
			envPrefix, cfg.Outbox.Publisher)
	}
	if cfg.Outbox.BatchSize < 1 {
		fail("%sOUTBOX_BATCH_SIZE must be at least 1, got %d", envPrefix, cfg.Outbox.BatchSize)
	}

	if len(errs) > 0 {
		return Config{}, fmt.Errorf("load config: %s", strings.Join(errs, "; "))
	}
	return cfg, nil
}

func env(key, fallback string) string {
	if v, ok := os.LookupEnv(envPrefix + key); ok && v != "" {
		return v
	}
	return fallback
}

func envList(key string, fallback []string) []string {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	parts := strings.Split(raw, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}
	if len(out) == 0 {
		return fallback
	}
	return out
}

func envInt(key string, fallback int, fail func(string, ...any)) int {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	v, err := strconv.Atoi(raw)
	if err != nil {
		fail("%s%s: %v", envPrefix, key, err)
		return fallback
	}
	return v
}

// envBool parses a boolean, rejecting anything strconv does not recognise
// rather than treating it as false. A typo in LEDGER_ADVISORY_LOCKS silently
// meaning "off" is how a feature flag stops being a feature flag.
func envBool(key string, fallback bool, fail func(string, ...any)) bool {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseBool(raw)
	if err != nil {
		fail("%s%s: %v", envPrefix, key, err)
		return fallback
	}
	return v
}

func envFloat(key string, fallback float64, fail func(string, ...any)) float64 {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	v, err := strconv.ParseFloat(raw, 64)
	if err != nil {
		fail("%s%s: %v", envPrefix, key, err)
		return fallback
	}
	return v
}

func envDuration(key string, fallback time.Duration, fail func(string, ...any)) time.Duration {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	v, err := time.ParseDuration(raw)
	if err != nil {
		fail("%s%s: %v", envPrefix, key, err)
		return fallback
	}
	return v
}

func envLogLevel(key string, fallback slog.Level, fail func(string, ...any)) slog.Level {
	raw := env(key, "")
	if raw == "" {
		return fallback
	}
	var level slog.Level
	if err := level.UnmarshalText([]byte(raw)); err != nil {
		fail("%s%s: %v", envPrefix, key, err)
		return fallback
	}
	return level
}

// defaultWorkerID names this replica in saga_instances.lease_owner.
//
// The hostname, which in a container is the container id: when a saga is found
// stuck holding a lease, the first question is which process was driving it,
// and a value that answers that without a lookup is worth more than a prettier
// one that does not.
func defaultWorkerID() string {
	host, err := os.Hostname()
	if err != nil || host == "" {
		return "unknown-worker"
	}
	return host
}
