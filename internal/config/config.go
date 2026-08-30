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
	Kafka         Kafka
	Redis         Redis
	Observability Observability
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

// Redis configures the idempotency fast path. Redis is a latency optimisation
// only: correctness never depends on it being reachable.
type Redis struct {
	Addr string
	DB   int
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
			Addr:            env("HTTP_ADDR", ":8080"),
			ReadTimeout:     envDuration("HTTP_READ_TIMEOUT", 5*time.Second, fail),
			WriteTimeout:    envDuration("HTTP_WRITE_TIMEOUT", 10*time.Second, fail),
			IdleTimeout:     envDuration("HTTP_IDLE_TIMEOUT", 60*time.Second, fail),
			ShutdownTimeout: envDuration("HTTP_SHUTDOWN_TIMEOUT", 15*time.Second, fail),
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
		Kafka: Kafka{
			Brokers:       envList("KAFKA_BROKERS", []string{"localhost:9092"}),
			ConsumerGroup: env("KAFKA_CONSUMER_GROUP", "ledger-"+service),
		},
		Redis: Redis{
			Addr: env("REDIS_ADDR", "localhost:6379"),
			DB:   envInt("REDIS_DB", 0, fail),
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
