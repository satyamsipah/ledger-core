// Package db owns the PostgreSQL connection pool and the conventions every
// repository inherits from it.
package db

import (
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/config"
)

// Pool wraps a pgx pool together with the default per-statement timeout, so a
// repository can obtain a bounded context without knowing where the number came
// from. CLAUDE.md requires every DB call to carry a timeout; carrying the
// budget alongside the pool is what makes that convenient enough to actually
// follow.
type Pool struct {
	*pgxpool.Pool

	queryTimeout time.Duration
}

// NewPool opens and verifies a connection pool.
//
// It pings before returning because a pool that has never connected reports
// itself healthy: without the ping, a bad DSN surfaces on the first request
// instead of at startup, where it belongs.
func NewPool(ctx context.Context, cfg config.Postgres, logger *slog.Logger) (*Pool, error) {
	poolCfg, err := pgxpool.ParseConfig(cfg.DSN)
	if err != nil {
		return nil, fmt.Errorf("parse postgres dsn: %w", err)
	}

	poolCfg.MaxConns = cfg.MaxConns
	poolCfg.MinConns = cfg.MinConns
	poolCfg.MaxConnLifetime = cfg.MaxConnLifetime
	poolCfg.MaxConnIdleTime = cfg.MaxConnIdleTime
	poolCfg.ConnConfig.ConnectTimeout = cfg.ConnectTimeout

	// pgx's own default is already QueryExecModeCacheStatement -- server-side
	// prepared statements, cached per connection -- so this only ever changes
	// anything when POSTGRES_QUERY_EXEC_MODE is explicitly overridden to
	// "simple_protocol" for the Phase 7 optimisation-cycle comparison
	// (docs/DECISIONS.md). config.Load already rejects any other value.
	if cfg.QueryExecMode == "simple_protocol" {
		poolCfg.ConnConfig.DefaultQueryExecMode = pgx.QueryExecModeSimpleProtocol
	}

	pool, err := pgxpool.NewWithConfig(ctx, poolCfg)
	if err != nil {
		return nil, fmt.Errorf("create postgres pool: %w", err)
	}

	pingCtx, cancel := context.WithTimeout(ctx, cfg.ConnectTimeout)
	defer cancel()
	if err := pool.Ping(pingCtx); err != nil {
		pool.Close()
		return nil, fmt.Errorf("ping postgres: %w", err)
	}

	logger.Info("postgres pool ready",
		slog.String("host", poolCfg.ConnConfig.Host),
		slog.String("database", poolCfg.ConnConfig.Database),
		slog.Int("max_conns", int(poolCfg.MaxConns)))

	return &Pool{Pool: pool, queryTimeout: cfg.QueryTimeout}, nil
}

// QueryContext derives a context bounded by the configured statement timeout.
// Callers must call the returned cancel func.
func (p *Pool) QueryContext(ctx context.Context) (context.Context, context.CancelFunc) {
	return context.WithTimeout(ctx, p.queryTimeout)
}

// Name identifies this dependency in readiness output.
func (p *Pool) Name() string { return "postgres" }

// Check satisfies the readiness Checker contract.
func (p *Pool) Check(ctx context.Context) error {
	ctx, cancel := p.QueryContext(ctx)
	defer cancel()
	if err := p.Ping(ctx); err != nil {
		return fmt.Errorf("ping postgres: %w", err)
	}
	return nil
}
