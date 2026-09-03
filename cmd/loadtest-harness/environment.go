// Gathers exactly what CLAUDE.md's Phase 7 brief asks the report be honest
// about: the hardware this ran on, the Postgres configuration in effect, and
// the connection pool settings actually applied -- queried from the running
// stack itself rather than restated from documentation, so this cannot drift
// from what a reproduction run actually used.
package main

import (
	"context"
	"fmt"
	"os/exec"
	"runtime"
	"strings"

	"github.com/jackc/pgx/v5/pgxpool"
)

type environmentInfo struct {
	Caveat string `json:"caveat"`

	OS       string `json:"os"`
	Arch     string `json:"arch"`
	CPUCount int    `json:"cpu_count"`

	PostgresVersion       string `json:"postgres_version"`
	PostgresSharedBuffers string `json:"postgres_shared_buffers"`
	PostgresMaxConns      string `json:"postgres_max_connections"`
	PostgresWALLevel      string `json:"postgres_wal_level"`

	// PoolMaxConns/PoolMinConns are read from the running api container's own
	// environment (LEDGER_POSTGRES_MAX_CONNS / LEDGER_POSTGRES_MIN_CONNS),
	// not restated from internal/config's documented defaults: the whole
	// point of the pool-tuning step in the optimisation cycle is to override
	// these, and a hardcoded default would silently go stale the first time
	// that step actually changes them.
	PoolMaxConns string `json:"pool_max_conns"`
	PoolMinConns string `json:"pool_min_conns"`
}

const environmentCaveat = "Single-node local/dev environment (Docker Compose on one developer machine), not a production cluster. Absolute numbers describe this machine; do not quote them as production capacity."

func gatherEnvironmentInfo(ctx context.Context, composeFile string, pool *pgxpool.Pool) (environmentInfo, error) {
	info := environmentInfo{
		Caveat:   environmentCaveat,
		OS:       runtime.GOOS,
		Arch:     runtime.GOARCH,
		CPUCount: runtime.NumCPU(),
	}

	if err := pool.QueryRow(ctx, `SELECT version()`).Scan(&info.PostgresVersion); err != nil {
		return environmentInfo{}, fmt.Errorf("query postgres version: %w", err)
	}
	if err := pool.QueryRow(ctx, `SHOW shared_buffers`).Scan(&info.PostgresSharedBuffers); err != nil {
		return environmentInfo{}, fmt.Errorf("query shared_buffers: %w", err)
	}
	if err := pool.QueryRow(ctx, `SHOW max_connections`).Scan(&info.PostgresMaxConns); err != nil {
		return environmentInfo{}, fmt.Errorf("query max_connections: %w", err)
	}
	if err := pool.QueryRow(ctx, `SHOW wal_level`).Scan(&info.PostgresWALLevel); err != nil {
		return environmentInfo{}, fmt.Errorf("query wal_level: %w", err)
	}

	info.PoolMaxConns = containerEnvOrDefault(ctx, composeFile, "api", "LEDGER_POSTGRES_MAX_CONNS", "20 (internal/config default; not overridden)")
	info.PoolMinConns = containerEnvOrDefault(ctx, composeFile, "api", "LEDGER_POSTGRES_MIN_CONNS", "2 (internal/config default; not overridden)")

	return info, nil
}

// containerEnvOrDefault reads one environment variable out of a running
// compose service. `docker compose exec printenv` exits non-zero when the
// variable is unset, which is expected whenever a step has not overridden
// it -- not an error, just the signal to report the documented default
// instead.
func containerEnvOrDefault(ctx context.Context, composeFile, service, key, fallback string) string {
	//nolint:gosec // G204: composeFile is this process's own CLI flag,
	// service/key are call-site literals ("api", "LEDGER_POSTGRES_MAX_CONNS")
	// -- never external input; see resolveContainerNames's identical
	// reasoning.
	cmd := exec.CommandContext(ctx, "docker", "compose", "-f", composeFile, "exec", "-T", service, "printenv", key)
	out, err := cmd.Output()
	if err != nil {
		return fallback
	}
	value := strings.TrimSpace(string(out))
	if value == "" {
		return fallback
	}
	return value
}
