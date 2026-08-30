package test

import (
	"context"
	"fmt"
	"os"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"
)

// One migrated container is shared by every test in this package. Starting a
// container per test would add minutes to the suite for no isolation benefit:
// the tests never delete each other's rows, because they cannot -- the journal
// is append-only -- so each one works on freshly generated account and
// transaction ids instead.
//
// TestMigrations_RoundTrip is the exception and starts its own container, since
// it needs to tear the schema down.
var sharedPool *pgxpool.Pool

func TestMain(m *testing.M) {
	code, err := runSuite(m)
	if err != nil {
		fmt.Fprintf(os.Stderr, "test setup failed: %v\n", err)
		os.Exit(1)
	}
	os.Exit(code)
}

func runSuite(m *testing.M) (int, error) {
	ctx := context.Background()

	dsn, stop, err := startPostgres(ctx)
	if err != nil {
		return 0, err
	}
	defer func() { _ = stop(ctx) }()
	sharedDSN = dsn

	migrator, err := newMigrator(dsn)
	if err != nil {
		return 0, err
	}
	if err := migrator.Up(); err != nil {
		return 0, fmt.Errorf("apply migrations: %w", err)
	}
	sourceErr, dbErr := migrator.Close()
	if sourceErr != nil {
		return 0, fmt.Errorf("close migration source: %w", sourceErr)
	}
	if dbErr != nil {
		return 0, fmt.Errorf("close migration database: %w", dbErr)
	}

	pool, err := newPool(ctx, dsn)
	if err != nil {
		return 0, err
	}
	defer pool.Close()
	sharedPool = pool

	return m.Run(), nil
}
