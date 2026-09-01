package test

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/auth/pgauth"
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

// sharedAPIKey authenticates every HTTP-driven test in this package as one
// fixed principal, issued once here rather than per-test for the same reason
// sharedPool is shared: there is no isolation benefit to a fresh principal per
// test, since api_keys is never truncated either. Tests specifically about
// D24's principal scoping issue their OWN second principal via
// issuePrincipal, so the two are never confused.
const sharedTestPrincipal = "test-principal"

var sharedAPIKey string

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

	authStore := pgauth.New(pool, 30*time.Second)
	sharedAPIKey, err = authStore.Issue(ctx, sharedTestPrincipal)
	if err != nil {
		return 0, fmt.Errorf("issue shared test api key: %w", err)
	}

	return m.Run(), nil
}

// issuePrincipal mints a fresh API key for a distinct principal, for tests
// that need to prove two different callers cannot collide on an idempotency
// key -- see docs/DECISIONS.md D24.
func issuePrincipal(t *testing.T, ctx context.Context, principalID string) string {
	t.Helper()
	key, err := pgauth.New(sharedPool, 30*time.Second).Issue(ctx, principalID)
	if err != nil {
		t.Fatalf("issue api key for %s: %v", principalID, err)
	}
	return key
}
