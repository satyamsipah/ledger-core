package test

import (
	"context"
	"errors"
	"testing"

	"github.com/golang-migrate/migrate/v4"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// expectedTables is every table the schema owns, excluding golang-migrate's own
// bookkeeping table.
var expectedTables = []string{
	"account_balances",
	"accounts",
	"balance_projections",
	"idempotency_keys",
	"journal_entries",
	"outbox",
	"processed_events",
	"transactions",
}

// expectedFunctions are the trigger functions the invariants depend on. They are
// asserted separately from tables because a down-migration that drops tables but
// leaves functions behind still passes a table-only check, and then collides on
// the next `up` with "function already exists" -- or worse, silently keeps a
// stale definition.
var expectedFunctions = []string{
	"ledger_assert_transaction_balanced",
	"ledger_create_account_balance",
	"ledger_reject_journal_mutation",
	// ledger_shard_account (migration 000012) was missing from this list
	// before now -- a pre-existing gap, not introduced here, but caught while
	// this list was already being extended for Phase 4's tables. Its absence
	// meant a down-migration dropping the function without a matching
	// reversal would have passed a table-only check.
	"ledger_shard_account",
	"ledger_sync_allow_negative",
	"set_updated_at",
}

// TestMigrations_RoundTrip proves every migration is genuinely reversible, which
// CLAUDE.md requires and which is impossible to verify by reading .down.sql
// files. It uses its own container because it tears the schema down.
func TestMigrations_RoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	dsn, stop, err := startPostgres(ctx)
	require.NoError(t, err)
	t.Cleanup(func() { _ = stop(context.Background()) })

	pool, err := newPool(ctx, dsn)
	require.NoError(t, err)
	t.Cleanup(pool.Close)

	migrator, err := newMigrator(dsn)
	require.NoError(t, err)
	t.Cleanup(func() {
		sourceErr, dbErr := migrator.Close()
		assert.NoError(t, sourceErr)
		assert.NoError(t, dbErr)
	})

	t.Run("should create the full schema when migrating up", func(t *testing.T) {
		require.NoError(t, migrator.Up())

		assert.ElementsMatch(t, expectedTables, listTables(t, ctx, pool))
		for _, fn := range expectedFunctions {
			assert.True(t, functionExists(t, ctx, pool, fn), "function %s should exist", fn)
		}
		assert.True(t, publicationExists(t, ctx, pool, "ledger_outbox_pub"))
		assert.True(t, constraintTriggerIsDeferred(t, ctx, pool, "journal_entries_balanced"),
			"the balance trigger must be DEFERRABLE INITIALLY DEFERRED")
	})

	t.Run("should leave nothing behind when migrating all the way down", func(t *testing.T) {
		require.NoError(t, migrator.Down())

		assert.Empty(t, listTables(t, ctx, pool), "down migrations must drop every table")
		for _, fn := range expectedFunctions {
			assert.False(t, functionExists(t, ctx, pool, fn),
				"function %s should have been dropped", fn)
		}
		assert.False(t, publicationExists(t, ctx, pool, "ledger_outbox_pub"),
			"the CDC publication should have been dropped")
	})

	t.Run("should rebuild the schema when migrating up a second time", func(t *testing.T) {
		require.NoError(t, migrator.Up())
		assert.ElementsMatch(t, expectedTables, listTables(t, ctx, pool))
	})

	t.Run("should step down and back up one version at a time", func(t *testing.T) {
		// Stepwise, because `down -all` can mask a migration whose reverse only
		// works when the ones after it have already been undone.
		for {
			err := migrator.Steps(-1)
			if errors.Is(err, migrate.ErrNoChange) {
				break
			}
			require.NoError(t, err)

			version, dirty, err := migrator.Version()
			if errors.Is(err, migrate.ErrNilVersion) {
				break
			}
			require.NoError(t, err)
			require.False(t, dirty, "migration %d left the database dirty", version)
		}

		require.NoError(t, migrator.Up())
		assert.ElementsMatch(t, expectedTables, listTables(t, ctx, pool))
	})
}

func listTables(t *testing.T, ctx context.Context, pool *pgxpool.Pool) []string {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT tablename FROM pg_tables
		 WHERE schemaname = 'public' AND tablename <> 'schema_migrations'
		 ORDER BY tablename`)
	require.NoError(t, err)
	defer rows.Close()

	var tables []string
	for rows.Next() {
		var name string
		require.NoError(t, rows.Scan(&name))
		tables = append(tables, name)
	}
	require.NoError(t, rows.Err())
	return tables
}

func functionExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) bool {
	t.Helper()

	var exists bool
	require.NoError(t, pool.QueryRow(ctx, `
		SELECT EXISTS (
			SELECT 1 FROM pg_proc p
			  JOIN pg_namespace n ON n.oid = p.pronamespace
			 WHERE n.nspname = 'public' AND p.proname = $1)`, name).Scan(&exists))
	return exists
}

func publicationExists(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) bool {
	t.Helper()

	var exists bool
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM pg_publication WHERE pubname = $1)`, name).Scan(&exists))
	return exists
}

// constraintTriggerIsDeferred reads pg_trigger directly. The deferral is the
// entire mechanism behind invariant 1, so it is asserted against the catalogue
// rather than inferred from behaviour alone.
func constraintTriggerIsDeferred(t *testing.T, ctx context.Context, pool *pgxpool.Pool, name string) bool {
	t.Helper()

	var deferrable, initiallyDeferred bool
	err := pool.QueryRow(ctx, `
		SELECT tgdeferrable, tginitdeferred
		  FROM pg_trigger
		 WHERE tgname = $1 AND NOT tgisinternal`, name).Scan(&deferrable, &initiallyDeferred)
	require.NoError(t, err, "trigger %s should exist", name)

	return deferrable && initiallyDeferred
}
