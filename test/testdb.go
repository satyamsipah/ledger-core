// Package test holds integration tests that run against a real PostgreSQL
// instance in a container.
//
// There are no mocks here and there will not be. Every invariant this project
// cares about -- deferred balance checking, append-only enforcement, overdraft
// rejection -- is implemented by the database. A mock would assert that the
// test's own model of Postgres behaves as the test expects, which is worth
// nothing.
package test

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	"github.com/testcontainers/testcontainers-go/wait"

	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/ledger/pgledger"
	"github.com/satyamsipah/ledger-core/migrations"
)

const (
	postgresImage = "postgres:16-alpine"
	dbName        = "ledger_test"
	dbUser        = "ledger"
	dbPassword    = "ledger"
)

// startPostgres brings up a container and returns its DSN. The container is
// torn down when the test (or TestMain, via the returned func) finishes.
func startPostgres(ctx context.Context) (dsn string, stop func(context.Context) error, err error) {
	container, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase(dbName),
		tcpostgres.WithUsername(dbUser),
		tcpostgres.WithPassword(dbPassword),
		// wal_level=logical so the container matches the compose stack and
		// CREATE PUBLICATION is exercised the way it will actually run.
		testcontainers.WithCmdArgs("-c", "wal_level=logical", "-c", "max_replication_slots=4"),
		testcontainers.WithWaitStrategy(
			// Occurrence 2: Postgres announces readiness once while running its
			// init scripts on a socket nobody can reach, then again for real.
			// Waiting for the first one produces connection-refused flakes.
			wait.ForLog("database system is ready to accept connections").
				WithOccurrence(2).
				WithStartupTimeout(90*time.Second),
		),
	)
	if err != nil {
		return "", nil, fmt.Errorf("start postgres container: %w", err)
	}

	dsn, err = container.ConnectionString(ctx, "sslmode=disable")
	if err != nil {
		_ = container.Terminate(ctx)
		return "", nil, fmt.Errorf("postgres connection string: %w", err)
	}

	// Wrapped rather than returned directly: Terminate takes variadic options,
	// which would leak into every caller's signature for no benefit.
	stop = func(ctx context.Context) error { return container.Terminate(ctx) }

	return dsn, stop, nil
}

// newMigrator builds a migrator over the embedded migration files, so tests
// apply byte-for-byte the same SQL that ships.
func newMigrator(dsn string) (*migrate.Migrate, error) {
	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return nil, fmt.Errorf("open embedded migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", source, migrateURL(dsn))
	if err != nil {
		return nil, fmt.Errorf("create migrator: %w", err)
	}
	return m, nil
}

// migrateURL rewrites a pgx DSN to the scheme golang-migrate's pgx/v5 driver
// registers itself under.
func migrateURL(dsn string) string {
	return "pgx5://" + strings.TrimPrefix(dsn, "postgres://")
}

// newPool opens a pool sized for the concurrency tests. The default of
// max(4, NumCPU) would serialise 100 goroutines down to a handful of
// connections and quietly turn a concurrency test into a sequential one.
func newPool(ctx context.Context, dsn string) (*pgxpool.Pool, error) {
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return nil, fmt.Errorf("parse test dsn: %w", err)
	}
	cfg.MaxConns = 25
	cfg.MinConns = 5

	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		return nil, fmt.Errorf("open test pool: %w", err)
	}
	return pool, nil
}

// newLedgerService builds a service over the shared pool.
//
// The timeout is deliberately generous. It bounds a whole posting transaction,
// and the concurrency tests deliberately queue two hundred writers on five
// accounts -- a production-sized budget would turn honest lock waiting into
// spurious failures and hide whatever the test was actually looking for.
func newLedgerService(pool *pgxpool.Pool) *ledger.LedgerService {
	return ledger.NewLedgerService(pgledger.New(pool, 30*time.Second))
}

// newAccount inserts an ACTIVE asset account, returning the id.
func newAccount(t *testing.T, ctx context.Context, pool *pgxpool.Pool, currency string, allowNegative bool) uuid.UUID {
	t.Helper()
	return newTypedAccount(t, ctx, pool, ledger.AccountTypeAsset, currency, allowNegative)
}

// newTypedAccount inserts an ACTIVE account of the given type, returning the id.
//
// Each call uses a fresh UUID and external_ref so tests never collide and never
// need to clean up after each other -- which matters here because
// journal_entries cannot be truncated, by design.
//
// The type matters more than it looks: an ASSET account is DEBIT-normal, so the
// two sign conventions coincide on it and a sign bug stays invisible. Tests
// that care about the balance sign use LIABILITY accounts, where they diverge.
func newTypedAccount(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	accountType ledger.AccountType,
	currency string,
	allowNegative bool,
) uuid.UUID {
	t.Helper()

	id := mustUUIDv7(t)
	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (id, external_ref, account_type, normal_balance,
		                      currency, owner_id, allow_negative, status)
		VALUES ($1, $2, $3, $4, $5, NULL, $6, 'ACTIVE')`,
		id, "test-"+id.String(), accountType, accountType.NormalBalance(), currency, allowNegative)
	require.NoError(t, err, "insert account")

	// The balance row is not inserted here: migration 000009 creates it from an
	// AFTER INSERT trigger on accounts, so that the posting path's
	// SELECT ... FOR UPDATE always has a row to lock. Inserting it again would
	// be redundant, and asserting it exists is worth more than creating it.
	var balances int
	require.NoError(t, pool.QueryRow(ctx,
		`SELECT count(*) FROM account_balances WHERE account_id = $1`, id).Scan(&balances))
	require.Equal(t, 1, balances, "creating an account must create its balance row")

	return id
}

// newTransaction inserts a PENDING transaction header inside tx.
func newTransaction(t *testing.T, ctx context.Context, tx pgx.Tx) uuid.UUID {
	t.Helper()

	id := mustUUIDv7(t)
	_, err := tx.Exec(ctx, `
		INSERT INTO transactions (id, transaction_type, status)
		VALUES ($1, 'TRANSFER', 'PENDING')`, id)
	require.NoError(t, err, "insert transaction")

	return id
}

// leg describes one journal entry to post.
type leg struct {
	account   uuid.UUID
	direction string
	amount    int64
	currency  string
}

// postLegs inserts entries inside tx, numbering entry_seq from zero. It returns
// the insert error rather than failing the test, because several tests are
// specifically about an insert that must be allowed to succeed even though the
// eventual COMMIT will not.
func postLegs(ctx context.Context, tx pgx.Tx, txID uuid.UUID, legs ...leg) error {
	for i, l := range legs {
		id, err := uuid.NewV7()
		if err != nil {
			return fmt.Errorf("generate entry id: %w", err)
		}
		_, err = tx.Exec(ctx, `
			INSERT INTO journal_entries (id, transaction_id, account_id, direction,
			                             amount_minor, currency, entry_seq)
			VALUES ($1, $2, $3, $4, $5, $6, $7)`,
			id, txID, l.account, l.direction, l.amount, l.currency, i)
		if err != nil {
			return fmt.Errorf("insert entry %d: %w", i, err)
		}
	}
	return nil
}

// assertGlobalInvariant asserts the property the whole system exists to
// preserve: no (transaction_id, currency) group in the journal has a non-zero
// signed sum. Every write-path test ends with this, per CLAUDE.md.
func assertGlobalInvariant(t *testing.T, ctx context.Context, pool *pgxpool.Pool) {
	t.Helper()

	rows, err := pool.Query(ctx, `
		SELECT transaction_id, currency,
		       SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) AS imbalance
		  FROM journal_entries
		 GROUP BY transaction_id, currency
		HAVING SUM(CASE WHEN direction = 'DEBIT' THEN amount_minor ELSE -amount_minor END) <> 0`)
	require.NoError(t, err, "query global invariant")
	defer rows.Close()

	var offenders []string
	for rows.Next() {
		var txID uuid.UUID
		var currency string
		var imbalance int64
		require.NoError(t, rows.Scan(&txID, &currency, &imbalance))
		offenders = append(offenders, fmt.Sprintf("%s/%s off by %d", txID, currency, imbalance))
	}
	require.NoError(t, rows.Err())
	require.Empty(t, offenders, "journal contains unbalanced transactions: %v", offenders)
}

func mustUUIDv7(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV7()
	require.NoError(t, err, "generate uuidv7")
	return id
}
