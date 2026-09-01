// Package pgledger implements the ledger's persistence port against
// PostgreSQL.
//
// Every statement that mutates the ledger lives here, and the reason they are
// gathered in one package rather than spread across the service is the locking:
// the deadlock-free ordering, the row locks that make the overdraft check
// meaningful, and the single transaction the deferred trigger needs are all
// properties of how these queries are issued, not of what they say.
package pgledger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgerrcode"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/db"
	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/idempotency/pgidem"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// Compile-time proof that this package satisfies the ports it claims to.
var (
	_ ledger.Repository = (*Repository)(nil)
	_ ledger.Tx         = (*txn)(nil)
)

// advisoryLockClass namespaces this service's advisory locks.
//
// The two-argument form of pg_advisory_xact_lock exists precisely so that
// unrelated subsystems can share a database without colliding in a lock space
// that is global to it. A single-argument lock keyed on an account hash would
// be one careless hash collision away from blocking on some other component's
// lock, and the resulting wait would be invisible in every query plan.
const advisoryLockClass = 0x4C454447 // "LEDG"

// Repository is the PostgreSQL-backed ledger repository.
type Repository struct {
	pool          *pgxpool.Pool
	timeout       time.Duration
	retrier       *db.Retrier
	advisoryLocks bool
}

// Option configures a Repository.
type Option func(*Repository)

// WithRetrier installs a retrier for aborted transactions. Without one a
// repository still works and simply surfaces 40001 and 40P01 to the caller,
// which is the Phase 2 behaviour.
func WithRetrier(retrier *db.Retrier) Option {
	return func(r *Repository) { r.retrier = retrier }
}

// WithAdvisoryLocks turns on per-account advisory locking.
//
// ALL OR NOTHING, PER PROCESS. Advisory locks live in a lock space entirely
// separate from row locks, so a deployment where some write paths take them and
// others do not has two independent lock orderings instead of the single global
// one D11 depends on -- and two orderings is exactly the shape a deadlock needs.
// The flag is therefore read once at startup and applied to every write path,
// never per request.
func WithAdvisoryLocks(enabled bool) Option {
	return func(r *Repository) { r.advisoryLocks = enabled }
}

// New builds a repository over an existing pool.
//
// The timeout bounds each logical operation rather than each statement: a
// posting transaction that holds account row locks past its budget is blocking
// every other writer touching those accounts, so the deadline that matters is
// the one on the whole unit of work, not on its individual round trips. A
// retried transaction is still one logical operation, so the budget covers
// every attempt rather than resetting for each -- otherwise a five-attempt
// retry would quietly consume five times the deadline the HTTP layer believes
// it granted.
func New(pool *pgxpool.Pool, timeout time.Duration, opts ...Option) *Repository {
	r := &Repository{pool: pool, timeout: timeout}
	for _, opt := range opts {
		opt(r)
	}
	return r
}

// InTx runs fn inside one READ COMMITTED transaction.
//
// # WHY READ COMMITTED AND NOT SOMETHING STRONGER
//
// Every invariant this ledger enforces is either a property of a single row
// (`allow_negative OR available_minor >= 0`, on one account) or of one
// transaction's own entries (the deferred sum, evaluated at COMMIT with every
// leg present). Neither is a predicate across rows, so there is no write skew
// here for SERIALIZABLE's predicate locking to catch -- it would add mandatory
// retries and, worse, take large predicate locks over the ranges of
// journal_entries that the statement and temporal queries scan, aborting
// writers that never conflicted with anything.
//
// REPEATABLE READ would catch the one anomaly that does threaten us, a lost
// update on a balance, but it catches it by aborting. A payments ledger has
// permanently hot accounts -- every pay-in credits the same house float -- and
// on those, aborting converts contention into wasted work plus a retry storm,
// where an explicit row lock converts it into a queue. Blocking degrades
// linearly; aborting degrades all at once.
//
// So: READ COMMITTED, and the lost update is prevented by taking the row lock
// explicitly in LockAccounts. The known weakness of that trade -- it is only
// correct while every write path remembers to lock -- is covered by the
// database itself, which enforces the overdraft CHECK and the balance trigger
// unconditionally. A path that forgets produces a loud constraint violation,
// not a quietly wrong balance.
//
// RETRIES, AND WHY THEY DO NOT CONTRADICT ANY OF THE ABOVE: the retrier only
// re-runs 40001 and 40P01, the two aborts PostgreSQL guarantees rolled back
// nothing. Under this isolation level and locking strategy neither should
// occur, which is the point -- ledger_db_tx_retries_total is how "should not
// occur" gets measured instead of assumed. See internal/db/retry.go.
func (r *Repository) InTx(ctx context.Context, fn func(context.Context, ledger.Tx) error) error {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	if r.retrier == nil {
		return r.runTx(ctx, fn)
	}
	return r.retrier.Do(ctx, "ledger_tx", func(ctx context.Context) error {
		return r.runTx(ctx, fn)
	})
}

// runTx is one attempt: begin, run, commit.
func (r *Repository) runTx(ctx context.Context, fn func(context.Context, ledger.Tx) error) error {
	pgTx, err := r.pool.BeginTx(ctx, pgx.TxOptions{
		IsoLevel:   pgx.ReadCommitted,
		AccessMode: pgx.ReadWrite,
	})
	if err != nil {
		return fmt.Errorf("begin ledger transaction: %w", err)
	}

	// Rollback on a context that cannot already be cancelled: if the deadline
	// above fired, a rollback issued on ctx would fail immediately and leave the
	// connection to be destroyed rather than returned to the pool.
	defer func() { _ = pgTx.Rollback(context.WithoutCancel(ctx)) }()

	if err := fn(ctx, &txn{tx: pgTx, advisoryLocks: r.advisoryLocks}); err != nil {
		return err
	}

	// COMMIT is where the deferred balance trigger runs, so an unbalanced
	// transaction fails here and nowhere earlier. Mapping the error at this
	// point is what turns that into ErrUnbalancedTransaction instead of a raw
	// check_violation surfacing from a commit call.
	if err := pgTx.Commit(ctx); err != nil {
		return fmt.Errorf("commit ledger transaction: %w", mapError(err))
	}

	return nil
}

// GetBalance reads the synchronous balance row.
func (r *Repository) GetBalance(ctx context.Context, accountID uuid.UUID) (ledger.Balance, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var (
		availableMinor int64
		pendingMinor   int64
		version        int64
		updatedAt      time.Time
		currency       string
	)
	// The physical CTE is what makes sharding invisible to every read: an
	// ordinary account resolves to itself, a sharded one resolves to itself
	// plus its shards, and the aggregate below is correct for both without a
	// branch. One join deep, which is why nested sharding is forbidden.
	//
	// SUM(version) rather than MAX, and it is not an approximation. Each
	// shard's version only ever increases by one per write, so their sum is
	// monotonic and advances by exactly one per logical write -- which is the
	// property the projector needs in order to discard a redelivered event. MAX
	// would stall whenever a write landed on a shard that was behind.
	err := r.pool.QueryRow(ctx, `
		WITH physical AS (
		    SELECT id FROM accounts WHERE id = $1
		    UNION ALL
		    SELECT id FROM accounts WHERE parent_account_id = $1
		)
		SELECT SUM(ab.available_minor), SUM(ab.pending_minor),
		       SUM(ab.version), MAX(ab.updated_at), a.currency
		  FROM physical p
		  JOIN account_balances ab ON ab.account_id = p.id
		  JOIN accounts a ON a.id = $1
		 GROUP BY a.currency`, accountID).
		Scan(&availableMinor, &pendingMinor, &version, &updatedAt, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Balance{}, fmt.Errorf("account %s: %w", accountID, ledger.ErrAccountNotFound)
	}
	if err != nil {
		return ledger.Balance{}, fmt.Errorf("read balance for account %s: %w", accountID, mapError(err))
	}

	available, err := ledger.NewMoney(availableMinor, currency)
	if err != nil {
		return ledger.Balance{}, fmt.Errorf("balance for account %s: %w", accountID, err)
	}
	pending, err := ledger.NewMoney(pendingMinor, currency)
	if err != nil {
		return ledger.Balance{}, fmt.Errorf("pending balance for account %s: %w", accountID, err)
	}

	return ledger.Balance{
		AccountID: accountID,
		Available: available,
		Pending:   pending,
		Version:   version,
		UpdatedAt: updatedAt,
	}, nil
}

// GetBalanceAsOf recomputes a balance from the journal at an instant.
//
// One statement, not several, which is what makes it correct under READ
// COMMITTED: a single statement sees one snapshot, so the baseline and the
// entries summed on top of it cannot straddle a concurrent commit and count the
// same money twice.
//
// THE SNAPSHOT SEAM: the `baseline` CTE is a constant zero today because there
// is no balance_snapshots table yet. When one arrives, only that CTE changes --
// it selects the newest snapshot at or before $2 -- and the rest of the query,
// including the (created_at, id) boundary that makes the hand-off exact, already
// works. The boundary is a pair rather than a timestamp on purpose: entries can
// share a created_at, and a snapshot cutting between two of them on time alone
// would either double-count or drop the ties.
func (r *Repository) GetBalanceAsOf(ctx context.Context, accountID uuid.UUID, at time.Time) (ledger.Money, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	var (
		currency     string
		balanceMinor int64
	)
	err := r.pool.QueryRow(ctx, `
		WITH acct AS (
		    SELECT id, currency, normal_balance FROM accounts WHERE id = $1
		),
		physical AS (
		    SELECT id FROM accounts WHERE id = $1
		    UNION ALL
		    SELECT id FROM accounts WHERE parent_account_id = $1
		),
		baseline AS (
		    SELECT 0::bigint                                       AS balance_minor,
		           '-infinity'::timestamptz                        AS through_at,
		           '00000000-0000-0000-0000-000000000000'::uuid    AS through_id
		)
		SELECT a.currency,
		       b.balance_minor
		         + COALESCE(SUM(CASE WHEN je.direction = a.normal_balance
		                             THEN je.amount_minor ELSE -je.amount_minor END), 0)
		  FROM acct a
		  CROSS JOIN baseline b
		  LEFT JOIN journal_entries je
		         ON je.account_id IN (SELECT id FROM physical)
		        AND (je.created_at, je.id) > (b.through_at, b.through_id)
		        AND je.created_at <= $2
		 GROUP BY a.currency, b.balance_minor`, accountID, at).
		Scan(&currency, &balanceMinor)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Money{}, fmt.Errorf("account %s: %w", accountID, ledger.ErrAccountNotFound)
	}
	if err != nil {
		return ledger.Money{}, fmt.Errorf("read balance for account %s as of %s: %w",
			accountID, at, mapError(err))
	}

	return ledger.NewMoney(balanceMinor, currency)
}

// GetStatement returns one keyset page with a running balance.
//
// Also a single statement, for the same reason as GetBalanceAsOf: the opening
// balance and the page it opens have to come from one snapshot, or a
// transaction committing between two queries would leave a statement whose
// running balance does not reconcile with its own lines.
func (r *Repository) GetStatement(ctx context.Context, q ledger.StatementQuery) (ledger.Statement, error) {
	ctx, cancel := context.WithTimeout(ctx, r.timeout)
	defer cancel()

	// The first page starts from (From, uuid.Nil). Every real entry id sorts
	// above the nil UUID, so a strict `>` still includes entries stamped
	// exactly at From -- which lets one exclusive comparison serve both the
	// first page and every page after it, instead of an inclusive variant for
	// the first that then has to avoid repeating a row.
	cursorAt, cursorID := q.From, uuid.Nil
	if q.After != nil {
		cursorAt, cursorID = q.After.CreatedAt, q.After.EntryID
	}

	// One extra row, discarded before returning, so "is there another page?" is
	// answered by fact rather than by the guess that a full page implies more.
	rows, err := r.pool.Query(ctx, `
		WITH acct AS (
		    SELECT id, currency, normal_balance FROM accounts WHERE id = $1
		),
		physical AS (
		    SELECT id FROM accounts WHERE id = $1
		    UNION ALL
		    SELECT id FROM accounts WHERE parent_account_id = $1
		),
		opening AS (
		    SELECT COALESCE(SUM(CASE WHEN je.direction = a.normal_balance
		                             THEN je.amount_minor ELSE -je.amount_minor END), 0) AS balance_minor
		      FROM acct a
		      LEFT JOIN journal_entries je
		             ON je.account_id IN (SELECT id FROM physical)
		            AND (je.created_at, je.id) <= ($2, $3)
		),
		page AS (
		    SELECT je.id, je.transaction_id, je.direction, je.amount_minor,
		           je.entry_seq, je.created_at,
		           CASE WHEN je.direction = a.normal_balance
		                THEN je.amount_minor ELSE -je.amount_minor END AS signed_minor
		      FROM acct a
		      JOIN journal_entries je ON je.account_id IN (SELECT id FROM physical)
		     WHERE (je.created_at, je.id) > ($2, $3)
		       AND je.created_at <= $4
		     ORDER BY je.created_at, je.id
		     LIMIT $5
		)
		SELECT a.currency,
		       o.balance_minor,
		       p.id, p.transaction_id, p.direction, p.amount_minor, p.entry_seq,
		       p.created_at, p.signed_minor,
		       o.balance_minor + SUM(p.signed_minor) OVER (
		           ORDER BY p.created_at, p.id
		           ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS running_minor
		  FROM acct a
		  CROSS JOIN opening o
		  LEFT JOIN page p ON TRUE
		 ORDER BY p.created_at, p.id`,
		q.AccountID, cursorAt, cursorID, q.To, q.Limit+1)
	if err != nil {
		return ledger.Statement{}, fmt.Errorf("read statement for account %s: %w", q.AccountID, mapError(err))
	}
	defer rows.Close()

	statement := ledger.Statement{
		AccountID: q.AccountID,
		From:      q.From,
		To:        q.To,
	}

	var openingMinor int64
	found := false

	for rows.Next() {
		// The LEFT JOIN against `page` guarantees at least one row even when the
		// period holds no entries, so the currency and opening balance come back
		// for an empty statement too. On that row every p.* column is NULL,
		// which is what the pointers below detect.
		var (
			currency      string
			openingScan   int64
			entryID       *uuid.UUID
			transactionID *uuid.UUID
			direction     *string
			amountMinor   *int64
			entrySeq      *int
			createdAt     *time.Time
			signedMinor   *int64
			runningMinor  *int64
		)
		if err := rows.Scan(&currency, &openingScan, &entryID, &transactionID, &direction,
			&amountMinor, &entrySeq, &createdAt, &signedMinor, &runningMinor); err != nil {
			return ledger.Statement{}, fmt.Errorf("scan statement row for account %s: %w", q.AccountID, err)
		}

		found = true
		statement.Currency = currency
		openingMinor = openingScan

		if entryID == nil {
			continue
		}

		signed, err := ledger.NewMoney(*signedMinor, currency)
		if err != nil {
			return ledger.Statement{}, fmt.Errorf("statement line %s: %w", *entryID, err)
		}
		running, err := ledger.NewMoney(*runningMinor, currency)
		if err != nil {
			return ledger.Statement{}, fmt.Errorf("statement line %s: %w", *entryID, err)
		}
		amount, err := ledger.NewMoney(*amountMinor, currency)
		if err != nil {
			return ledger.Statement{}, fmt.Errorf("statement line %s: %w", *entryID, err)
		}

		statement.Lines = append(statement.Lines, ledger.StatementLine{
			Entry: ledger.JournalEntry{
				ID:            *entryID,
				TransactionID: *transactionID,
				AccountID:     q.AccountID,
				Direction:     ledger.Direction(*direction),
				Amount:        amount,
				EntrySeq:      *entrySeq,
				CreatedAt:     *createdAt,
			},
			Signed:         signed,
			RunningBalance: running,
		})
	}
	if err := rows.Err(); err != nil {
		return ledger.Statement{}, fmt.Errorf("read statement for account %s: %w", q.AccountID, mapError(err))
	}
	if !found {
		return ledger.Statement{}, fmt.Errorf("account %s: %w", q.AccountID, ledger.ErrAccountNotFound)
	}

	if len(statement.Lines) > q.Limit {
		last := statement.Lines[q.Limit-1]
		statement.Lines = statement.Lines[:q.Limit]
		statement.NextCursor = &ledger.StatementCursor{
			CreatedAt: last.Entry.CreatedAt,
			EntryID:   last.Entry.ID,
		}
	}

	opening, err := ledger.NewMoney(openingMinor, statement.Currency)
	if err != nil {
		return ledger.Statement{}, fmt.Errorf("statement opening balance for account %s: %w", q.AccountID, err)
	}
	statement.Opening = opening
	statement.Closing = opening
	if n := len(statement.Lines); n > 0 {
		statement.Closing = statement.Lines[n-1].RunningBalance
	}

	return statement, nil
}

// txn implements ledger.Tx against one pgx transaction.
type txn struct {
	tx            pgx.Tx
	advisoryLocks bool
}

// takeAdvisoryLocks locks each account in the advisory lock space before any
// row lock is taken, when the feature is enabled.
//
// WHAT THIS BUYS, STATED HONESTLY: with the ordered row locking of D11 already
// in place, this does not remove contention -- it moves where writers queue.
// The gain is that they queue *earlier*: pg_advisory_xact_lock is a hash-table
// entry, so a blocked writer stops before touching the heap, before the
// visibility checks on accounts and account_balances, and before the buffer
// pins those take. On an extremely hot account that shortens the critical
// section by the cost of the reads the losers no longer perform. It is a
// second-order effect and it is measured rather than assumed; see
// docs/DECISIONS.md D25 for the benchmark that decides whether to enable it.
//
// A batch rather than one statement over unnest(), and the reason is the same
// caveat D11 records about ORDER BY: relying on the order in which PostgreSQL
// evaluates a function across the rows of a projection is relying on a plan
// shape, not on anything guaranteed. Statements in a batch execute in the order
// they were queued, which is a guarantee, and they still cost one round trip.
// ids is already in ascending order, which is what keeps this ordering the same
// one the row locks use.
func (t *txn) takeAdvisoryLocks(ctx context.Context, ids []uuid.UUID) error {
	if !t.advisoryLocks || len(ids) == 0 {
		return nil
	}

	batch := &pgx.Batch{}
	for _, id := range ids {
		// hashtext rather than a Go-side hash, so the key depends only on the
		// account id and stays identical across processes and language runtimes.
		// Collisions cost throughput -- two unrelated accounts sharing a queue --
		// and never correctness, because the row locks are still the thing that
		// actually serialises the write.
		batch.Queue(`SELECT pg_advisory_xact_lock($1::int, hashtext($2::text))`,
			advisoryLockClass, id.String())
	}

	results := t.tx.SendBatch(ctx, batch)
	for range ids {
		if _, err := results.Exec(); err != nil {
			_ = results.Close()
			return fmt.Errorf("take advisory account locks: %w", mapError(err))
		}
	}
	if err := results.Close(); err != nil {
		return fmt.Errorf("close advisory account lock batch: %w", mapError(err))
	}
	return nil
}

// ResolveShards returns the shard accounts of every sharded id among ids.
//
// Only children are returned, never the parent itself. A sharded parent may
// still hold a balance from before it was split -- ledger_shard_account
// deliberately does not move it -- and that balance is counted in the logical
// total, but routing new writes onto the parent would put 1/(n+1) of the
// traffic back on the single row this feature exists to relieve.
//
// The empty result is the fast path and the common one: an installation with no
// sharded accounts pays one indexed lookup that matches nothing, and no entry
// is rewritten.
func (t *txn) ResolveShards(ctx context.Context, ids []uuid.UUID) (map[uuid.UUID][]uuid.UUID, error) {
	rows, err := t.tx.Query(ctx, `
		SELECT parent_account_id, id
		  FROM accounts
		 WHERE parent_account_id = ANY($1::uuid[])
		   AND status = 'ACTIVE'
		 ORDER BY parent_account_id, shard_index`, ids)
	if err != nil {
		return nil, fmt.Errorf("resolve account shards: %w", mapError(err))
	}
	defer rows.Close()

	var shards map[uuid.UUID][]uuid.UUID
	for rows.Next() {
		var parent, shard uuid.UUID
		if err := rows.Scan(&parent, &shard); err != nil {
			return nil, fmt.Errorf("scan account shard: %w", err)
		}
		if shards == nil {
			shards = make(map[uuid.UUID][]uuid.UUID, 1)
		}
		shards[parent] = append(shards[parent], shard)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("resolve account shards: %w", mapError(err))
	}

	return shards, nil
}

// LockAccounts takes the row locks that serialise concurrent posting.
//
// TWO LOCK STRENGTHS, ON PURPOSE:
//
//   - FOR UPDATE on account_balances, the row this transaction is about to
//     change. This is the serialisation point: it makes the balance read below
//     and the write that follows atomic with respect to every other poster.
//
//   - FOR NO KEY UPDATE on accounts, which is read for status and currency but
//     never written. The weaker mode matters: inserting into journal_entries
//     takes FOR KEY SHARE on the referenced accounts row to enforce the foreign
//     key, and FOR UPDATE conflicts with FOR KEY SHARE while FOR NO KEY UPDATE
//     does not. Locking accounts FOR UPDATE would therefore make every posting
//     block every other posting's foreign key check, serialising transactions
//     that share no account at all.
//
// ORDER BY a.id is the deadlock prevention itself, not a tidy-output flourish:
// PostgreSQL places the LockRows node above the sort in the plan, so rows are
// locked in the order this query returns them, and every transaction therefore
// acquires a shared set of accounts in the same sequence. Locking the same set
// in caller order instead -- one statement per account, the obvious
// implementation -- produces real 40P01 deadlocks within seconds under the
// reverse-order concurrency test, which is what that test is for.
//
// Note that "LockRows sits above Sort" is a property of the plan shape rather
// than something the SQL standard promises, which is the other reason the
// guarantee is asserted by experiment rather than by reading.
//
// allow_negative is read from account_balances rather than from accounts
// because the denormalised copy is the one account_balances_no_overdraft_check
// actually evaluates. Reading the source of truth for the flag while the
// constraint reads the copy would let the two disagree without anything
// noticing.
func (t *txn) LockAccounts(ctx context.Context, ids []uuid.UUID) ([]ledger.LockedAccount, error) {
	// Before the row locks, never after, and in the same ascending id order.
	// A second lock space entered in a different order than the first is how a
	// deadlock gets built out of two individually correct orderings.
	if err := t.takeAdvisoryLocks(ctx, ids); err != nil {
		return nil, err
	}

	rows, err := t.tx.Query(ctx, `
		SELECT a.id, a.currency, a.account_type, a.normal_balance, a.status,
		       ab.allow_negative, ab.available_minor, ab.pending_minor, ab.version
		  FROM accounts a
		  JOIN account_balances ab ON ab.account_id = a.id
		 WHERE a.id = ANY($1::uuid[])
		 ORDER BY a.id
		   FOR NO KEY UPDATE OF a
		   FOR UPDATE OF ab`, ids)
	if err != nil {
		return nil, fmt.Errorf("lock accounts: %w", mapError(err))
	}
	defer rows.Close()

	locked := make([]ledger.LockedAccount, 0, len(ids))
	for rows.Next() {
		var a ledger.LockedAccount
		if err := rows.Scan(&a.ID, &a.Currency, &a.Type, &a.NormalBalance, &a.Status,
			&a.AllowNegative, &a.AvailableMinor, &a.PendingMinor, &a.Version); err != nil {
			return nil, fmt.Errorf("scan locked account: %w", err)
		}
		locked = append(locked, a)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("lock accounts: %w", mapError(err))
	}

	return locked, nil
}

// InsertTransaction writes the header.
//
// posted_at is derived in SQL from the status rather than passed in, so
// transactions_posted_at_check -- which requires the two to agree -- cannot be
// violated by a caller that sets one and forgets the other. Both timestamps
// come from the database clock for the same reason the ledger has one clock:
// application hosts drift, and a ledger ordered by several disagreeing clocks
// is not ordered.
func (t *txn) InsertTransaction(ctx context.Context, transaction *ledger.Transaction) error {
	metadata, err := marshalMetadata(transaction.Metadata)
	if err != nil {
		return fmt.Errorf("encode metadata for transaction %s: %w", transaction.ID, err)
	}

	err = t.tx.QueryRow(ctx, `
		INSERT INTO transactions (id, idempotency_key, transaction_type, status,
		                          external_ref, metadata, posted_at)
		VALUES ($1, $2, $3, $4, $5, $6,
		        CASE WHEN $4 = 'PENDING' THEN NULL ELSE now() END)
		RETURNING created_at, posted_at`,
		transaction.ID, transaction.IdempotencyKey, transaction.Type, transaction.Status,
		transaction.ExternalRef, metadata).
		Scan(&transaction.CreatedAt, &transaction.PostedAt)
	if err != nil {
		return fmt.Errorf("insert transaction %s: %w", transaction.ID, mapError(err))
	}

	return nil
}

// InsertEntries appends the journal rows for one transaction in a single
// statement.
//
// unnest rather than a batch of INSERTs: the legs of a transaction are written
// together on every posting, and one round trip instead of N keeps the account
// row locks held for as short a time as possible. Lock hold time is what
// determines throughput on a hot account, so it is worth a slightly denser
// statement.
func (t *txn) InsertEntries(ctx context.Context, entries []ledger.JournalEntry) error {
	if len(entries) == 0 {
		return nil
	}

	var (
		ids        = make([]uuid.UUID, len(entries))
		txIDs      = make([]uuid.UUID, len(entries))
		accountIDs = make([]uuid.UUID, len(entries))
		directions = make([]string, len(entries))
		amounts    = make([]int64, len(entries))
		currencies = make([]string, len(entries))
		seqs       = make([]int32, len(entries))
	)
	for i, e := range entries {
		ids[i] = e.ID
		txIDs[i] = e.TransactionID
		accountIDs[i] = e.AccountID
		directions[i] = string(e.Direction)
		amounts[i] = e.Amount.AmountMinor()
		currencies[i] = e.Amount.Currency()
		seqs[i] = int32(e.EntrySeq)
	}

	rows, err := t.tx.Query(ctx, `
		INSERT INTO journal_entries (id, transaction_id, account_id, direction,
		                             amount_minor, currency, entry_seq)
		SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[],
		                     $5::bigint[], $6::text[], $7::int[])
		RETURNING id, created_at`,
		ids, txIDs, accountIDs, directions, amounts, currencies, seqs)
	if err != nil {
		return fmt.Errorf("insert journal entries for transaction %s: %w",
			entries[0].TransactionID, mapError(err))
	}
	defer rows.Close()

	// RETURNING order is not specified, so timestamps are matched back by id
	// rather than by position. They are in fact all identical -- now() is fixed
	// for the whole transaction -- but relying on that would make this code
	// wrong the moment the default changed.
	created := make(map[uuid.UUID]time.Time, len(entries))
	for rows.Next() {
		var (
			id        uuid.UUID
			createdAt time.Time
		)
		if err := rows.Scan(&id, &createdAt); err != nil {
			return fmt.Errorf("scan inserted journal entry: %w", err)
		}
		created[id] = createdAt
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("insert journal entries for transaction %s: %w",
			entries[0].TransactionID, mapError(err))
	}

	for i := range entries {
		entries[i].CreatedAt = created[entries[i].ID]
	}

	return nil
}

// ApplyBalanceDelta moves one balance under its expected version.
//
// The `version = $4` predicate cannot fail while the caller holds this row's
// FOR UPDATE lock -- nothing else can write the row until this transaction ends.
// It is kept as a tripwire rather than as concurrency control: if it ever
// matches nothing, the lock was not held, and that is a bug to surface loudly
// rather than a conflict to retry. See ledger.ErrBalanceVersionConflict.
//
// The version bump itself is not decorative. It is what lets the Kafka-driven
// projector discard a redelivered event instead of applying a balance change
// twice, which matters because outbox delivery is at-least-once by design.
func (t *txn) ApplyBalanceDelta(ctx context.Context, d ledger.BalanceDelta) (ledger.Balance, error) {
	var (
		availableMinor int64
		pendingMinor   int64
		version        int64
		updatedAt      time.Time
		currency       string
	)
	err := t.tx.QueryRow(ctx, `
		UPDATE account_balances ab
		   SET available_minor = ab.available_minor + $2,
		       last_entry_id   = $3,
		       version         = ab.version + 1
		  FROM accounts a
		 WHERE ab.account_id = $1
		   AND a.id = ab.account_id
		   AND ab.version = $4
		RETURNING ab.available_minor, ab.pending_minor, ab.version, ab.updated_at, a.currency`,
		d.AccountID, d.DeltaMinor, d.LastEntryID, d.ExpectedVersion).
		Scan(&availableMinor, &pendingMinor, &version, &updatedAt, &currency)
	if errors.Is(err, pgx.ErrNoRows) {
		return ledger.Balance{}, fmt.Errorf("account %s at version %d: %w",
			d.AccountID, d.ExpectedVersion, ledger.ErrBalanceVersionConflict)
	}
	if err != nil {
		return ledger.Balance{}, fmt.Errorf("apply balance delta %d to account %s: %w",
			d.DeltaMinor, d.AccountID, mapError(err))
	}

	available, err := ledger.NewMoney(availableMinor, currency)
	if err != nil {
		return ledger.Balance{}, fmt.Errorf("balance for account %s: %w", d.AccountID, err)
	}
	pending, err := ledger.NewMoney(pendingMinor, currency)
	if err != nil {
		return ledger.Balance{}, fmt.Errorf("pending balance for account %s: %w", d.AccountID, err)
	}

	return ledger.Balance{
		AccountID: d.AccountID,
		Available: available,
		Pending:   pending,
		Version:   version,
		UpdatedAt: updatedAt,
	}, nil
}

// LoadTransactionForUpdate reads a transaction and its legs, holding a lock on
// the header row.
//
// The lock is what makes a reversal safe against a concurrent reversal of the
// same transaction: the status is read and acted on without any window between
// the two. Entries are read without a lock because they cannot change -- the
// journal rejects UPDATE and DELETE outright.
func (t *txn) LoadTransactionForUpdate(ctx context.Context, id uuid.UUID) (*ledger.Transaction, error) {
	var (
		transaction ledger.Transaction
		metadata    []byte
	)
	err := t.tx.QueryRow(ctx, `
		SELECT id, idempotency_key, transaction_type, status, external_ref,
		       metadata, created_at, posted_at
		  FROM transactions
		 WHERE id = $1
		   FOR UPDATE`, id).
		Scan(&transaction.ID, &transaction.IdempotencyKey, &transaction.Type,
			&transaction.Status, &transaction.ExternalRef, &metadata,
			&transaction.CreatedAt, &transaction.PostedAt)
	if errors.Is(err, pgx.ErrNoRows) {
		return nil, fmt.Errorf("transaction %s: %w", id, ledger.ErrTransactionNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("load transaction %s: %w", id, mapError(err))
	}

	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &transaction.Metadata); err != nil {
			return nil, fmt.Errorf("decode metadata for transaction %s: %w", id, err)
		}
	}

	rows, err := t.tx.Query(ctx, `
		SELECT id, transaction_id, account_id, direction, amount_minor, currency,
		       entry_seq, created_at
		  FROM journal_entries
		 WHERE transaction_id = $1
		 ORDER BY entry_seq`, id)
	if err != nil {
		return nil, fmt.Errorf("load entries for transaction %s: %w", id, mapError(err))
	}
	defer rows.Close()

	for rows.Next() {
		var (
			entry       ledger.JournalEntry
			amountMinor int64
			currency    string
		)
		if err := rows.Scan(&entry.ID, &entry.TransactionID, &entry.AccountID,
			&entry.Direction, &amountMinor, &currency, &entry.EntrySeq, &entry.CreatedAt); err != nil {
			return nil, fmt.Errorf("scan entry for transaction %s: %w", id, err)
		}

		amount, err := ledger.NewMoney(amountMinor, currency)
		if err != nil {
			return nil, fmt.Errorf("entry %s: %w", entry.ID, err)
		}
		entry.Amount = amount

		transaction.Entries = append(transaction.Entries, entry)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load entries for transaction %s: %w", id, mapError(err))
	}

	return &transaction, nil
}

// MarkReversed moves a POSTED transaction to REVERSED, and touches nothing else.
//
// The `status = 'POSTED'` predicate is a second guard behind the row lock the
// caller already holds. It would be sufficient on its own: under READ COMMITTED
// a blocked UPDATE re-evaluates its WHERE clause against the row version the
// winner committed, so a second reversal finds REVERSED and matches nothing.
func (t *txn) MarkReversed(ctx context.Context, id uuid.UUID) error {
	tag, err := t.tx.Exec(ctx, `
		UPDATE transactions
		   SET status = 'REVERSED'
		 WHERE id = $1 AND status = 'POSTED'`, id)
	if err != nil {
		return fmt.Errorf("mark transaction %s reversed: %w", id, mapError(err))
	}
	if tag.RowsAffected() == 0 {
		return fmt.Errorf("mark transaction %s reversed: %w", id, ledger.ErrTransactionNotPosted)
	}
	return nil
}

// AppendEvent writes the outbox row inside the caller's transaction, which is
// what keeps invariant 6 true without a distributed transaction.
func (t *txn) AppendEvent(ctx context.Context, e outbox.Event) error {
	return outbox.Append(ctx, t.tx, e)
}

// CompleteIdempotency marks the request's key COMPLETED inside the caller's
// transaction, which is what keeps invariant 5 true without a second write.
//
// Delegated to pgidem for the same reason AppendEvent delegates to outbox: the
// statement belongs with the table it owns, and what belongs here is the fact
// that it is issued against t.tx and not against a pool.
func (t *txn) CompleteIdempotency(ctx context.Context, c idempotency.Completion) error {
	return pgidem.Complete(ctx, t.tx, c)
}

func marshalMetadata(metadata map[string]any) ([]byte, error) {
	if len(metadata) == 0 {
		// The column is NOT NULL DEFAULT '{}'; sending an explicit empty object
		// keeps the value the same whether metadata was nil or absent.
		return []byte(`{}`), nil
	}
	return json.Marshal(metadata)
}

// mapError translates PostgreSQL errors into the domain sentinels declared in
// internal/ledger.
//
// This is where the database's enforcement becomes a usable API. Every
// constraint matched here has an application-level check in front of it that
// normally fires first; reaching this code means something got past that check,
// which is exactly when a caller most needs a typed error rather than a
// SQLSTATE.
func mapError(err error) error {
	var pgErr *pgconn.PgError
	if !errors.As(err, &pgErr) {
		return err
	}

	switch pgErr.Code {
	case pgerrcode.CheckViolation:
		switch {
		case pgErr.ConstraintName == "account_balances_no_overdraft_check":
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrInsufficientFunds)

		// The deferred balance trigger raises check_violation from a RAISE
		// EXCEPTION, which carries no constraint name -- the message is the only
		// thing distinguishing it from a table constraint.
		case strings.Contains(pgErr.Message, "does not balance"):
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrUnbalancedTransaction)

		case pgErr.ConstraintName == "journal_entries_amount_check":
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrInvalidEntry)
		}

	case pgerrcode.RestrictViolation:
		if strings.Contains(pgErr.Message, "append-only") {
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrImmutableJournal)
		}

	case pgerrcode.UniqueViolation:
		if pgErr.ConstraintName == "transactions_idempotency_key_key" {
			// The database's own defence of invariant 5, and the only one that
			// owes nothing to internal/idempotency being correct. It normally
			// stays silent because the idempotency record catches a duplicate
			// first; reaching it means the record was swept while the key it
			// describes lived on, which is exactly what a retry arriving after
			// the 24-hour TTL looks like. Closes the Phase 2 gap that left this
			// surfacing as a raw unique_violation.
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrDuplicateIdempotencyKey)
		}

	case pgerrcode.ForeignKeyViolation:
		switch pgErr.ConstraintName {
		case "journal_entries_account_currency_fkey":
			// This fires both for an unknown account and for an account holding
			// a different currency, because it is one composite key. The
			// currency reading is the useful one: an unknown account is caught
			// earlier, by LockAccounts returning no row for it.
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrCurrencyMismatch)
		case "journal_entries_transaction_id_fkey":
			return fmt.Errorf("%s: %w", pgErr.Message, ledger.ErrTransactionNotFound)
		}
	}

	return err
}
