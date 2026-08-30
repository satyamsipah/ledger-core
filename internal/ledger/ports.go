package ledger

import (
	"context"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// Repository is the persistence port for the ledger.
//
// It exists to keep SQL out of the service, not to enable mocking: CLAUDE.md
// forbids mocking database behaviour, and rightly so, because every invariant
// this package is built on is enforced by PostgreSQL. A fake implementation
// would assert that a hand-written model of Postgres behaves the way the test
// author imagined, which proves nothing about the deferred trigger, the CHECK
// constraint, or row locking.
type Repository interface {
	// InTx runs fn inside a single READ COMMITTED database transaction and
	// commits it if fn returns nil.
	//
	// The whole posting path lives inside one call because the deferred balance
	// trigger only fires at COMMIT: splitting the work across two transactions
	// would let a half-posted transaction become visible and would move the
	// invariant check to a point where it can no longer reject anything useful.
	InTx(ctx context.Context, fn func(context.Context, Tx) error) error

	// GetBalance reads the synchronous, authoritative balance row.
	GetBalance(ctx context.Context, accountID uuid.UUID) (Balance, error)

	// GetBalanceAsOf recomputes a balance from the journal at an instant.
	GetBalanceAsOf(ctx context.Context, accountID uuid.UUID, at time.Time) (Money, error)

	// GetStatement returns one keyset-paginated page of entries with a running
	// balance.
	GetStatement(ctx context.Context, q StatementQuery) (Statement, error)
}

// Tx is the set of writes available inside one database transaction. Every
// method here assumes the caller already holds the relevant row locks, which
// LockAccounts is what provides.
type Tx interface {
	// LockAccounts takes the row locks that serialise concurrent posting, and
	// returns each account together with its current balance.
	//
	// ids must be sorted; see Service.PostTransaction for why the order is
	// what prevents deadlock.
	LockAccounts(ctx context.Context, ids []uuid.UUID) ([]LockedAccount, error)

	// InsertTransaction writes the header and fills in the timestamps the
	// database generated.
	InsertTransaction(ctx context.Context, t *Transaction) error

	// InsertEntries appends journal rows. There is no matching update or delete
	// anywhere in this interface, because the database has no matching update or
	// delete either.
	InsertEntries(ctx context.Context, entries []JournalEntry) error

	// ApplyBalanceDelta moves one account's balance, guarded by its expected
	// version, and returns the row as it now stands.
	ApplyBalanceDelta(ctx context.Context, d BalanceDelta) (Balance, error)

	// LoadTransactionForUpdate reads a transaction and its entries while holding
	// a row lock on the header, so a status transition decided from what it
	// returns cannot race another one.
	LoadTransactionForUpdate(ctx context.Context, id uuid.UUID) (*Transaction, error)

	// MarkReversed moves a POSTED transaction to REVERSED. This is the only
	// mutation the ledger ever performs on transaction history, and it touches
	// the status column alone.
	MarkReversed(ctx context.Context, id uuid.UUID) error

	// AppendEvent writes the outbox row that carries this change to Kafka.
	AppendEvent(ctx context.Context, e outbox.Event) error
}

// LockedAccount is an account and its balance, read under the row locks taken
// by LockAccounts. Every field the posting decision depends on is captured
// here, so that decision is made from one consistent, locked read rather than
// from several unsynchronised ones.
type LockedAccount struct {
	ID             uuid.UUID
	Currency       string
	Type           AccountType
	NormalBalance  Direction
	Status         AccountStatus
	AllowNegative  bool
	AvailableMinor int64
	PendingMinor   int64
	Version        int64
}

// BalanceDelta is one account's net movement for a single transaction.
//
// Entries are aggregated into one delta per account rather than applied leg by
// leg: a transaction touching the same account twice would otherwise take two
// version bumps for one logical change, and the read-side projection would see
// a version sequence it cannot reconcile against a single event.
type BalanceDelta struct {
	AccountID uuid.UUID

	// DeltaMinor is already signed by the account's normal balance. See the
	// sign-convention comment in types.go.
	DeltaMinor int64

	// ExpectedVersion is the version read under the lock. See
	// ErrBalanceVersionConflict for why a mismatch is a bug and not contention.
	ExpectedVersion int64

	// LastEntryID is the final entry of this transaction touching the account,
	// giving the balance row a pointer back into the journal that produced it.
	LastEntryID uuid.UUID
}

// StatementCursor is a keyset position in an account's journal.
//
// It is a pair rather than a timestamp because timestamps tie: several entries
// can share a created_at, and paginating on the timestamp alone either repeats
// or skips them at a page boundary. The id breaks the tie, and matches the
// ordering of journal_entries_account_id_created_at_idx.
type StatementCursor struct {
	CreatedAt time.Time
	EntryID   uuid.UUID
}

// StatementQuery selects one page of an account statement.
type StatementQuery struct {
	AccountID uuid.UUID
	From      time.Time
	To        time.Time
	Limit     int

	// After is the cursor returned by the previous page, or nil for the first.
	After *StatementCursor
}

// StatementLine is one entry with its effect on the account.
type StatementLine struct {
	Entry JournalEntry

	// Signed is the entry amount in the account's own direction: positive when
	// it increases what the account holds. Negative amounts are legal here and
	// nowhere else in the journal.
	Signed Money

	// RunningBalance is the account's balance immediately after this entry,
	// counted from the statement's opening balance.
	RunningBalance Money
}

// Statement is one page of an account's history.
type Statement struct {
	AccountID uuid.UUID
	Currency  string
	From      time.Time
	To        time.Time

	// Opening is the balance immediately before the first line of this page,
	// not before the requested period: on page three it is the closing balance
	// of page two, so the running totals stay continuous across pages.
	Opening Money
	Closing Money

	Lines []StatementLine

	// NextCursor is nil once the page is the last one.
	NextCursor *StatementCursor
}
