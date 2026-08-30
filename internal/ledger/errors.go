package ledger

import "errors"

// Domain errors are sentinels so that callers can branch on errors.Is rather
// than matching strings, and so an HTTP handler can map a domain condition to a
// status code without knowing which repository produced it.
var (
	// ErrInsufficientFunds means a debit would take an account below zero and
	// the account is not flagged allow_negative. Callers should surface this to
	// the client as a 422, never retry it: retrying cannot make it succeed.
	ErrInsufficientFunds = errors.New("ledger: insufficient funds")

	// ErrUnbalancedTransaction means the entries submitted for a transaction do
	// not sum to zero within a currency. If this surfaces from the database
	// rather than from validation, a write path skipped its own checks and the
	// deferred trigger caught it at COMMIT.
	ErrUnbalancedTransaction = errors.New("ledger: transaction does not balance")

	// ErrCurrencyMismatch means an entry's currency differs from the account it
	// posts to. Enforced declaratively by a composite foreign key, so this is a
	// programming error rather than a user-facing condition.
	ErrCurrencyMismatch = errors.New("ledger: entry currency does not match account currency")

	// ErrAccountNotFound means no account exists for the given identifier.
	ErrAccountNotFound = errors.New("ledger: account not found")

	// ErrAccountNotPostable means the account exists but is FROZEN or CLOSED.
	ErrAccountNotPostable = errors.New("ledger: account is not open for posting")

	// ErrImmutableJournal means something attempted to UPDATE or DELETE a
	// journal entry. Corrections are made by posting a reversing transaction.
	ErrImmutableJournal = errors.New("ledger: journal entries are append-only")

	// ErrTransactionNotFound means no transaction exists for the given id.
	ErrTransactionNotFound = errors.New("ledger: transaction not found")

	// ErrTransactionNotPending means a transaction was already POSTED or
	// REVERSED and cannot make that transition again. Usually a duplicate
	// delivery of a saga step rather than a bug.
	ErrTransactionNotPending = errors.New("ledger: transaction is not pending")
)
