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

	// ErrTransactionNotPosted means a reversal was requested for a transaction
	// that never posted. Reversing a PENDING transaction is not a correction,
	// it is a cancellation, and the saga owns that path.
	ErrTransactionNotPosted = errors.New("ledger: transaction has not posted")

	// ErrAlreadyReversed means the transaction has already been reversed.
	// Distinguished from ErrTransactionNotPosted because the two point at very
	// different bugs: a duplicate reversal request versus a wrong id.
	ErrAlreadyReversed = errors.New("ledger: transaction has already been reversed")

	// ErrReversalReasonRequired means a reversal arrived with no reason. A
	// reversal is the only way history is ever corrected, so an unexplained one
	// is an audit hole rather than a tidiness complaint.
	ErrReversalReasonRequired = errors.New("ledger: a reversal must carry a reason")

	// ErrMoneyOverflow means an arithmetic result does not fit in int64. Money
	// silently wrapping past 9.2e18 minor units would produce a balance of the
	// wrong sign, which is worse than any refusal to compute.
	ErrMoneyOverflow = errors.New("ledger: monetary amount overflows int64")

	// ErrInvalidCurrency means a currency is not a three-letter ISO-4217 code.
	// Mirrors accounts_currency_check so the same value is rejected whether it
	// arrives through this package or through raw SQL.
	ErrInvalidCurrency = errors.New("ledger: currency must be a three-letter ISO-4217 code")

	// ErrScaleMismatch means decoded JSON carried a scale that disagrees with
	// the currency's ISO-4217 exponent. Worth rejecting rather than ignoring: a
	// client sending {"amount":"1250","currency":"INR","scale":0} probably means
	// 1250 rupees, not 1250 paise, and accepting it silently loses a factor of
	// a hundred.
	ErrScaleMismatch = errors.New("ledger: scale does not match the currency's ISO-4217 exponent")

	// ErrMixedCurrency means one transaction's entries span several currencies.
	// The schema permits this -- the deferred trigger balances per currency
	// precisely so an FX transaction can carry both legs -- but the posting
	// service does not, until the FX rate handling that makes it meaningful
	// exists. See docs/DECISIONS.md, Phase 2.
	ErrMixedCurrency = errors.New("ledger: all entries in a transaction must share one currency")

	// ErrTooFewEntries means a transaction carried fewer than two entries.
	// A single leg cannot sum to zero, so this is caught here to produce a
	// domain error rather than a check_violation from the deferred trigger.
	ErrTooFewEntries = errors.New("ledger: a transaction needs at least two entries")

	// ErrInvalidEntry means an entry is malformed: unknown direction, or an
	// amount that is not strictly positive. Sign belongs in direction, never in
	// the amount -- a negative amount means someone encoded direction twice.
	ErrInvalidEntry = errors.New("ledger: journal entry is malformed")

	// ErrInvalidTransactionType means the transaction type is not one the
	// schema's CHECK constraint accepts.
	ErrInvalidTransactionType = errors.New("ledger: unknown transaction type")

	// ErrMissingRenderer means a request carried an idempotency key with no
	// ResponseRenderer. Committing that would leave the key IN_PROGRESS over a
	// transaction that really posted -- the one state the idempotency design
	// guarantees is unreachable -- so it fails the transaction instead.
	ErrMissingRenderer = errors.New("ledger: an idempotent request needs a response renderer")

	// ErrDuplicateIdempotencyKey means transactions_idempotency_key_key rejected
	// the insert: this key already names a transaction.
	//
	// Reaching this means the idempotency record was gone while the key it
	// describes was not -- almost always a retry arriving after the 24-hour TTL
	// swept the replay record. That is the designed outcome rather than a
	// failure: the record's expiry ends the ability to replay, never the
	// uniqueness of the key, so the retry is refused instead of posting a
	// second transaction.
	ErrDuplicateIdempotencyKey = errors.New("ledger: idempotency key already names a transaction")

	// ErrBalanceVersionConflict means a balance UPDATE guarded by
	// `WHERE version = $expected` matched no row while the posting path held
	// that row's lock. That combination is impossible unless something mutated
	// the row without taking the lock, so this is an internal invariant
	// violation and must never be retried -- a retry would simply re-read the
	// state that a lock was supposed to have protected.
	ErrBalanceVersionConflict = errors.New("ledger: balance changed while locked")
)
