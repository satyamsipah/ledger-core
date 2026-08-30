package ledger

import (
	"fmt"
	"time"

	"github.com/google/uuid"
)

// Direction is which side of the journal an entry falls on. The sign of an
// entry lives here and never in its amount, matching
// journal_entries_amount_check.
type Direction string

// The two sides of the journal, matching journal_entries_direction_check.
const (
	DirectionDebit  Direction = "DEBIT"
	DirectionCredit Direction = "CREDIT"
)

// Valid mirrors journal_entries_direction_check.
func (d Direction) Valid() bool {
	return d == DirectionDebit || d == DirectionCredit
}

// Opposite returns the other side. This is the whole mechanism behind a
// reversal: mirroring every leg's direction and leaving the amounts untouched
// produces a transaction that exactly undoes the original, without a single
// subtraction and therefore without any chance of an arithmetic slip.
func (d Direction) Opposite() Direction {
	if d == DirectionDebit {
		return DirectionCredit
	}
	return DirectionDebit
}

// AccountType classifies an account in the accounting equation.
type AccountType string

// The five account types, matching accounts_account_type_check. There is no
// contra type: see DECISIONS.md D8 for why, and for the one constraint to drop
// the day a real contra account appears.
const (
	AccountTypeAsset     AccountType = "ASSET"
	AccountTypeLiability AccountType = "LIABILITY"
	AccountTypeEquity    AccountType = "EQUITY"
	AccountTypeRevenue   AccountType = "REVENUE"
	AccountTypeExpense   AccountType = "EXPENSE"
)

// Valid mirrors accounts_account_type_check.
func (t AccountType) Valid() bool {
	switch t {
	case AccountTypeAsset, AccountTypeLiability, AccountTypeEquity,
		AccountTypeRevenue, AccountTypeExpense:
		return true
	default:
		return false
	}
}

// NormalBalance returns the side on which this account type increases.
//
// It mirrors accounts_normal_balance_matches_type_check, which forbids the two
// from disagreeing. Both exist because the database constraint is what stays
// true across migrations and admin sessions, while this method is what lets Go
// code derive the sign without a round trip.
func (t AccountType) NormalBalance() Direction {
	switch t {
	case AccountTypeAsset, AccountTypeExpense:
		return DirectionDebit
	default:
		return DirectionCredit
	}
}

// AccountStatus gates whether an account may be posted to.
type AccountStatus string

// Account statuses, matching accounts_status_check.
const (
	AccountStatusActive AccountStatus = "ACTIVE"
	AccountStatusFrozen AccountStatus = "FROZEN"
	AccountStatusClosed AccountStatus = "CLOSED"
)

// Postable reports whether entries may be written against the account. Only
// ACTIVE qualifies: a FROZEN account is under investigation and a CLOSED one is
// history, and posting to either would be a correctness problem dressed as a
// convenience.
func (s AccountStatus) Postable() bool { return s == AccountStatusActive }

// TransactionStatus is a transaction's position in its lifecycle.
type TransactionStatus string

// Transaction statuses, matching transactions_status_check. The only transition
// this package performs on a committed transaction is POSTED -> REVERSED.
const (
	TransactionStatusPending  TransactionStatus = "PENDING"
	TransactionStatusPosted   TransactionStatus = "POSTED"
	TransactionStatusReversed TransactionStatus = "REVERSED"
)

// TransactionType records why a transaction exists.
type TransactionType string

// Transaction types, matching transactions_type_check. FX is declared but not
// yet reachable through PostTransaction, which posts a single currency per
// transaction; see ErrMixedCurrency.
const (
	TransactionTypeTransfer   TransactionType = "TRANSFER"
	TransactionTypePayin      TransactionType = "PAYIN"
	TransactionTypePayout     TransactionType = "PAYOUT"
	TransactionTypeFee        TransactionType = "FEE"
	TransactionTypeFX         TransactionType = "FX"
	TransactionTypeReversal   TransactionType = "REVERSAL"
	TransactionTypeAdjustment TransactionType = "ADJUSTMENT"
)

// Valid mirrors transactions_type_check.
func (t TransactionType) Valid() bool {
	switch t {
	case TransactionTypeTransfer, TransactionTypePayin, TransactionTypePayout,
		TransactionTypeFee, TransactionTypeFX, TransactionTypeReversal,
		TransactionTypeAdjustment:
		return true
	default:
		return false
	}
}

// Account is a node in the chart of accounts.
type Account struct {
	ID            uuid.UUID
	ExternalRef   string
	Type          AccountType
	NormalBalance Direction
	Currency      string
	OwnerID       *string
	AllowNegative bool
	Status        AccountStatus
	Version       int64
	CreatedAt     time.Time
	UpdatedAt     time.Time
}

// JournalEntry is one immutable leg of a transaction. There is no method here
// that mutates it, and the database rejects UPDATE and DELETE outright.
type JournalEntry struct {
	ID            uuid.UUID
	TransactionID uuid.UUID
	AccountID     uuid.UUID
	Direction     Direction
	Amount        Money
	EntrySeq      int
	CreatedAt     time.Time
}

// Transaction is the header a set of journal entries belongs to.
type Transaction struct {
	ID             uuid.UUID
	IdempotencyKey *string
	Type           TransactionType
	Status         TransactionStatus
	ExternalRef    *string
	Metadata       map[string]any
	CreatedAt      time.Time
	PostedAt       *time.Time
	Entries        []JournalEntry
}

// Balance is an account's position as held by the synchronous balance row.
type Balance struct {
	AccountID uuid.UUID
	Available Money
	Pending   Money
	Version   int64
	UpdatedAt time.Time
}

// ---------------------------------------------------------------------------
// THE TWO SIGN CONVENTIONS
//
// This system carries two different notions of "signed amount", they disagree
// with each other, and confusing them corrupts balances in a way that still
// passes the balance invariant. Both are correct in their own scope.
//
//  1. TRANSACTION SIGN, used to decide whether a transaction balances:
//         DEBIT = +, CREDIT = -
//     summed over one (transaction_id, currency) group, and required to be
//     exactly zero. It ignores account type entirely, because "debits equal
//     credits" is a property of the transaction, not of the accounts it
//     touches. This convention lives in the database, in
//     ledger_assert_transaction_balanced() in migration 000005, and is not
//     implemented in Go at all.
//
//  2. ACCOUNT SIGN, used to move an account's stored balance, which is what
//     signedAmount below computes:
//         direction == the account's normal balance  ->  +
//         direction != the account's normal balance  ->  -
//
// Why the second is not simply the first: a customer wallet is a LIABILITY and
// therefore CREDIT-normal -- the platform owes the user, so a credit increases
// what the user holds. The platform's bank account is an ASSET and therefore
// DEBIT-normal. Under convention 1, a CREDIT is negative in both cases. Storing
// that in account_balances.available_minor would make a funded wallet hold a
// negative balance, and account_balances_no_overdraft_check -- which reads
// `allow_negative OR available_minor >= 0` -- would fire on every wallet with
// money in it while ignoring genuinely overdrawn asset accounts.
//
// Signing by the account's own normal balance instead makes available_minor
// mean the same thing on every account regardless of type: how much value this
// account holds, counted in its natural direction. Positive is healthy
// everywhere, which is exactly what makes one CHECK constraint sufficient for
// the whole chart of accounts.
//
// The two conventions coincide for DEBIT-normal accounts, which is why a bug
// here survives any test written only against asset accounts. The tests use
// wallets on purpose.
// ---------------------------------------------------------------------------

// signedAmount converts one entry into the delta to apply to the balance of an
// account whose normal balance is normalBalance. See the block comment above
// for why the account's own normal balance, and not the entry direction alone,
// determines the sign.
//
// amountMinor is required to be strictly positive, mirroring
// journal_entries_amount_check: a negative amount would mean direction had been
// encoded twice, and the two encodings would cancel into a silently wrong sign
// rather than an error.
func signedAmount(direction, normalBalance Direction, amountMinor int64) (int64, error) {
	if !direction.Valid() {
		return 0, fmt.Errorf("direction %q: %w", direction, ErrInvalidEntry)
	}
	if !normalBalance.Valid() {
		return 0, fmt.Errorf("normal balance %q: %w", normalBalance, ErrInvalidEntry)
	}
	if amountMinor <= 0 {
		return 0, fmt.Errorf("amount %d must be positive: %w", amountMinor, ErrInvalidEntry)
	}

	if direction == normalBalance {
		return amountMinor, nil
	}
	return -amountMinor, nil
}
