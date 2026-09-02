// Package reconciliation proves the ledger agrees with the outside world,
// instead of trusting that it does.
//
// THREE-WAY MATCH. Every run compares three independently derived views of
// the same external_ref: the ledger's own transactions table, the saga that
// (if any) drove the money, and a settlement file from the PSP -- a party
// outside this codebase's control entirely. Two agreeing while a third
// dissents localises the bug immediately, the same reasoning D1 and D38 in
// docs/DECISIONS.md already apply to account_balances, the projection, and
// the journal.
//
// WHERE THE JOIN HAPPENS. The three-way match is one SQL statement
// (pgreconciliation.Repository.Match), not a Go-side scan of three loaded
// tables: Postgres is already the source of truth for exactly this shape of
// set comparison, and asking it to do the join is both simpler and correct
// for a PSP file with an unbounded row count. What IS Go-side is
// classification (classify.go) -- turning a joined record into one of the
// six categories -- because that logic takes a configurable parameter (the
// timing window) and belongs with the rest of this package's business rules,
// not baked into a query string.
//
// LEDGER AMOUNT IS INVARIANT-DERIVED, NOT TYPE-SPECIFIC. A transaction's
// comparable amount is the sum of its DEBIT-side entries. That is not a
// convention this package invented -- it falls directly out of invariant 1
// (every transaction's signed entries sum to zero), which means DEBIT sum and
// CREDIT sum are always equal for any balanced transaction, in any currency,
// regardless of how many legs it has or what business event it represents.
// Using it here means this package never needs to know that a payout has a
// RESERVE leg and a SETTLE leg, or that a fee is optional -- it only needs
// the invariant every transaction in this ledger already satisfies.
//
// WHEN ONE external_ref NAMES SEVERAL TRANSACTIONS. The payout saga posts two
// -- RESERVE and SETTLE -- under the same external_ref (see
// internal/saga/payout). Match resolves that by taking the most recently
// created transaction for a given external_ref as the ledger's current
// position, the same way a human reconciling a reference with two ledger
// entries would read the later one as authoritative.
package reconciliation
