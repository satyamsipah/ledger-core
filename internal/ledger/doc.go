// Package ledger holds the double-entry domain: accounts, transactions,
// journal entries, and the posting rules that keep them consistent.
//
// The database enforces the invariants this package is built around -- balanced
// transactions via a deferred constraint trigger, an append-only journal, and
// non-negative balances via a CHECK constraint. That is not redundancy to be
// tidied away later. Application-level checks describe what this code intends;
// the database constraints are what remain true when a migration, an admin
// session, or a future service does something this code never anticipated.
//
// Phase 2 adds the posting core: Money, the double-entry domain types,
// LedgerService.PostTransaction and ReverseTransaction, and the balance
// queries. There is still no HTTP layer -- the service is the boundary.
//
// Two comments in this package are worth reading before changing anything in
// it: the sign-convention block in types.go, which explains why an account's
// balance is not signed the same way its transactions are, and the isolation
// note on pgledger.Repository.InTx, which explains why the write path runs at
// READ COMMITTED and what that obliges every write path to do.
package ledger
