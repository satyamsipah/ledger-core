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
// Phase 1 defines the schema and the domain error vocabulary. Posting logic
// arrives in Phase 2.
package ledger
