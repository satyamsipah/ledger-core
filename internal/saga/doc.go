// Package saga orchestrates multi-step ledger operations that cannot be a
// single database transaction -- a payout that must reserve funds, call a
// gateway, and then post settlement.
//
// Compensation rather than rollback: once a step has committed, the only way
// back is a compensating transaction that posts reversing entries. The journal
// is append-only, so "undo" is not available at any layer, and a saga that
// assumes otherwise will corrupt the ledger rather than fail loudly.
//
// This package holds the state machine's vocabulary and its persistence port,
// and deliberately imports nothing from internal/ledger. That is what lets
// internal/ledger import THIS package to expose AdvanceSaga on its Tx port, so
// a saga's state transition commits in the same database transaction as the
// money it describes. The orchestrator that drives the machine lives in
// internal/saga/payout, which may import both.
package saga
