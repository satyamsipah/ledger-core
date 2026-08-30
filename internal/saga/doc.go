// Package saga orchestrates multi-step ledger operations that cannot be a
// single database transaction -- a payout that must reserve funds, call a
// gateway, and then post settlement.
//
// Compensation rather than rollback: once a step has committed, the only way
// back is a compensating transaction that posts reversing entries. The journal
// is append-only, so "undo" is not available at any layer, and a saga that
// assumes otherwise will corrupt the ledger rather than fail loudly.
//
// Phase 1 reserves the package. The orchestrator arrives in Phase 3.
package saga
