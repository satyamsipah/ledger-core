// Package consistency runs the checks that prove this ledger's invariants
// hold against the data actually at rest, rather than trusting that the
// write path enforced them.
//
// Every check here is a query, not a store. Unlike internal/reconciliation --
// which persists a run and its exceptions because a PSP file's findings are
// an audit trail someone reviews later -- these checks answer a yes/no
// question about the ledger's own internal state, cheaply and repeatably
// enough to run on a short ticker. The result is a metric and, when
// something is wrong, a log line naming the offending rows; there is no
// table of past findings to query, mirroring internal/projector.Rebuild's own
// shape rather than internal/reconciliation's.
//
// THREE CHECKS, THREE DIFFERENT REASONS THEY EXIST:
//
//   - CheckGlobalInvariant proves the deferred balance trigger (migration
//     000005) is still doing its job across the WHOLE journal, not merely on
//     the one transaction it last fired for. Grouped by currency, because
//     invariant 1 balances per (transaction_id, currency) -- summing every
//     currency together would let a bug that is wrong in two currencies at
//     once cancel out and hide.
//
//   - CheckProjectionDrift recomputes every account's balance directly from
//     journal_entries and diffs it against account_balances, the SYNCHRONOUS
//     balance the write path itself maintains under lock (D1). This is
//     deliberately not the same comparison internal/projector.Rebuild already
//     makes -- that one diffs against balance_projections, the
//     Kafka-DRIVEN read model. D1 names three independently derived
//     balances precisely so any two agreeing while a third dissents
//     localises the bug; this check is the second leg of that triangle, the
//     one nothing before Phase 6 verified on a schedule.
//
//   - CheckOrphans proves two structural claims: that no POSTED or REVERSED
//     transaction has fewer than two entries (a bug once posted, though
//     legitimate for a still-PENDING header -- see docs/DECISIONS.md,
//     Phase 1's known gap), and that no journal_entries row lacks a parent
//     transaction. The second is unconstructible given the foreign key
//     journal_entries carries -- checked anyway, because a query costs one
//     round trip and a migration mistake that dropped the constraint would
//     otherwise be invisible until something else broke.
package consistency
