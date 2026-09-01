// Package idempotency makes write requests safe to retry.
//
// The guarantee (invariant 5 in CLAUDE.md): the same key with the same request
// body never creates a second transaction, at any level of concurrency.
//
// # THE ONE PROPERTY EVERYTHING ELSE RESTS ON
//
// A record in IN_PROGRESS is proof that no transaction committed under that
// key. It holds because the move to COMPLETED is executed inside the same
// database transaction as the journal entries -- pgidem.Complete takes a
// pgx.Tx, not a pool, so the broken version cannot be written -- and therefore
// the money and the record become durable together or not at all.
//
// Every other decision in this package is downstream of that one. A stale lease
// can be reclaimed with no fencing token, because a stale lease means no
// commit. A crashed request costs a delay rather than a duplicate, because the
// worst state it can leave behind is a reservation over work that never
// happened. And the failure mode the design exists to prevent -- money moved,
// key does not know it -- has no reachable state at all.
//
// # THE THREE DEFENCES, IN ORDER OF WHO GETS THERE FIRST
//
//  1. idempotency_keys.key, the primary key. Concurrent requests race to insert
//     it; exactly one wins and the losers replay, wait, or are refused.
//  2. The `status = 'IN_PROGRESS'` guard on the completing UPDATE. It resolves
//     the one race a lease permits -- two requests genuinely executing at once
//     after a reclaim -- by aborting the loser's transaction and discarding its
//     journal entries.
//  3. transactions_idempotency_key_key, the partial unique index added in
//     migration 000003. It fires inside the ledger transaction regardless of
//     anything this package does, so even a wholly broken lease implementation
//     produces a constraint violation rather than a second transaction.
//
// The first two are this package's; the third is the database's, and it is the
// one that would still be standing if the other two were deleted.
//
// # WHAT IS DELIBERATELY NOT LOAD-BEARING
//
// The reservation commits in its own transaction, before any ledger work. That
// is what makes IN_PROGRESS observable -- a row written in the ledger's
// transaction is invisible until it commits, by which point its status is
// already COMPLETED, so a 409 with Retry-After could never be issued. Losing a
// reservation, duplicating one, or abandoning one costs availability and never
// correctness, which is precisely why it is allowed to live outside the atomic
// step.
//
// The Cache is a read-through fast path holding terminal records only, and the
// service is correct with it switched off. NoopCache is the default. See
// docs/DECISIONS.md D20 through D23 for the full reasoning, the rejected
// alternatives, and the known gap around key namespacing before authentication
// exists.
package idempotency
