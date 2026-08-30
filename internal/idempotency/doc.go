// Package idempotency makes write requests safe to retry.
//
// The guarantee (invariant 5 in CLAUDE.md): the same key with the same request
// body never creates a second transaction, at any level of concurrency.
//
// Correctness rests on the primary key of idempotency_keys and nothing else.
// Concurrent requests race to insert the key; exactly one wins, and the losers
// either replay the winner's stored response or wait for it. Redis, when
// configured, only short-circuits the read of an already-COMPLETED key -- the
// system stays correct with Redis switched off entirely, which is the only
// arrangement worth having.
//
// Phase 1 defines the schema and the error vocabulary. The store arrives in
// Phase 2.
package idempotency
