// Package projector consumes ledger events from Kafka and maintains
// balance_projections, a denormalised read model built entirely from the
// event stream -- independently of account_balances, which the write path
// maintains synchronously inside the posting transaction.
//
// The independence is the point. account_balances is authoritative: it is
// what account_balances_no_overdraft_check evaluates, updated under the row
// lock that makes concurrent posting safe (D10, D11). balance_projections is
// a second, unrelated derivation of the same fact, computed by a Kafka
// consumer that has never taken a row lock and never will. Two numbers
// computed the same way agreeing proves nothing; two numbers computed by
// different code paths agreeing is what gives the reconciliation engine a
// real job, and what gives an operator confidence the event pipeline is
// telling the truth.
//
// # WHY APPLYING AN EVENT IS SAFE UNDER REDELIVERY AND OUT-OF-ORDER ARRIVAL
//
// Every event this package applies carries the RESULTING balance and version
// for each account it touched, not a delta (see
// ledger.transactionEvent.Balances). Applying it is therefore a compare-and-
// set: "set this account's projection to this value, but only if the
// incoming version is newer than what is already stored." That single
// property is what makes the apply path correct regardless of two things the
// outbox's at-least-once delivery does not otherwise promise -- a message
// arriving twice (the CAS is a no-op the second time) and two messages about
// different transactions on the same account arriving in an unexpected order
// (whichever has the higher version wins, converging to the same final state
// either way).
//
// processed_events sits alongside the CAS rather than replacing it, and
// Apply's doc comment explains exactly what gap it closes that version
// comparison alone does not.
//
// # WHAT THIS PACKAGE DOES NOT DO
//
// It never reads account_balances and never writes it. A projector that
// "corrected" the write path's own numbers would collapse the two
// independent derivations D1 (Phase 1) built reconciliation around back into
// one, and a bug shared by both sides of a comparison is invisible to it.
package projector
