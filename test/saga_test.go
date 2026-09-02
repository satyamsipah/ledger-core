package test

import (
	"context"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	gatewaymock "github.com/satyamsipah/ledger-core/internal/gateway/mock"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/saga"
	"github.com/satyamsipah/ledger-core/internal/saga/payout"
	"github.com/satyamsipah/ledger-core/internal/saga/pgsaga"
)

const (
	payoutAmount = 10_000
	payoutFee    = 250
)

// TestSagaPayout_HappyPathSettlesToMerchantAndFees pins what success looks
// like, so that every failure test below is measured against a known shape
// rather than against an absence of errors.
func TestSagaPayout_HappyPathSettlesToMerchantAndFees(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

	gatewayServer, listener := startMockGateway(t)
	orchestrator, _, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

	instance, err := orchestrator.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
	require.NoError(t, err)

	final := driveTo(t, ctx, orchestrator, instance.ID, saga.StatusCompleted)
	assert.Equal(t, saga.StepDone, final.CurrentStep)

	wallet, _ := balanceOf(t, ctx, accounts.wallet)
	suspense, suspensePending := balanceOf(t, ctx, accounts.suspense)
	merchant, _ := balanceOf(t, ctx, accounts.merchant)
	fees, _ := balanceOf(t, ctx, accounts.fees)

	assert.Zero(t, wallet, "the whole wallet balance was paid out")
	assert.Zero(t, suspense, "suspense is a waypoint, never a destination")
	assert.Zero(t, suspensePending, "the hold must be released when the saga settles")
	assert.Equal(t, int64(payoutAmount-payoutFee), merchant)
	assert.Equal(t, int64(payoutFee), fees)

	assert.Equal(t, 1, gatewayServer.Count(), "the customer is charged exactly once")
	assert.Equal(t, 2, countSagaTransactions(t, ctx, instance.ID),
		"a successful payout is exactly two ledger transactions: reserve and settle")

	// SagaStepCompleted was declared in D32 against an orchestrator that did
	// not exist. Three steps, three events, written in the same transactions as
	// the money -- so this also asserts invariant 6 for the saga path.
	assert.Equal(t, 2, countSagaOutboxEvents(t, ctx, instance.ID, saga.EventTypeSagaStepCompleted),
		"each ledger step emits its event inside its own transaction")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSagaPayout_GatewayFailureCompensatesToTheExactPreSagaState is the core
// compensation guarantee: a failed saga leaves the ledger indistinguishable
// from one that never ran.
//
// "Indistinguishable" is asserted on balances, not on the journal, and the
// difference matters. The journal is append-only, so a compensated saga leaves
// two transactions behind forever -- the reserve and its reversal. That history
// is the point (D15: the correction is itself a fact). What must be restored is
// every balance, exactly, to the unit.
func TestSagaPayout_GatewayFailureCompensatesToTheExactPreSagaState(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

	gatewayServer, listener := startMockGateway(t)
	gatewayServer.SetBehaviour(gatewaymock.Behaviour{Outcome: "decline"})

	orchestrator, _, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

	before := takeSnapshot(t, ctx, accounts)

	instance, err := orchestrator.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
	require.NoError(t, err)

	driveTo(t, ctx, orchestrator, instance.ID, saga.StatusCompensated)

	after := takeSnapshot(t, ctx, accounts)
	assert.Equal(t, before, after,
		"a compensated saga must restore every balance exactly, including the pending hold")

	assert.Equal(t, 2, countSagaTransactions(t, ctx, instance.ID),
		"compensation is a reversing transaction, not an edit: reserve plus its mirror")

	var (
		forward      int
		compensating int
	)
	for _, a := range sagaAttempts(t, ctx, instance.ID) {
		if a.Direction == saga.DirectionCompensation {
			compensating++
			continue
		}
		forward++
	}
	assert.Equal(t, 1, compensating, "exactly one compensation ran")
	assert.Positive(t, forward, "the audit log records the forward attempts too")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSagaPayout_AmbiguousGatewayOutcomeIsResolvedByQueryNotByGuess is the
// worst case in the whole design: the gateway's outcome is unknown.
//
// Each sub-case is a REAL ambiguity -- a genuinely unanswered HTTP request, or
// a gateway that has been killed -- produced by the mock rather than by a flag
// in the orchestrator, per .claude/rules/testing.md. The property under test is
// that the saga never picks a side it cannot support: it asks, and if it cannot
// get an answer it stops rather than inventing one.
func TestSagaPayout_AmbiguousGatewayOutcomeIsResolvedByQueryNotByGuess(t *testing.T) {
	t.Parallel()

	t.Run("should settle when the probe finds the payment really succeeded", func(t *testing.T) {
		ctx := context.Background()
		service := newLedgerService(sharedPool)
		accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

		gatewayServer, listener := startMockGateway(t)
		// The payment is accepted and THEN the response is withheld. The
		// customer's money really has left; a saga that assumed failure here
		// would refund it and the merchant would never be paid.
		gatewayServer.SetBehaviour(gatewaymock.Behaviour{Hang: gatewaymock.HangAfterRecording})

		orchestrator, metrics, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

		instance, err := orchestrator.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
		require.NoError(t, err)

		unresolved := driveTo(t, ctx, orchestrator, instance.ID, saga.StatusGatewayPending)
		assert.Equal(t, saga.StepGateway, unresolved.CurrentStep)

		// While undecided, the money is parked in suspense: taken from the
		// customer, not given to the merchant. That is what makes waiting safe.
		walletMid, _ := balanceOf(t, ctx, accounts.wallet)
		suspenseMid, pendingMid := balanceOf(t, ctx, accounts.suspense)
		merchantMid, _ := balanceOf(t, ctx, accounts.merchant)
		assert.Zero(t, walletMid, "the wallet is debited before the gateway is called")
		assert.Equal(t, int64(payoutAmount), suspenseMid, "the money waits in suspense")
		assert.Equal(t, int64(payoutAmount), pendingMid, "and is marked as in flight")
		assert.Zero(t, merchantMid, "the merchant is not paid on an unknown outcome")

		// The gateway recovers. The saga asks, rather than assuming.
		gatewayServer.SetBehaviour(gatewaymock.Behaviour{})
		driveTo(t, ctx, orchestrator, instance.ID, saga.StatusCompleted)

		merchant, _ := balanceOf(t, ctx, accounts.merchant)
		_, pending := balanceOf(t, ctx, accounts.suspense)
		assert.Equal(t, int64(payoutAmount-payoutFee), merchant)
		assert.Zero(t, pending)
		assert.Equal(t, 1, gatewayServer.Count(), "the customer must be charged exactly once")
		assert.Positive(t, counterValue(t, metrics, "ledger_saga_gateway_probes_total",
			map[string]string{"outcome": "conclusive"}), "the outcome was resolved by a probe")

		assertGlobalInvariant(t, ctx, sharedPool)
	})

	t.Run("should compensate when the probe finds no payment was ever made", func(t *testing.T) {
		ctx := context.Background()
		service := newLedgerService(sharedPool)
		accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

		gatewayServer, listener := startMockGateway(t)
		// Withheld BEFORE acceptance: identical to the caller, opposite in
		// fact. No payment exists, so compensating is correct here -- and the
		// saga can only know that by asking.
		gatewayServer.SetBehaviour(gatewaymock.Behaviour{Hang: gatewaymock.HangBeforeRecording})

		orchestrator, _, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

		before := takeSnapshot(t, ctx, accounts)
		instance, err := orchestrator.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
		require.NoError(t, err)

		driveTo(t, ctx, orchestrator, instance.ID, saga.StatusGatewayPending)

		gatewayServer.SetBehaviour(gatewaymock.Behaviour{})
		driveTo(t, ctx, orchestrator, instance.ID, saga.StatusCompensated)

		assert.Equal(t, before, takeSnapshot(t, ctx, accounts),
			"a payment that never happened must leave the ledger exactly as it was")
		assert.Zero(t, gatewayServer.Count(), "no payment was ever accepted")

		assertGlobalInvariant(t, ctx, sharedPool)
	})

	t.Run("should need manual review when the gateway can never say what it did", func(t *testing.T) {
		ctx := context.Background()
		service := newLedgerService(sharedPool)
		accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

		gatewayServer, listener := startMockGateway(t)
		gatewayServer.SetBehaviour(gatewaymock.Behaviour{Hang: gatewaymock.HangAfterRecording})

		orchestrator, metrics, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

		instance, err := orchestrator.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
		require.NoError(t, err)

		driveTo(t, ctx, orchestrator, instance.ID, saga.StatusGatewayPending)

		// Kill the gateway outright. It held its payments in memory, so the
		// answer is now gone for good -- the genuinely unrecoverable case, and
		// the one automation must refuse to resolve.
		listener.Close()

		final := driveTo(t, ctx, orchestrator, instance.ID, saga.StatusNeedsManualReview)
		assert.NotEmpty(t, final.LastError, "the escalation must say what went wrong")

		suspense, pending := balanceOf(t, ctx, accounts.suspense)
		assert.Equal(t, int64(payoutAmount), suspense,
			"the money stays in suspense: a wrong state, but a named and audited one")
		assert.Equal(t, int64(payoutAmount), pending, "and stays marked as in flight")

		assert.Equal(t, float64(1), counterValue(t, metrics, "ledger_saga_manual_review_total",
			map[string]string{"reason": "gateway_outcome_unknown"}), "the alert must fire")
		assert.Equal(t, 1, countSagaOutboxEvents(t, ctx, instance.ID, saga.EventTypeSagaNeedsManualReview),
			"the alert event must be emitted, never silently dropped")

		assertGlobalInvariant(t, ctx, sharedPool)
	})
}

// TestSagaPayout_CompensationExhaustionNeedsManualReview drives the failure the
// requirements single out: the undo itself cannot run.
//
// The compensation is made to fail for a REAL reason rather than an injected
// one -- the customer's wallet is frozen between the reserve and the
// compensation, and a reversal goes through the same Postable() check as a
// fresh post (a Phase 2 property, deliberately unchanged). Nothing in the
// orchestrator knows this test exists.
func TestSagaPayout_CompensationExhaustionNeedsManualReview(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

	gatewayServer, listener := startMockGateway(t)
	gatewayServer.SetBehaviour(gatewaymock.Behaviour{Outcome: "decline"})

	orchestrator, metrics, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

	instance, err := orchestrator.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
	require.NoError(t, err)

	// Reserve first, so there is something to compensate, and only then make
	// compensating impossible.
	driveTo(t, ctx, orchestrator, instance.ID, saga.StatusReserved)
	setStatus(t, ctx, accounts.wallet, "FROZEN")

	final := driveTo(t, ctx, orchestrator, instance.ID, saga.StatusNeedsManualReview)

	assert.GreaterOrEqual(t, final.RetryCount, 3,
		"the compensation must exhaust its budget before a human is paged")
	assert.Contains(t, final.LastError, "not open for posting",
		"the escalation must carry the reason a human needs")

	// The money is stranded, and that is the correct outcome. What must NOT
	// have happened is an automatic adjustment papering over it.
	wallet, _ := balanceOf(t, ctx, accounts.wallet)
	suspense, pending := balanceOf(t, ctx, accounts.suspense)
	assert.Zero(t, wallet, "the debit stands: nothing invented money to put back")
	assert.Equal(t, int64(payoutAmount), suspense, "the funds are held, not lost")
	assert.Equal(t, int64(payoutAmount), pending, "and are still marked in flight")

	assert.Equal(t, float64(1), counterValue(t, metrics, "ledger_saga_manual_review_total",
		map[string]string{"reason": "compensation_exhausted"}))
	assert.Equal(t, 1, countSagaOutboxEvents(t, ctx, instance.ID, saga.EventTypeSagaNeedsManualReview))

	// Every failed attempt is on the record. "Tried four times and was refused
	// each time" is a different fact from "did nothing for an hour", and an
	// operator needs to be able to tell them apart.
	failed := 0
	for _, a := range sagaAttempts(t, ctx, instance.ID) {
		if a.Direction == saga.DirectionCompensation && a.Status == saga.StepFailed {
			failed++
		}
	}
	assert.GreaterOrEqual(t, failed, 3, "every compensation attempt is audited")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSagaPayout_CrashMidFlightResumesExactlyOnce kills the orchestrator's
// database connection at the one instant a crash could plausibly duplicate a
// side effect: inside the settle transaction, after its journal entries are
// inserted and before the saga is advanced.
//
// The crash is real -- pg_terminate_backend against a named backend, the same
// mechanism the outbox and idempotency crash tests use. A simulated error would
// exercise Go's error handling and say nothing about whether PostgreSQL rolled
// the transaction back.
func TestSagaPayout_CrashMidFlightResumesExactlyOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	accounts := newPayoutAccounts(t, ctx, service, payoutAmount)

	gatewayServer, listener := startMockGateway(t)
	survivor, _, sagaType := newOrchestrator(t, sharedPool, listener.URL, nil)

	instance, err := survivor.Start(ctx, accounts.payload(payoutAmount, payoutFee), "test-principal", nil)
	require.NoError(t, err)

	driveTo(t, ctx, survivor, instance.ID, saga.StatusGatewaySucceeded)
	require.Equal(t, 1, gatewayServer.Count(), "the gateway has been called once")

	// A second orchestrator, on its own addressable connection, takes the saga
	// and is killed inside the settle transaction.
	appName := "saga-orchestrator-crash-" + uuid.NewString()[:8]
	victimPool := newNamedPool(ctx, t, appName)
	victim, _, _ := newOrchestrator(t, victimPool, listener.URL, func(c *payout.Config) {
		c.SagaType = sagaType
		// A short lease so the survivor can reclaim the saga promptly once this
		// replica is gone, which is exactly what a dead replica's lease does.
		c.Lease = 500 * time.Millisecond
	})

	entered := make(chan struct{})
	killed := make(chan struct{})
	victim.WithCrashHook(func() {
		close(entered)
		<-killed
	})

	done := make(chan struct{})
	go func() {
		defer close(done)
		victim.DriveOnce(ctx)
	}()

	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("the settle transaction never reached the crash hook")
	}

	require.True(t, terminateBackend(ctx, t, sharedPool, appName),
		"the orchestrator's backend must be found and terminated for this test to mean anything")
	close(killed)

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("DriveOnce never returned after its connection was terminated")
	}

	// The transaction rolled back: no money moved, and the saga still describes
	// itself as it did before the attempt.
	merchantAfterCrash, _ := balanceOf(t, ctx, accounts.merchant)
	assert.Zero(t, merchantAfterCrash, "a rolled-back settle must have credited nobody")
	suspenseAfterCrash, pendingAfterCrash := balanceOf(t, ctx, accounts.suspense)
	assert.Equal(t, int64(payoutAmount), suspenseAfterCrash, "the funds are untouched")
	assert.Equal(t, int64(payoutAmount), pendingAfterCrash, "and the hold was not released")

	crashed := sagaInstance(t, ctx, instance.ID)
	assert.Equal(t, saga.StatusGatewaySucceeded, crashed.Status,
		"the saga must resume from the step that was persisted, not from one that half ran")

	// Restart: a different orchestrator picks the saga up and finishes it.
	driveTo(t, ctx, survivor, instance.ID, saga.StatusCompleted)

	merchant, _ := balanceOf(t, ctx, accounts.merchant)
	fees, _ := balanceOf(t, ctx, accounts.fees)
	_, pending := balanceOf(t, ctx, accounts.suspense)
	assert.Equal(t, int64(payoutAmount-payoutFee), merchant, "the merchant is paid exactly once")
	assert.Equal(t, int64(payoutFee), fees, "and the fee is taken exactly once")
	assert.Zero(t, pending, "the hold is released exactly once")

	assert.Equal(t, 1, gatewayServer.Count(), "the crash must not have caused a second charge")
	assert.Equal(t, 2, countSagaTransactions(t, ctx, instance.ID),
		"exactly two transactions survive: the reserve and one settle")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSagaPayout_ConcurrentSagasOnOneWalletCannotDoubleSpend runs a hundred
// payouts against a wallet that can afford forty of them.
//
// The guarantee is invariant 4's, not the saga's: the reserve step really
// debits the wallet, so the ordinary overdraft CHECK under the ordinary row
// lock is what refuses the forty-first. That is the whole argument for moving
// the money into suspense rather than merely flagging it as held -- a hold in
// pending_minor is invisible to that CHECK and would have refused nobody.
func TestSagaPayout_ConcurrentSagasOnOneWalletCannotDoubleSpend(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	const (
		sagas      = 100
		affordable = 40
		amount     = 100
		fee        = 10
	)

	service := newLedgerService(sharedPool)
	accounts := newPayoutAccounts(t, ctx, service, affordable*amount)

	_, listener := startMockGateway(t)
	orchestrator, _, sagaType := newOrchestrator(t, sharedPool, listener.URL, nil)

	// A hundred goroutines start a hundred sagas at once.
	ids := make([]uuid.UUID, sagas)
	group, groupCtx := errgroup.WithContext(ctx)
	start := make(chan struct{})
	for i := range sagas {
		group.Go(func() error {
			<-start
			instance, err := orchestrator.Start(groupCtx, accounts.payload(amount, fee), "test-principal", nil)
			if err != nil {
				return err
			}
			ids[i] = instance.ID
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait(), "starting a saga must never fail on contention alone")

	// Eight replicas drive them, so the lease and the guarded transition are
	// under real contention rather than driven one at a time.
	replicas := make([]*payout.Orchestrator, 8)
	for i := range replicas {
		replicas[i], _, _ = newOrchestrator(t, sharedPool, listener.URL, func(c *payout.Config) {
			c.SagaType = sagaType
		})
	}
	driveAllToTerminal(t, ctx, replicas, ids)

	store := pgsaga.New(sharedPool, 30*time.Second)
	var completed, failed atomic.Int64
	for _, id := range ids {
		instance, err := store.Get(ctx, id)
		require.NoError(t, err)
		switch instance.Status {
		case saga.StatusCompleted:
			completed.Add(1)
		case saga.StatusFailed:
			require.Contains(t, instance.LastError, ledger.ErrInsufficientFunds.Error(),
				"the only legitimate reason to refuse a payout here is an empty wallet")
			failed.Add(1)
		default:
			t.Fatalf("saga %s ended in %s (last error: %s)", id, instance.Status, instance.LastError)
		}
	}

	assert.Equal(t, int64(affordable), completed.Load(),
		"exactly the affordable number of payouts may succeed")
	assert.Equal(t, int64(sagas-affordable), failed.Load(),
		"every other payout must be refused, not queued and not partially applied")

	wallet, _ := balanceOf(t, ctx, accounts.wallet)
	suspense, pending := balanceOf(t, ctx, accounts.suspense)
	merchant, _ := balanceOf(t, ctx, accounts.merchant)
	fees, _ := balanceOf(t, ctx, accounts.fees)

	assert.Zero(t, wallet, "the wallet is drained to exactly zero, never below it")
	assert.Zero(t, suspense, "suspense nets to zero once every saga is terminal")
	assert.Zero(t, pending, "every hold is released")
	assert.Equal(t, int64(affordable*(amount-fee)), merchant)
	assert.Equal(t, int64(affordable*fee), fees)
	assert.Equal(t, int64(affordable*amount), journalBalance(t, ctx, accounts.merchant)+journalBalance(t, ctx, accounts.fees),
		"the journal, summed independently, must agree with the balance rows")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// driveAllToTerminal runs several orchestrator replicas concurrently until
// every saga is terminal.
func driveAllToTerminal(t *testing.T, ctx context.Context, replicas []*payout.Orchestrator, ids []uuid.UUID) {
	t.Helper()

	store := pgsaga.New(sharedPool, 30*time.Second)
	deadline := time.Now().Add(90 * time.Second)

	for time.Now().Before(deadline) {
		var wg sync.WaitGroup
		wg.Add(len(replicas))
		for _, replica := range replicas {
			go func() {
				defer wg.Done()
				replica.DriveOnce(ctx)
				replica.SweepOnce(ctx)
			}()
		}
		wg.Wait()

		outstanding := 0
		for _, id := range ids {
			instance, err := store.Get(ctx, id)
			require.NoError(t, err)
			if !instance.Status.Terminal() {
				outstanding++
			}
		}
		if outstanding == 0 {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}

	t.Fatal("sagas never all reached a terminal status")
}
