package test

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"golang.org/x/sync/errgroup"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// renderStub is a ResponseRenderer that reports the transaction it was handed.
// Real rendering belongs to the HTTP layer; what these tests need is only that
// something was stored and that it describes the right transaction.
func renderStub(t *testing.T) ledger.ResponseRenderer {
	t.Helper()
	return func(tx *ledger.Transaction) (int, []byte, error) {
		body, err := json.Marshal(map[string]string{"transaction_id": tx.ID.String()})
		return 201, body, err
	}
}

func transferRequest(t *testing.T, from, to uuid.UUID, minor int64, currency string) ledger.TransactionRequest {
	t.Helper()
	return ledger.TransactionRequest{
		Type: ledger.TransactionTypeTransfer,
		Entries: []ledger.EntryRequest{
			{AccountID: from, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(minor, currency)},
			{AccountID: to, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(minor, currency)},
		},
	}
}

// ageOut backdates a record so it looks like one written a day ago and now past
// its TTL.
//
// created_at moves with expires_at because idempotency_keys_expiry_check
// requires expires_at > created_at. Backdating the whole row rather than
// disabling the constraint keeps the fixture inside the set of states the
// service can actually produce, which is the only kind of state a test should
// assert against.
func ageOut(t *testing.T, ctx context.Context, key string) {
	t.Helper()

	_, err := sharedPool.Exec(ctx, `
		UPDATE idempotency_keys
		   SET created_at       = now() - interval '48 hours',
		       expires_at       = now() - interval '24 hours',
		       lease_expires_at = now() - interval '24 hours'
		 WHERE key = $1`, key)
	require.NoError(t, err, "age out idempotency record")
}

// TestIdempotency_ReserveIsExclusiveUnderConcurrency asserts the primary key is
// doing the job the schema comment claims for it.
//
// Everything else in this package builds on "exactly one request holds the key".
// If that is not true under 100 simultaneous inserts then no later assertion
// about double-posting means anything, so it is worth proving on its own before
// any ledger work is involved.
func TestIdempotency_ReserveIsExclusiveUnderConcurrency(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newIdempotencyStore(sharedPool)
	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{"a":1}`))
	require.NoError(t, err)

	const goroutines = 100
	var (
		mu   sync.Mutex
		wins int
	)

	group, groupCtx := errgroup.WithContext(ctx)
	start := make(chan struct{})
	for range goroutines {
		group.Go(func() error {
			<-start
			won, err := store.Reserve(groupCtx, idempotency.Reservation{
				Key:         key,
				Fingerprint: fingerprint,
				Method:      "POST",
				Route:       "/v1/transactions",
				TTL:         idempotency.DefaultTTL,
				Lease:       idempotency.DefaultLease,
			})
			if err != nil {
				return err
			}
			if won {
				mu.Lock()
				wins++
				mu.Unlock()
			}
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait())

	assert.Equal(t, 1, wins, "exactly one of %d concurrent reservations must win the key", goroutines)
}

// TestIdempotency_SameKeyConcurrentlyPostsExactlyOnce is invariant 5 stated as a
// test: 100 goroutines, one key, one transaction.
//
// The assertion is deliberately made against the transactions table rather than
// against idempotency_keys. The promise is about how much money moved, and a
// test that only counted idempotency records would pass just as happily on a
// system that wrote one record and two transactions.
func TestIdempotency_SameKeyConcurrentlyPostsExactlyOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	manager := newIdempotencyManager(t, sharedPool, idempotency.DefaultLease)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	body := []byte(`{"amount":"5000","currency":"INR"}`)
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", body)
	require.NoError(t, err)

	const goroutines = 100
	var (
		mu        sync.Mutex
		posted    int
		replayed  int
		inFlight  int
		otherErrs []error
	)

	group, groupCtx := errgroup.WithContext(ctx)
	start := make(chan struct{})
	for range goroutines {
		group.Go(func() error {
			<-start

			_, disposition, err := manager.Acquire(groupCtx, idempotency.Reservation{
				Key:         key,
				Fingerprint: fingerprint,
				Method:      "POST",
				Route:       "/v1/transactions",
			})

			switch {
			case idempotency.IsInProgress(err):
				// The honest outcome for a duplicate arriving while the first is
				// still running: refused fast, with a Retry-After, rather than
				// queued behind it holding a connection.
				mu.Lock()
				inFlight++
				mu.Unlock()
				return nil
			case err != nil:
				mu.Lock()
				otherErrs = append(otherErrs, err)
				mu.Unlock()
				return nil
			case disposition == idempotency.Replay:
				mu.Lock()
				replayed++
				mu.Unlock()
				return nil
			}

			request := transferRequest(t, from, to, 5000, "INR")
			request.Idempotency = &ledger.Idempotent{Key: key, Render: renderStub(t)}
			_, postErr := service.PostTransaction(groupCtx, request)

			mu.Lock()
			defer mu.Unlock()
			if postErr != nil {
				otherErrs = append(otherErrs, postErr)
				return nil
			}
			posted++
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait())

	assert.Empty(t, otherErrs, "no goroutine should fail for a reason other than in-progress")
	assert.Equal(t, 1, posted, "exactly one goroutine may execute the transaction")
	assert.Equal(t, goroutines, posted+replayed+inFlight, "every goroutine must reach a defined outcome")

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key),
		"invariant 5: one key, one transaction, whatever the concurrency")

	status, transactionID, found := idempotencyRecord(t, ctx, sharedPool, key)
	require.True(t, found)
	assert.Equal(t, "COMPLETED", status)
	require.NotNil(t, transactionID, "a COMPLETED record must name the transaction it created")

	// The money moved exactly once.
	balance, err := service.GetBalance(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, int64(5000), balance.Available.AmountMinor())

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestIdempotency_MutatedBodyIsRejected covers the 422 branch: same key,
// different content.
func TestIdempotency_MutatedBodyIsRejected(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	manager := newIdempotencyManager(t, sharedPool, idempotency.DefaultLease)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	original, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{"amount":"5000"}`))
	require.NoError(t, err)

	_, _, err = manager.Acquire(ctx, idempotency.Reservation{
		Key: key, Fingerprint: original, Method: "POST", Route: "/v1/transactions",
	})
	require.NoError(t, err)

	request := transferRequest(t, from, to, 5000, "INR")
	request.Idempotency = &ledger.Idempotent{Key: key, Render: renderStub(t)}
	_, err = service.PostTransaction(ctx, request)
	require.NoError(t, err)

	tests := []struct {
		name string
		body string
	}{
		{name: "should reject when a value changed", body: `{"amount":"9000"}`},
		{name: "should reject when a field was added", body: `{"amount":"5000","extra":1}`},
		{name: "should reject when a field was removed", body: `{}`},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mutated, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(tc.body))
			require.NoError(t, err)

			_, _, err = manager.Acquire(ctx, idempotency.Reservation{
				Key: key, Fingerprint: mutated, Method: "POST", Route: "/v1/transactions",
			})
			require.ErrorIs(t, err, idempotency.ErrIdempotencyConflict)
		})
	}

	// A body that is only cosmetically different is the same request, and must
	// replay rather than conflict. This is the half of the fingerprint contract
	// that a stricter implementation would break.
	t.Run("should replay when the body differs only in formatting", func(t *testing.T) {
		equivalent, err := idempotency.FingerprintOf("POST", "/v1/transactions",
			[]byte("{\n\t\"amount\" : \"5000\"\n}"))
		require.NoError(t, err)

		record, disposition, err := manager.Acquire(ctx, idempotency.Reservation{
			Key: key, Fingerprint: equivalent, Method: "POST", Route: "/v1/transactions",
		})
		require.NoError(t, err)
		assert.Equal(t, idempotency.Replay, disposition)
		require.NotNil(t, record)
		assert.Equal(t, 201, record.ResponseStatus)
	})

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key))
	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestIdempotency_CrashBeforeCompletionLeavesNoTransaction is the crash-window
// test, and it kills a real connection to produce the crash.
//
// The window it targets is the one the whole design is built around: the ledger
// transaction has inserted its journal entries and moved its balances, and the
// idempotency record has not yet been marked COMPLETED. A process dying here is
// the scenario that makes a two-transaction design double-post.
//
// The renderer runs at exactly that point -- after the entries, before the
// completion -- so blocking in it holds the transaction open in precisely the
// dangerous state while another session terminates the backend. What the
// assertions then check is that PostgreSQL rolled the whole thing back: no
// transaction, no entries, and a record still IN_PROGRESS. IN_PROGRESS is what
// makes the retry safe, because it is proof the money never moved.
func TestIdempotency_CrashBeforeCompletionLeavesNoTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	appName := "ledger-crash-" + uuid.NewString()[:8]
	victimPool := newNamedPool(ctx, t, appName)
	victimService := newLedgerService(victimPool)

	store := newIdempotencyStore(sharedPool)
	manager := newIdempotencyManager(t, sharedPool, 2*time.Second)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{"amount":"7500"}`))
	require.NoError(t, err)

	_, _, err = manager.Acquire(ctx, idempotency.Reservation{
		Key: key, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
	})
	require.NoError(t, err)

	// The renderer parks the transaction in the dangerous state and signals the
	// killer, rather than the killer racing a sleep. entered/killed make the
	// ordering a fact instead of a timing guess.
	entered := make(chan struct{})
	killed := make(chan struct{})

	request := transferRequest(t, from, to, 7500, "INR")
	request.Idempotency = &ledger.Idempotent{
		Key: key,
		Render: func(tx *ledger.Transaction) (int, []byte, error) {
			close(entered)
			<-killed
			return 201, []byte(`{"transaction_id":"` + tx.ID.String() + `"}`), nil
		},
	}

	var postErr error
	done := make(chan struct{})
	go func() {
		defer close(done)
		_, postErr = victimService.PostTransaction(ctx, request)
	}()

	select {
	case <-entered:
	case <-time.After(20 * time.Second):
		t.Fatal("posting transaction never reached the render step")
	}

	require.True(t, terminateBackend(ctx, t, sharedPool, appName),
		"the posting backend must be found and terminated for this test to mean anything")
	close(killed)

	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("posting transaction never returned after its connection was terminated")
	}

	require.Error(t, postErr, "a terminated connection must surface as an error, never as a silent success")

	// The three assertions that matter, in the order they would fail.
	assert.Equal(t, 0, countTransactionsWithKey(t, ctx, sharedPool, key),
		"a transaction whose connection died before COMMIT must leave no row")

	status, transactionID, found := idempotencyRecord(t, ctx, sharedPool, key)
	require.True(t, found, "the reservation committed on its own and must survive the crash")
	assert.Equal(t, "IN_PROGRESS", status,
		"IN_PROGRESS is the proof that no transaction committed; COMPLETED here would be the double-post bug")
	assert.Nil(t, transactionID)

	balance, err := newLedgerService(sharedPool).GetBalance(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, int64(0), balance.Available.AmountMinor(), "no money may have moved")

	// The retry. Its lease has to expire first, which is what the short lease on
	// this manager is for; then the same key executes cleanly and exactly once.
	require.Eventually(t, func() bool {
		won, err := store.Reclaim(ctx, "", key, idempotency.DefaultLease)
		return err == nil && won
	}, 10*time.Second, 200*time.Millisecond, "the abandoned lease must become reclaimable")

	retry := transferRequest(t, from, to, 7500, "INR")
	retry.Idempotency = &ledger.Idempotent{Key: key, Render: renderStub(t)}
	_, err = newLedgerService(sharedPool).PostTransaction(ctx, retry)
	require.NoError(t, err, "the retry must succeed once the abandoned lease is reclaimed")

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key),
		"the crash and the retry together must still produce exactly one transaction")

	status, transactionID, _ = idempotencyRecord(t, ctx, sharedPool, key)
	assert.Equal(t, "COMPLETED", status)
	assert.NotNil(t, transactionID)

	balance, err = newLedgerService(sharedPool).GetBalance(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, int64(7500), balance.Available.AmountMinor())

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestIdempotency_LeaseIsOnlyReclaimableOnceExpired pins the guard that stops a
// live request being trampled.
func TestIdempotency_LeaseIsOnlyReclaimableOnceExpired(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newIdempotencyStore(sharedPool)
	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{}`))
	require.NoError(t, err)

	won, err := store.Reserve(ctx, idempotency.Reservation{
		Key: key, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: 2 * time.Second,
	})
	require.NoError(t, err)
	require.True(t, won)

	reclaimed, err := store.Reclaim(ctx, "", key, idempotency.DefaultLease)
	require.NoError(t, err)
	assert.False(t, reclaimed, "a live lease must not be reclaimable")

	require.Eventually(t, func() bool {
		reclaimed, err := store.Reclaim(ctx, "", key, 30*time.Second)
		return err == nil && reclaimed
	}, 10*time.Second, 200*time.Millisecond, "an expired lease must become reclaimable")

	// And only once: the winner pushed the deadline into the future, so a second
	// reclaimer re-evaluates the guard and finds nothing to take.
	again, err := store.Reclaim(ctx, "", key, idempotency.DefaultLease)
	require.NoError(t, err)
	assert.False(t, again, "only one reclaimer may win a given lease")
}

// TestIdempotency_ConcurrentReclaimersProduceOneWinner is the same guard under
// real contention rather than in sequence.
func TestIdempotency_ConcurrentReclaimersProduceOneWinner(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newIdempotencyStore(sharedPool)
	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{}`))
	require.NoError(t, err)

	won, err := store.Reserve(ctx, idempotency.Reservation{
		Key: key, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: time.Second,
	})
	require.NoError(t, err)
	require.True(t, won)

	time.Sleep(1500 * time.Millisecond)

	const goroutines = 100
	var (
		mu   sync.Mutex
		wins int
	)

	group, groupCtx := errgroup.WithContext(ctx)
	start := make(chan struct{})
	for range goroutines {
		group.Go(func() error {
			<-start
			won, err := store.Reclaim(groupCtx, "", key, 30*time.Second)
			if err != nil {
				return err
			}
			if won {
				mu.Lock()
				wins++
				mu.Unlock()
			}
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait())

	assert.Equal(t, 1, wins, "exactly one of %d simultaneous reclaimers may win", goroutines)
}

// TestIdempotency_ReleaseCannotRemoveACompletedRecord covers the guard that
// turns the most dangerous ordering into a no-op.
//
// The scenario: the ledger transaction committed, and the process then failed on
// its way to writing the response, so the deferred release runs against a record
// that is already COMPLETED. An unguarded DELETE here would erase the record of
// a transaction that really posted and free the key to post a second one.
func TestIdempotency_ReleaseCannotRemoveACompletedRecord(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	store := newIdempotencyStore(sharedPool)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{"amount":"1200"}`))
	require.NoError(t, err)

	won, err := store.Reserve(ctx, idempotency.Reservation{
		Key: key, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: idempotency.DefaultLease,
	})
	require.NoError(t, err)
	require.True(t, won)

	request := transferRequest(t, from, to, 1200, "INR")
	request.Idempotency = &ledger.Idempotent{Key: key, Render: renderStub(t)}
	_, err = service.PostTransaction(ctx, request)
	require.NoError(t, err)

	require.NoError(t, store.Release(ctx, "", key), "release must not error on a completed record")

	status, transactionID, found := idempotencyRecord(t, ctx, sharedPool, key)
	require.True(t, found, "releasing a COMPLETED record must be a no-op, not a delete")
	assert.Equal(t, "COMPLETED", status)
	assert.NotNil(t, transactionID)

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestIdempotency_CompletingALostLeaseAbortsTheTransaction is the third defence
// under test: two executions genuinely racing after a reclaim.
//
// The loser's completing UPDATE matches no IN_PROGRESS row, which returns
// ErrLeaseLost from inside the ledger transaction and takes its journal entries
// down with it. Without that guard both executions would commit, each balanced
// and each individually valid, and the balance invariant would not notice the
// money moving twice.
func TestIdempotency_CompletingALostLeaseAbortsTheTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	store := newIdempotencyStore(sharedPool)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{"amount":"400"}`))
	require.NoError(t, err)

	won, err := store.Reserve(ctx, idempotency.Reservation{
		Key: key, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: idempotency.DefaultLease,
	})
	require.NoError(t, err)
	require.True(t, won)

	// Stand in for the winner of a reclaim race by completing the key from
	// another transaction while this one is mid-flight.
	request := transferRequest(t, from, to, 400, "INR")
	request.Idempotency = &ledger.Idempotent{
		Key: key,
		Render: func(tx *ledger.Transaction) (int, []byte, error) {
			require.NoError(t, store.Fail(ctx, "", key, 422, []byte(`{"error":"taken"}`)))
			return 201, []byte(`{"transaction_id":"` + tx.ID.String() + `"}`), nil
		},
	}

	_, err = service.PostTransaction(ctx, request)
	require.ErrorIs(t, err, idempotency.ErrLeaseLost,
		"losing the lease mid-transaction must abort rather than commit alongside the winner")

	assert.Equal(t, 0, countTransactionsWithKey(t, ctx, sharedPool, key),
		"the loser's journal entries must have rolled back with its completion")

	balance, err := service.GetBalance(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, int64(0), balance.Available.AmountMinor())

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestIdempotency_ExpiredKeyIsRefusedRatherThanReexecuted covers the decision
// that the TTL bounds storage and never correctness.
//
// After the replay record is swept the key is gone from idempotency_keys, but it
// still names a transaction. A retry arriving now must be refused by
// transactions_idempotency_key_key rather than posting a second transaction --
// the expiry ended the ability to replay, not the uniqueness of the key.
func TestIdempotency_ExpiredKeyIsRefusedRatherThanReexecuted(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	store := newIdempotencyStore(sharedPool)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{"amount":"600"}`))
	require.NoError(t, err)

	won, err := store.Reserve(ctx, idempotency.Reservation{
		Key: key, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: idempotency.DefaultLease,
	})
	require.NoError(t, err)
	require.True(t, won)

	request := transferRequest(t, from, to, 600, "INR")
	request.Idempotency = &ledger.Idempotent{Key: key, Render: renderStub(t)}
	_, err = service.PostTransaction(ctx, request)
	require.NoError(t, err)

	// Age the record out and sweep it, which is what would happen 24 hours on.
	// created_at moves back with it: idempotency_keys_expiry_check requires
	// expires_at > created_at, so a record cannot be aged by pushing only its
	// expiry into the past. The constraint is right to refuse that -- it is not
	// a state the service can ever produce.
	ageOut(t, ctx, key)

	swept, err := store.Sweep(ctx, 1000)
	require.NoError(t, err)
	require.GreaterOrEqual(t, swept, int64(1), "the sweeper must delete the aged-out record")

	_, _, found := idempotencyRecord(t, ctx, sharedPool, key)
	require.False(t, found, "the replay record is gone")

	// The key, however, is not. A retry now must be refused.
	retry := transferRequest(t, from, to, 600, "INR")
	retry.Idempotency = &ledger.Idempotent{Key: key, Render: renderStub(t)}
	_, err = service.PostTransaction(ctx, retry)
	require.ErrorIs(t, err, ledger.ErrDuplicateIdempotencyKey,
		"a retry after the TTL must be refused, never executed a second time")

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key),
		"the TTL bounds storage, never how many transactions a key may create")

	balance, err := service.GetBalance(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, int64(600), balance.Available.AmountMinor())

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestIdempotency_SweeperLeavesLiveRecordsAlone guards the other half of the
// sweeper's contract. A reaper that deleted a live record would silently free a
// key whose transaction still exists, and the next retry would be refused by a
// constraint instead of replayed.
func TestIdempotency_SweeperLeavesLiveRecordsAlone(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := newIdempotencyStore(sharedPool)
	fingerprint, err := idempotency.FingerprintOf("POST", "/v1/transactions", []byte(`{}`))
	require.NoError(t, err)

	live := uuid.NewString()
	won, err := store.Reserve(ctx, idempotency.Reservation{
		Key: live, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: idempotency.DefaultLease,
	})
	require.NoError(t, err)
	require.True(t, won)

	expired := uuid.NewString()
	won, err = store.Reserve(ctx, idempotency.Reservation{
		Key: expired, Fingerprint: fingerprint, Method: "POST", Route: "/v1/transactions",
		TTL: idempotency.DefaultTTL, Lease: idempotency.DefaultLease,
	})
	require.NoError(t, err)
	require.True(t, won)

	ageOut(t, ctx, expired)

	_, err = store.Sweep(ctx, 1000)
	require.NoError(t, err)

	_, _, found := idempotencyRecord(t, ctx, sharedPool, expired)
	assert.False(t, found, "an expired record must be swept")

	_, _, found = idempotencyRecord(t, ctx, sharedPool, live)
	assert.True(t, found, "a live record must survive the sweep")
}

// TestIdempotency_KeyIsScopedToItsEndpoint asserts that the method and route in
// the fingerprint actually bind.
//
// Without them, one key reused across two endpoints would replay the first
// endpoint's response for the second -- a 201 describing a transaction that is
// not the one the caller asked about.
func TestIdempotency_KeyIsScopedToItsEndpoint(t *testing.T) {
	t.Parallel()

	body := []byte(`{"reason":"duplicate"}`)

	post, err := idempotency.FingerprintOf("POST", "/v1/transactions", body)
	require.NoError(t, err)
	reverse, err := idempotency.FingerprintOf("POST", "/v1/transactions/{id}/reverse", body)
	require.NoError(t, err)
	get, err := idempotency.FingerprintOf("GET", "/v1/transactions", body)
	require.NoError(t, err)

	assert.False(t, post.Equal(reverse), "the same body on two routes must not share a fingerprint")
	assert.False(t, post.Equal(get), "the same body under two methods must not share a fingerprint")

	same, err := idempotency.FingerprintOf("POST", "/v1/transactions", body)
	require.NoError(t, err)
	assert.True(t, post.Equal(same), "the same request must fingerprint identically")
}

func TestParseKey(t *testing.T) {
	t.Parallel()

	canonical := uuid.New()

	tests := []struct {
		name    string
		in      string
		want    string
		wantErr error
	}{
		{
			name: "should accept a canonical UUID",
			in:   canonical.String(),
			want: canonical.String(),
		},
		{
			name: "should normalise an upper-case UUID to one key",
			in:   fmt.Sprintf("%X", []byte(canonical[:])),
			want: canonical.String(),
		},
		{
			name:    "should reject an absent key",
			in:      "",
			wantErr: idempotency.ErrMissingKey,
		},
		{
			name:    "should reject an opaque non-UUID key",
			in:      "order-1",
			wantErr: idempotency.ErrInvalidKey,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			got, err := idempotency.ParseKey(tc.in)
			if tc.wantErr != nil {
				require.ErrorIs(t, err, tc.wantErr)
				return
			}
			require.NoError(t, err)
			assert.Equal(t, tc.want, got)
		})
	}
}
