package test

import (
	"context"
	"io"
	"log/slog"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/gateway"
	gatewaymock "github.com/satyamsipah/ledger-core/internal/gateway/mock"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/ledger/pgledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/saga"
	"github.com/satyamsipah/ledger-core/internal/saga/payout"
	"github.com/satyamsipah/ledger-core/internal/saga/pgsaga"
)

// sagaStepTimeout is short so the sweeper's recovery path can be driven inside
// a test rather than waited out. It is the one knob these tests shrink; every
// other timing property they assert is a real one.
const sagaStepTimeout = 300 * time.Millisecond

// payoutAccounts is the four-account cast of a marketplace payout.
//
// Every one of them is CREDIT-normal (LIABILITY or REVENUE), which is
// deliberate for the reason testdb.go's newTypedAccount comment gives: on a
// DEBIT-normal ASSET account the transaction and balance sign conventions
// coincide, so a sign bug is invisible. A payout's real accounts are liabilities
// and revenue anyway, so the honest fixture is also the more searching one.
type payoutAccounts struct {
	wallet   uuid.UUID
	suspense uuid.UUID
	merchant uuid.UUID
	fees     uuid.UUID
	funder   uuid.UUID
}

// newPayoutAccounts opens the cast and funds the wallet through the real
// posting path, so no fixture is created by a route the system does not use.
func newPayoutAccounts(t *testing.T, ctx context.Context, svc *ledger.Service, opening int64) payoutAccounts {
	t.Helper()

	accounts := payoutAccounts{
		wallet:   newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false),
		suspense: newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false),
		merchant: newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false),
		fees:     newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeRevenue, "INR", false),
		funder:   newAccount(t, ctx, sharedPool, "INR", true),
	}
	if opening > 0 {
		fund(t, ctx, svc, accounts.funder, accounts.wallet, opening)
	}
	return accounts
}

// payload builds the saga input for this cast.
func (a payoutAccounts) payload(amount, fee int64) payout.Payload {
	return payout.Payload{
		CustomerWalletID:  a.wallet,
		SuspenseID:        a.suspense,
		MerchantPayableID: a.merchant,
		FeeRevenueID:      a.fees,
		AmountMinor:       amount,
		FeeMinor:          fee,
		Currency:          "INR",
	}
}

// startMockGateway runs the mock on a real TCP listener.
//
// A real listener, not an in-process fake, because these tests must be able to
// produce failures the orchestrator cannot distinguish from production ones: a
// genuine unanswered request, and a genuine connection refused when Close is
// called. See .claude/rules/testing.md.
func startMockGateway(t *testing.T) (*gatewaymock.Server, *httptest.Server) {
	t.Helper()

	server := gatewaymock.New(slog.New(slog.NewTextHandler(io.Discard, nil)))
	listener := httptest.NewServer(server.Handler())
	t.Cleanup(listener.Close)
	return server, listener
}

// newOrchestrator wires an orchestrator against the shared pool and a gateway.
//
// Each one gets its own saga_type, derived from the test name, so that
// concurrently running tests cannot claim each other's sagas -- claims are
// scoped by type, which is what makes that isolation free rather than a test
// harness contrivance.
func newOrchestrator(
	t *testing.T,
	pool *pgxpool.Pool,
	gatewayURL string,
	tune func(*payout.Config),
) (*payout.Orchestrator, *observability.Metrics, string) {
	t.Helper()

	metrics := observability.NewMetrics("test")
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	sagaType := payout.SagaType + "-" + uuid.NewString()[:8]

	cfg := payout.Config{
		SagaType:                sagaType,
		WorkerID:                "worker-" + uuid.NewString()[:8],
		ClaimInterval:           10 * time.Millisecond,
		ClaimBatch:              50,
		Lease:                   5 * time.Second,
		StepTimeout:             sagaStepTimeout,
		MaxStepAttempts:         3,
		MaxCompensationAttempts: 3,
		SweepInterval:           20 * time.Millisecond,
		MaxProbes:               3,
	}
	if tune != nil {
		tune(&cfg)
	}

	orchestrator := payout.New(
		pgsaga.New(pool, 30*time.Second),
		ledger.NewService(pgledger.New(pool, 30*time.Second), metrics),
		gateway.NewHTTPClient(gatewayURL, 2*time.Second, time.Second),
		logger, metrics, cfg)

	return orchestrator, metrics, sagaType
}

// driveTo runs the orchestrator until a saga reaches want, or fails the test.
//
// Both loops run on every pass, because which one advances a saga is part of
// what is under test: a saga waiting on an unresolved gateway call is reachable
// only through the sweeper, and one that is ready to move only through the
// claim loop. A test that drove just one of them could pass while the other was
// broken.
func driveTo(
	t *testing.T,
	ctx context.Context,
	orchestrator *payout.Orchestrator,
	sagaID uuid.UUID,
	want saga.Status,
) *saga.Instance {
	t.Helper()

	store := pgsaga.New(sharedPool, 30*time.Second)
	deadline := time.Now().Add(30 * time.Second)

	for time.Now().Before(deadline) {
		orchestrator.DriveOnce(ctx)

		current, err := store.Get(ctx, sagaID)
		require.NoError(t, err)
		if current.Status == want {
			return current
		}
		if current.Status.Terminal() {
			t.Fatalf("saga %s reached terminal status %s, wanted %s (last error: %s)",
				sagaID, current.Status, want, current.LastError)
		}

		orchestrator.SweepOnce(ctx)

		current, err = store.Get(ctx, sagaID)
		require.NoError(t, err)
		if current.Status == want {
			return current
		}
		if current.Status.Terminal() {
			t.Fatalf("saga %s reached terminal status %s, wanted %s (last error: %s)",
				sagaID, current.Status, want, current.LastError)
		}

		time.Sleep(25 * time.Millisecond)
	}

	current, err := store.Get(ctx, sagaID)
	require.NoError(t, err)
	t.Fatalf("saga %s never reached %s; stuck in %s (last error: %s)",
		sagaID, want, current.Status, current.LastError)
	return nil
}

// sagaInstance reads a saga's current row.
func sagaInstance(t *testing.T, ctx context.Context, sagaID uuid.UUID) *saga.Instance {
	t.Helper()
	in, err := pgsaga.New(sharedPool, 30*time.Second).Get(ctx, sagaID)
	require.NoError(t, err)
	return in
}

// sagaAttempts reads a saga's full audit log.
func sagaAttempts(t *testing.T, ctx context.Context, sagaID uuid.UUID) []saga.Attempt {
	t.Helper()
	attempts, err := pgsaga.New(sharedPool, 30*time.Second).Attempts(ctx, sagaID)
	require.NoError(t, err)
	return attempts
}

// balanceOf reads an account's available and pending minor units straight from
// account_balances, which is the authoritative row the overdraft CHECK guards.
func balanceOf(t *testing.T, ctx context.Context, account uuid.UUID) (available, pending int64) {
	t.Helper()
	require.NoError(t, sharedPool.QueryRow(ctx,
		`SELECT available_minor, pending_minor FROM account_balances WHERE account_id = $1`,
		account).Scan(&available, &pending))
	return available, pending
}

// snapshot is every number a saga is supposed to leave unchanged when it
// compensates.
type snapshot struct {
	wallet, suspense, merchant, fees int64
	suspensePending                  int64
}

func takeSnapshot(t *testing.T, ctx context.Context, a payoutAccounts) snapshot {
	t.Helper()

	wallet, _ := balanceOf(t, ctx, a.wallet)
	suspense, suspensePending := balanceOf(t, ctx, a.suspense)
	merchant, _ := balanceOf(t, ctx, a.merchant)
	fees, _ := balanceOf(t, ctx, a.fees)

	return snapshot{
		wallet: wallet, suspense: suspense, merchant: merchant, fees: fees,
		suspensePending: suspensePending,
	}
}

// countSagaOutboxEvents counts outbox rows a saga emitted of one type.
func countSagaOutboxEvents(t *testing.T, ctx context.Context, sagaID uuid.UUID, eventType string) int {
	t.Helper()

	var count int
	require.NoError(t, sharedPool.QueryRow(ctx, `
		SELECT count(*) FROM outbox
		 WHERE aggregate_type = 'saga' AND aggregate_id = $1 AND event_type = $2`,
		sagaID.String(), eventType).Scan(&count))
	return count
}

// countSagaTransactions counts ledger transactions a saga posted, read from the
// audit log rather than from transaction metadata: the audit log is what the
// compensation path itself consults, so asserting on it tests the thing the
// orchestrator relies on.
func countSagaTransactions(t *testing.T, ctx context.Context, sagaID uuid.UUID) int {
	t.Helper()

	count := 0
	for _, a := range sagaAttempts(t, ctx, sagaID) {
		if a.TransactionID != nil {
			count++
		}
	}
	return count
}
