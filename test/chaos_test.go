package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	nethttp "net/http"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/auth/pgauth"
	"github.com/satyamsipah/ledger-core/internal/consistency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// TestChaos_InvariantHoldsUnderRandomFaults runs real load against a real,
// already-running deployment while cmd/chaos-harness injects real faults at
// random -- DB connection failure, Kafka unavailability, a slow query
// actually holding a row lock, gateway timeouts and 500s, and clock skew --
// and asserts the one thing that must never be false regardless of what else
// goes wrong: the global invariant.
//
// UNLIKE EVERY OTHER TEST IN THIS SUITE, this one does not use Testcontainers
// and does not spin up anything itself. It cannot: cmd/chaos-harness pauses
// and unpauses real containers by name over the Docker Engine API, which only
// means something against a stack docker-compose actually started. Run
// `make chaos-up` first, then `make chaos-test` (which sets the two env vars
// below) -- or just `go test`, which finds them unset and skips with an
// explanation rather than failing confusingly against nothing.
//
// See docs/DECISIONS.md D51 for why this shape was chosen over teaching
// Testcontainers to orchestrate the whole compose file from inside Go: real
// value at a fraction of the complexity, honestly documented as a deliberate
// trade rather than a gap discovered later.
func TestChaos_InvariantHoldsUnderRandomFaults(t *testing.T) {
	harnessURL := os.Getenv("LEDGER_CHAOS_HARNESS_URL")
	apiURL := os.Getenv("LEDGER_CHAOS_API_URL")
	if harnessURL == "" || apiURL == "" {
		t.Skip("LEDGER_CHAOS_HARNESS_URL and LEDGER_CHAOS_API_URL are not set. " +
			"This test drives a real running docker-compose stack, not Testcontainers: " +
			"run `make chaos-up`, then `make chaos-test`.")
	}

	postgresDSN := os.Getenv("LEDGER_CHAOS_POSTGRES_DSN")
	if postgresDSN == "" {
		postgresDSN = "postgres://ledger:ledger@localhost:5432/ledger?sslmode=disable"
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	pool, err := pgxpool.New(ctx, postgresDSN)
	require.NoError(t, err, "connect to the chaos stack's postgres at %s", postgresDSN)
	defer pool.Close()
	require.NoError(t, pool.Ping(ctx), "ping the chaos stack's postgres -- is `make chaos-up` running?")

	apiKey, err := pgauth.New(pool, 10*time.Second).Issue(ctx, "chaos-test-"+uuid.NewString()[:8])
	require.NoError(t, err)

	from := newAccount(t, ctx, pool, "INR", true)
	to := newTypedAccount(t, ctx, pool, ledger.AccountTypeLiability, "INR", true)

	httpClient := &nethttp.Client{Timeout: 30 * time.Second}

	const runFor = 40 * time.Second
	deadline := time.Now().Add(runFor)

	var (
		wg                                               sync.WaitGroup
		postedOK, postedErr, faultsInjected, faultErrors atomic.Int64
	)

	// The load generator: post real transfers against the real, running API
	// for the whole test, tolerating failures. A fault is doing its job when
	// some of these fail -- that is not a test failure, it is the point.
	// What the fault must never do is make one SUCCEED wrongly.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			if postTransfer(ctx, httpClient, apiURL, apiKey, from, to, 100) {
				postedOK.Add(1)
			} else {
				postedErr.Add(1)
			}
			time.Sleep(50 * time.Millisecond)
		}
	}()

	// The fault injector: one fault at a time, a random kind, a bounded
	// duration, with a quiet interval between so the system gets a chance to
	// recover and the load generator gets a chance to succeed between
	// faults rather than only ever meeting an active one.
	wg.Add(1)
	go func() {
		defer wg.Done()
		for time.Now().Before(deadline) {
			if injectRandomFault(ctx, httpClient, harnessURL) {
				faultsInjected.Add(1)
			} else {
				faultErrors.Add(1)
			}
			time.Sleep(2 * time.Second)
		}
	}()

	wg.Wait()

	t.Logf("load: %d succeeded, %d failed (failure during an active fault is expected)",
		postedOK.Load(), postedErr.Load())
	t.Logf("faults: %d injected, %d harness-side errors", faultsInjected.Load(), faultErrors.Load())
	require.Positive(t, postedOK.Load(), "at least some transfers must have succeeded -- "+
		"zero successes for the whole run means the load generator or the API was broken from the start, "+
		"not that faults are being tolerated")

	// The assertion that matters. Not "most requests succeeded" -- some are
	// SUPPOSED to fail while a fault is active -- but that whatever the
	// system believes about itself is still true after being knocked around
	// for forty seconds.
	assertGlobalInvariant(t, ctx, pool)

	invariant, err := consistency.CheckGlobalInvariant(ctx, pool)
	require.NoError(t, err)
	require.True(t, invariant.OK(), "violations: %+v", invariant.Violations)

	orphans, err := consistency.CheckOrphans(ctx, pool)
	require.NoError(t, err)
	require.True(t, orphans.OK())
}

// postTransfer posts one real transfer over HTTP against the live API and
// reports whether it succeeded. Errors are expected and tolerated -- see the
// caller -- so this deliberately returns a bool rather than failing the test.
func postTransfer(ctx context.Context, client *nethttp.Client, apiURL, apiKey string, from, to uuid.UUID, amountMinor int64) bool {
	body, _ := json.Marshal(map[string]any{
		"type": "TRANSFER",
		"entries": []map[string]any{
			{"account_id": from.String(), "direction": "DEBIT", "amount": map[string]string{"amount": fmt.Sprint(amountMinor), "currency": "INR"}},
			{"account_id": to.String(), "direction": "CREDIT", "amount": map[string]string{"amount": fmt.Sprint(amountMinor), "currency": "INR"}},
		},
	})

	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, apiURL+"/v1/transactions", bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+apiKey)
	req.Header.Set("Idempotency-Key", uuid.NewString())

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == nethttp.StatusCreated
}

// chaosFault is one entry in the menu injectRandomFault draws from: the
// harness path to call and the body to call it with. Durations are short --
// a few seconds -- so several faults fit inside the test's run and the load
// generator sees real recovery windows between them, not one fault that
// happens to span the whole test.
type chaosFault struct {
	path string
	body map[string]any
}

func injectRandomFault(ctx context.Context, client *nethttp.Client, harnessURL string) bool {
	faults := []chaosFault{
		{"/faults/db-down", map[string]any{"duration_seconds": 3}},
		{"/faults/kafka-down", map[string]any{"duration_seconds": 3}},
		{"/faults/slow-query", map[string]any{"duration_seconds": 4}},
		{"/faults/gateway-timeout", map[string]any{"duration_seconds": 3, "latency_ms": 5000}},
		{"/faults/gateway-500", map[string]any{"duration_seconds": 3}},
		{"/faults/clock-skew", map[string]any{"duration_seconds": 3, "target": "api", "offset_seconds": 120}},
	}
	f := faults[rand.IntN(len(faults))]

	body, _ := json.Marshal(f.body)
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost, harnessURL+f.path, bytes.NewReader(body))
	if err != nil {
		return false
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := client.Do(req)
	if err != nil {
		return false
	}
	defer resp.Body.Close()
	return resp.StatusCode == nethttp.StatusOK
}
