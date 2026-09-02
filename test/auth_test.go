package test

import (
	"context"
	"encoding/json"
	nethttp "net/http"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/auth"
	"github.com/satyamsipah/ledger-core/internal/auth/pgauth"
	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// TestAPI_WriteRoutesRequireAuthentication is the test that would have caught
// D24 at its source: before this fix, every write route accepted any caller
// with no credential at all, which is the precondition for the whole
// namespace-collision bug -- an unauthenticated caller has nothing to be
// scoped BY.
func TestAPI_WriteRoutesRequireAuthentication(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	tests := []struct {
		name        string
		apiKey      string
		wantStatus  int
		wantProblem string
	}{
		{
			name:        "should reject a request with no Authorization header",
			apiKey:      "",
			wantStatus:  nethttp.StatusUnauthorized,
			wantProblem: "missing-api-key",
		},
		{
			name:        "should reject a key that was never issued",
			apiKey:      "lk_live_0000000000000000000000000000000000000000000000000000000000000000",
			wantStatus:  nethttp.StatusUnauthorized,
			wantProblem: "invalid-api-key",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			response, payload := doAs(t, ctx, server, tc.apiKey, nethttp.MethodPost,
				"/v1/transactions", uuid.NewString(), transferBody(from, to, 100))
			assert.Equal(t, tc.wantStatus, response.StatusCode, "body: %s", payload)
			assert.Equal(t, tc.wantProblem, problemType(t, payload))
		})
	}

	t.Cleanup(func() { assertGlobalInvariant(t, ctx, sharedPool) })
}

// TestAPI_IdempotencyKeysAreScopedToThePrincipal is the direct proof that D24
// is closed: two different, real, authenticated principals hold the IDENTICAL
// Idempotency-Key at once, submitting different bodies, and neither collides
// with the other.
//
// Before this fix, the second principal's request would have hit
// idempotency_keys' single-column primary key, found principal A's row, found
// a fingerprint mismatch against a different body, and been rejected with
// ErrIdempotencyConflict -- a 422 that both confirms the key is taken AND
// belongs to someone else. That is the leak D24 names explicitly.
func TestAPI_IdempotencyKeysAreScopedToThePrincipal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)

	keyA := issuePrincipal(t, ctx, "tenant-a-"+uuid.NewString())
	keyB := issuePrincipal(t, ctx, "tenant-b-"+uuid.NewString())

	fromA := newAccount(t, ctx, sharedPool, "INR", true)
	toA := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	fromB := newAccount(t, ctx, sharedPool, "INR", true)
	toB := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	// Both principals independently pick the identical key. In a global
	// namespace this is a race two clients could lose; in a scoped one it is
	// not a race at all, because they are not contending for the same row.
	sharedKey := uuid.NewString()

	responseA, payloadA := postAs(t, ctx, server, keyA, "/v1/transactions", sharedKey, transferBody(fromA, toA, 1100))
	require.Equal(t, nethttp.StatusCreated, responseA.StatusCode, "principal A body: %s", payloadA)

	responseB, payloadB := postAs(t, ctx, server, keyB, "/v1/transactions", sharedKey, transferBody(fromB, toB, 2200))
	require.Equal(t, nethttp.StatusCreated, responseB.StatusCode,
		"principal B must succeed under the identical key A is using: body: %s", payloadB)

	var txA, txB struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(payloadA, &txA))
	require.NoError(t, json.Unmarshal(payloadB, &txB))
	assert.NotEqual(t, txA.ID, txB.ID, "the two principals must produce two distinct transactions")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAPI_ACallerCannotReplayAnothersResponseWithTheSameKey goes further than
// the previous test: principal B sends not merely the same key, but the
// IDENTICAL body principal A used, which is the shape a client would send if
// it had guessed or observed A's key.
//
// Before D24, this would have replayed A's stored response verbatim --
// A's transaction id, A's accounts, A's amounts -- to B. Scoped by principal,
// B's request cannot even find A's row, so it executes as B's own new
// transaction instead of replaying anyone else's.
func TestAPI_ACallerCannotReplayAnothersResponseWithTheSameKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)

	keyA := issuePrincipal(t, ctx, "victim-"+uuid.NewString())
	keyB := issuePrincipal(t, ctx, "prober-"+uuid.NewString())

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	sharedKey := uuid.NewString()
	identicalBody := transferBody(from, to, 750)

	responseA, payloadA := postAs(t, ctx, server, keyA, "/v1/transactions", sharedKey, identicalBody)
	require.Equal(t, nethttp.StatusCreated, responseA.StatusCode, "body: %s", payloadA)
	assert.Empty(t, responseA.Header.Get("Idempotent-Replay"))

	responseB, payloadB := postAs(t, ctx, server, keyB, "/v1/transactions", sharedKey, identicalBody)
	require.Equal(t, nethttp.StatusCreated, responseB.StatusCode,
		"principal B sending A's exact key and body must execute as its own request: body: %s", payloadB)
	assert.Empty(t, responseB.Header.Get("Idempotent-Replay"),
		"B's response must not be marked as a replay of A's transaction")

	var txA, txB struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(payloadA, &txA))
	require.NoError(t, json.Unmarshal(payloadB, &txB))
	assert.NotEqual(t, txA.ID, txB.ID,
		"B must never be handed A's transaction id -- that is D24's leak, stated precisely")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestSagaPayout_IdempotencyKeysAreScopedToThePrincipal is
// TestAPI_IdempotencyKeysAreScopedToThePrincipal's counterpart at the saga
// layer, which D24 flagged explicitly as inheriting the same gap: two
// principals starting a payout under the identical Idempotency-Key must
// produce two distinct sagas, never one principal silently reusing -- or
// worse, being handed -- the other's.
func TestSagaPayout_IdempotencyKeysAreScopedToThePrincipal(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	service := newLedgerService(sharedPool)
	accountsA := newPayoutAccounts(t, ctx, service, payoutAmount)
	accountsB := newPayoutAccounts(t, ctx, service, payoutAmount)

	_, listener := startMockGateway(t)
	orchestrator, _, _ := newOrchestrator(t, sharedPool, listener.URL, nil)

	sharedKey := uuid.NewString()

	instanceA, err := orchestrator.Start(ctx, accountsA.payload(payoutAmount, payoutFee),
		"payout-tenant-a-"+uuid.NewString(), &sharedKey)
	require.NoError(t, err)

	instanceB, err := orchestrator.Start(ctx, accountsB.payload(payoutAmount, payoutFee),
		"payout-tenant-b-"+uuid.NewString(), &sharedKey)
	require.NoError(t, err)

	assert.NotEqual(t, instanceA.ID, instanceB.ID,
		"two principals sharing an idempotency key must start two distinct sagas")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAuth_RevokedKeyStopsAuthenticating proves revocation actually removes
// access, which is the property the status column exists to provide -- an
// ACTIVE-only lookup that a revoked key simply falls out of.
func TestAuth_RevokedKeyStopsAuthenticating(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := pgauth.New(sharedPool, 30*time.Second)
	principal := "revocation-test-" + uuid.NewString()

	rawKey, err := store.Issue(ctx, principal)
	require.NoError(t, err)

	got, err := store.Authenticate(ctx, rawKey)
	require.NoError(t, err)
	assert.Equal(t, principal, got)

	_, err = sharedPool.Exec(ctx,
		`UPDATE api_keys SET status = 'REVOKED', revoked_at = now() WHERE principal_id = $1`, principal)
	require.NoError(t, err)

	_, err = store.Authenticate(ctx, rawKey)
	assert.ErrorIs(t, err, auth.ErrInvalidAPIKey, "a revoked key must stop authenticating immediately")
}

// TestAuth_UnknownKeyDoesNotAuthenticate is the negative case a hashed store
// has to get right: a key that was never issued must fail exactly like a
// revoked one, with no distinguishing signal between the two.
func TestAuth_UnknownKeyDoesNotAuthenticate(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	store := pgauth.New(sharedPool, 30*time.Second)

	raw, _, err := auth.GenerateKey()
	require.NoError(t, err)

	_, err = store.Authenticate(ctx, raw)
	assert.ErrorIs(t, err, auth.ErrInvalidAPIKey)
}
