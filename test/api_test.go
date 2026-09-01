package test

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	nethttp "net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.yaml.in/yaml/v3"
	"golang.org/x/sync/errgroup"

	ledgerhttp "github.com/satyamsipah/ledger-core/internal/http"
	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

// newAPI stands up the real router over the real database.
//
// httptest.Server rather than calling handlers directly, because half of what
// this layer does lives in middleware -- the idempotency read path, the body
// buffering, the route pattern the fingerprint depends on -- and a test that
// invoked handlers would exercise none of it.
func newAPI(t *testing.T, lease time.Duration) *httptest.Server {
	t.Helper()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, _ := newRetryingLedgerService(t, sharedPool, false)

	handler := ledgerhttp.NewRouter(ledgerhttp.Deps{
		Service:     "test",
		Logger:      logger,
		Metrics:     observability.NewMetrics("test"),
		Ledger:      service,
		Idempotency: newIdempotencyManager(t, sharedPool, lease),
	})

	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)

	return server
}

// apiResponse is the part of a response a test needs, captured after the body
// has been read and closed.
//
// A struct rather than the *http.Response itself, because handing back a
// response whose body is already closed invites a use-after-close in a caller
// that reasonably assumes it still owns it. Keeping the field name StatusCode
// means an assertion reads exactly as it would against net/http.
type apiResponse struct {
	StatusCode int
	Header     nethttp.Header
}

// do issues one request and drains it, returning the status, the headers and
// the body.
func do(t *testing.T, ctx context.Context, server *httptest.Server, method, path, key, body string) (apiResponse, []byte) {
	t.Helper()

	var reader io.Reader
	if body != "" {
		reader = strings.NewReader(body)
	}

	// NewRequestWithContext rather than NewRequest: CLAUDE.md threads a context
	// through every call, and a test that opted out would be the one place a
	// hung request could not be cancelled.
	request, err := nethttp.NewRequestWithContext(ctx, method, server.URL+path, reader)
	require.NoError(t, err)
	if body != "" {
		request.Header.Set("Content-Type", "application/json")
	}
	if key != "" {
		request.Header.Set("Idempotency-Key", key)
	}

	response, err := server.Client().Do(request)
	require.NoError(t, err)
	defer func() { require.NoError(t, response.Body.Close()) }()

	payload, err := io.ReadAll(response.Body)
	require.NoError(t, err)

	return apiResponse{StatusCode: response.StatusCode, Header: response.Header}, payload
}

func post(t *testing.T, ctx context.Context, server *httptest.Server, path, key, body string) (apiResponse, []byte) {
	t.Helper()
	return do(t, ctx, server, nethttp.MethodPost, path, key, body)
}

func get(t *testing.T, ctx context.Context, server *httptest.Server, path string) (apiResponse, []byte) {
	t.Helper()
	return do(t, ctx, server, nethttp.MethodGet, path, "", "")
}

func transferBody(from, to uuid.UUID, minor int64) string {
	return fmt.Sprintf(`{
		"type": "TRANSFER",
		"entries": [
			{"account_id": %q, "direction": "DEBIT",  "amount": {"amount": "%d", "currency": "INR"}},
			{"account_id": %q, "direction": "CREDIT", "amount": {"amount": "%d", "currency": "INR"}}
		]
	}`, from, minor, to, minor)
}

// problemType pulls the machine-readable discriminator out of a problem
// document, which is the field a client is meant to switch on.
func problemType(t *testing.T, payload []byte) string {
	t.Helper()

	var problem struct {
		Type   string `json:"type"`
		Status int    `json:"status"`
	}
	require.NoError(t, json.Unmarshal(payload, &problem), "response is not a problem document: %s", payload)

	return strings.TrimPrefix(problem.Type, "https://ledger-core.invalid/problems/")
}

func TestAPI_PostTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	response, payload := post(t, ctx, server, "/v1/transactions", uuid.NewString(), transferBody(from, to, 2500))

	require.Equal(t, nethttp.StatusCreated, response.StatusCode, "body: %s", payload)
	assert.Equal(t, "application/json", response.Header.Get("Content-Type"))
	assert.Empty(t, response.Header.Get("Idempotent-Replay"), "a first execution is not a replay")

	var created struct {
		ID      string `json:"id"`
		Status  string `json:"status"`
		Entries []struct {
			Direction string `json:"direction"`
			Amount    struct {
				Amount   string `json:"amount"`
				Currency string `json:"currency"`
				Scale    int    `json:"scale"`
			} `json:"amount"`
		} `json:"entries"`
	}
	require.NoError(t, json.Unmarshal(payload, &created))

	assert.Equal(t, "POSTED", created.Status)
	require.Len(t, created.Entries, 2)

	// Amounts are strings on the wire. A JSON number here would silently lose
	// precision in the TypeScript dashboard past 2^53 minor units.
	assert.Equal(t, "2500", created.Entries[0].Amount.Amount)
	assert.Equal(t, 2, created.Entries[0].Amount.Scale)

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAPI_ReplayReturnsTheStoredResponseByteForByte is the promise an
// idempotent endpoint actually makes.
//
// Not "an equivalent response" -- the same bytes. A replay that re-rendered the
// transaction would drift from the original the first time the response shape
// changed, so a client retrying across a deploy would get a different answer
// for the same key.
func TestAPI_ReplayReturnsTheStoredResponseByteForByte(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	body := transferBody(from, to, 900)

	first, firstPayload := post(t, ctx, server, "/v1/transactions", key, body)
	require.Equal(t, nethttp.StatusCreated, first.StatusCode, "body: %s", firstPayload)

	second, secondPayload := post(t, ctx, server, "/v1/transactions", key, body)
	require.Equal(t, nethttp.StatusCreated, second.StatusCode, "a replay keeps the original status code")
	assert.Equal(t, "true", second.Header.Get("Idempotent-Replay"))
	assert.Equal(t, string(firstPayload), string(secondPayload))

	// A body differing only in formatting is the same request, so it replays
	// rather than executing. This is the half of the contract that a stricter
	// byte-comparison fingerprint would break.
	reformatted := strings.ReplaceAll(strings.ReplaceAll(body, "\n", " "), "\t", "")
	third, thirdPayload := post(t, ctx, server, "/v1/transactions", key, reformatted)
	assert.Equal(t, "true", third.Header.Get("Idempotent-Replay"),
		"whitespace is not part of a request's identity")
	assert.Equal(t, string(firstPayload), string(thirdPayload))

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key))
	assertGlobalInvariant(t, ctx, sharedPool)
}

func TestAPI_IdempotencyKeyErrors(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	t.Run("should reject a write with no key", func(t *testing.T) {
		response, payload := post(t, ctx, server, "/v1/transactions", "", transferBody(from, to, 100))
		assert.Equal(t, nethttp.StatusBadRequest, response.StatusCode)
		assert.Equal(t, "missing-idempotency-key", problemType(t, payload))
		assert.Equal(t, "application/problem+json", response.Header.Get("Content-Type"))
	})

	t.Run("should reject a key that is not a UUID", func(t *testing.T) {
		response, payload := post(t, ctx, server, "/v1/transactions", "order-1", transferBody(from, to, 100))
		assert.Equal(t, nethttp.StatusBadRequest, response.StatusCode)
		assert.Equal(t, "invalid-idempotency-key", problemType(t, payload))
	})

	t.Run("should reject the same key with a mutated body", func(t *testing.T) {
		key := uuid.NewString()

		first, payload := post(t, ctx, server, "/v1/transactions", key, transferBody(from, to, 700))
		require.Equal(t, nethttp.StatusCreated, first.StatusCode, "body: %s", payload)

		second, payload := post(t, ctx, server, "/v1/transactions", key, transferBody(from, to, 800))
		assert.Equal(t, nethttp.StatusUnprocessableEntity, second.StatusCode)
		assert.Equal(t, "idempotency-key-reused", problemType(t, payload))

		assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key),
			"a rejected mutation must not have posted anything")
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAPI_SameKeyUnderConcurrencyPostsOnce is invariant 5 over real HTTP.
//
// The service-level version of this lives in idempotency_test.go; this one adds
// what only the HTTP layer can be wrong about -- that the middleware buffers
// and restores the body correctly under load, that a 409 carries a usable
// Retry-After, and that a replayed body is identical to the executed one.
func TestAPI_SameKeyUnderConcurrencyPostsOnce(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	body := transferBody(from, to, 1500)

	const goroutines = 100
	var (
		mu       sync.Mutex
		created  int
		replayed int
		conflict int
		other    []int
		bodies   = map[string]struct{}{}
	)

	group := &errgroup.Group{}
	start := make(chan struct{})
	for range goroutines {
		group.Go(func() error {
			<-start
			response, payload := post(t, ctx, server, "/v1/transactions", key, body)

			mu.Lock()
			defer mu.Unlock()

			switch {
			case response.StatusCode == nethttp.StatusCreated && response.Header.Get("Idempotent-Replay") == "true":
				replayed++
				bodies[string(payload)] = struct{}{}
			case response.StatusCode == nethttp.StatusCreated:
				created++
				bodies[string(payload)] = struct{}{}
			case response.StatusCode == nethttp.StatusConflict:
				conflict++
				// A 409 without a usable Retry-After turns a well-behaved
				// client into a hot loop against the endpoint it is already
				// contending on.
				after, err := strconv.Atoi(response.Header.Get("Retry-After"))
				require.NoError(t, err, "409 must carry a numeric Retry-After")
				assert.GreaterOrEqual(t, after, 1)
			default:
				other = append(other, response.StatusCode)
			}
			return nil
		})
	}
	close(start)
	require.NoError(t, group.Wait())

	assert.Empty(t, other, "no request may fail with an unexpected status")
	assert.Equal(t, 1, created, "exactly one request may execute")
	assert.Equal(t, goroutines, created+replayed+conflict)
	assert.Len(t, bodies, 1, "every 201, executed or replayed, must be the identical document")

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key),
		"invariant 5 over HTTP: one key, one transaction")

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAPI_TransientRejectionReleasesTheKey covers the split between a cached
// failure and a released one, from the client's side.
//
// Insufficient funds is a property of the world, not of the request: the
// account may be funded a second later. Burning the key permanently would force
// an honest client to mint a new one for what is, to it, the same operation.
func TestAPI_TransientRejectionReleasesTheKey(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	wallet := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)
	sink := newAccount(t, ctx, sharedPool, "INR", true)

	key := uuid.NewString()
	body := transferBody(wallet, sink, 5000)

	response, payload := post(t, ctx, server, "/v1/transactions", key, body)
	require.Equal(t, nethttp.StatusUnprocessableEntity, response.StatusCode)
	assert.Equal(t, "insufficient-funds", problemType(t, payload))

	_, _, found := idempotencyRecord(t, ctx, sharedPool, key)
	assert.False(t, found, "a transient rejection must hand the key back, not burn it")

	// Fund the wallet, then retry the identical request under the same key. It
	// must now succeed rather than replay the earlier refusal.
	funder := newAccount(t, ctx, sharedPool, "INR", true)
	fund, fundPayload := post(t, ctx, server, "/v1/transactions", uuid.NewString(), transferBody(funder, wallet, 9000))
	require.Equal(t, nethttp.StatusCreated, fund.StatusCode, "body: %s", fundPayload)

	retry, retryPayload := post(t, ctx, server, "/v1/transactions", key, body)
	assert.Equal(t, nethttp.StatusCreated, retry.StatusCode,
		"the same key must be usable once the transient condition clears: %s", retryPayload)

	assert.Equal(t, 1, countTransactionsWithKey(t, ctx, sharedPool, key))
	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestAPI_DeterministicRejectionIsCachedAndReplayed is the other half of the
// same decision.
//
// An unbalanced transaction will never balance, so the rejection is cached and
// replayed rather than re-derived. The replay must reproduce the problem
// document exactly, content type included.
func TestAPI_DeterministicRejectionIsCachedAndReplayed(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	key := uuid.NewString()
	unbalanced := fmt.Sprintf(`{
		"type": "TRANSFER",
		"entries": [
			{"account_id": %q, "direction": "DEBIT",  "amount": {"amount": "100", "currency": "INR"}},
			{"account_id": %q, "direction": "CREDIT", "amount": {"amount": "250", "currency": "INR"}}
		]
	}`, from, to)

	first, firstPayload := post(t, ctx, server, "/v1/transactions", key, unbalanced)
	require.Equal(t, nethttp.StatusUnprocessableEntity, first.StatusCode)
	assert.Equal(t, "unbalanced-transaction", problemType(t, firstPayload))

	status, _, found := idempotencyRecord(t, ctx, sharedPool, key)
	require.True(t, found, "a deterministic rejection must be recorded")
	assert.Equal(t, "FAILED", status)

	second, secondPayload := post(t, ctx, server, "/v1/transactions", key, unbalanced)
	assert.Equal(t, nethttp.StatusUnprocessableEntity, second.StatusCode)
	assert.Equal(t, "true", second.Header.Get("Idempotent-Replay"))
	assert.Equal(t, "application/problem+json", second.Header.Get("Content-Type"),
		"a replayed rejection is still a problem document")
	assert.Equal(t, string(firstPayload), string(secondPayload))

	assertGlobalInvariant(t, ctx, sharedPool)
}

func TestAPI_ReverseTransaction(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	_, payload := post(t, ctx, server, "/v1/transactions", uuid.NewString(), transferBody(from, to, 3300))
	var posted struct {
		ID string `json:"id"`
	}
	require.NoError(t, json.Unmarshal(payload, &posted))

	key := uuid.NewString()
	path := "/v1/transactions/" + posted.ID + "/reverse"

	response, reversalPayload := post(t, ctx, server, path, key, `{"reason":"chargeback"}`)
	require.Equal(t, nethttp.StatusCreated, response.StatusCode, "body: %s", reversalPayload)

	var reversal struct {
		Type     string         `json:"type"`
		Metadata map[string]any `json:"metadata"`
	}
	require.NoError(t, json.Unmarshal(reversalPayload, &reversal))
	assert.Equal(t, "REVERSAL", reversal.Type)
	assert.Equal(t, posted.ID, reversal.Metadata["reverses_transaction_id"])

	// The reversal replays like any other write.
	replay, replayPayload := post(t, ctx, server, path, key, `{"reason":"chargeback"}`)
	assert.Equal(t, "true", replay.Header.Get("Idempotent-Replay"))
	assert.Equal(t, string(reversalPayload), string(replayPayload))

	// A different key against an already-reversed transaction is a conflict,
	// not a second reversal. Two reversals would each balance perfectly on
	// their own, so the balance invariant would never notice the double refund.
	conflict, conflictPayload := post(t, ctx, server, path, uuid.NewString(), `{"reason":"again"}`)
	assert.Equal(t, nethttp.StatusConflict, conflict.StatusCode)
	assert.Equal(t, "already-reversed", problemType(t, conflictPayload))

	balance, err := newLedgerService(sharedPool).GetBalance(ctx, to)
	require.NoError(t, err)
	assert.Equal(t, int64(0), balance.Available.AmountMinor(), "the reversal must undo exactly one transfer")

	assertGlobalInvariant(t, ctx, sharedPool)
}

func TestAPI_BalanceAndStatement(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	const posts = 5
	for range posts {
		response, payload := post(t, ctx, server, "/v1/transactions", uuid.NewString(), transferBody(from, to, 200))
		require.Equal(t, nethttp.StatusCreated, response.StatusCode, "body: %s", payload)
	}

	t.Run("should report the synchronous balance", func(t *testing.T) {
		response, payload := get(t, ctx, server, "/v1/accounts/"+to.String()+"/balance")
		require.Equal(t, nethttp.StatusOK, response.StatusCode, "body: %s", payload)

		var balance struct {
			Available struct {
				Amount string `json:"amount"`
			} `json:"available"`
			Version int64 `json:"version"`
		}
		require.NoError(t, json.Unmarshal(payload, &balance))
		assert.Equal(t, strconv.Itoa(posts*200), balance.Available.Amount)
		assert.Equal(t, int64(posts), balance.Version)
	})

	t.Run("should reconstruct the balance as of an instant", func(t *testing.T) {
		// RFC3339Nano, not RFC3339: second precision would truncate the
		// instant back past entries stamped microseconds ago, and the query
		// would correctly report a balance from before they were written.
		response, payload := get(t, ctx, server,
			"/v1/accounts/"+to.String()+"/balance?as_of="+
				url.QueryEscape(time.Now().UTC().Format(time.RFC3339Nano)))
		require.Equal(t, nethttp.StatusOK, response.StatusCode, "body: %s", payload)

		var asOf struct {
			AsOf    time.Time `json:"as_of"`
			Balance struct {
				Amount string `json:"amount"`
			} `json:"balance"`
		}
		require.NoError(t, json.Unmarshal(payload, &asOf))
		assert.Equal(t, strconv.Itoa(posts*200), asOf.Balance.Amount)
		assert.False(t, asOf.AsOf.IsZero(), "the instant is echoed because the answer is bounded-stale")
	})

	t.Run("should page a statement by cursor with a continuous running balance", func(t *testing.T) {
		from := time.Now().UTC().Add(-time.Hour).Format(time.RFC3339)
		to := time.Now().UTC().Add(time.Hour).Format(time.RFC3339)
		base := "/v1/accounts/" + newAccountWithHistory(t, ctx, server, posts).String() + "/statement"

		var (
			seen   int
			cursor string
			last   string
			pages  int
		)
		for {
			path := base + "?limit=2&from=" + from + "&to=" + to
			if cursor != "" {
				path += "&cursor=" + cursor
			}

			response, payload := get(t, ctx, server, path)
			require.Equal(t, nethttp.StatusOK, response.StatusCode, "body: %s", payload)

			var page struct {
				Opening struct {
					Amount string `json:"amount"`
				} `json:"opening"`
				Closing struct {
					Amount string `json:"amount"`
				} `json:"closing"`
				Lines []struct {
					RunningBalance struct {
						Amount string `json:"amount"`
					} `json:"running_balance"`
				} `json:"lines"`
				NextCursor *string `json:"next_cursor"`
			}
			require.NoError(t, json.Unmarshal(payload, &page))

			// Page N's opening must be page N-1's closing, or the running
			// balance a customer reads does not reconcile across a page break.
			if pages > 0 {
				assert.Equal(t, last, page.Opening.Amount,
					"page %d must open where page %d closed", pages+1, pages)
			}

			seen += len(page.Lines)
			last = page.Closing.Amount
			pages++

			if page.NextCursor == nil {
				break
			}
			cursor = *page.NextCursor
			require.Less(t, pages, 20, "pagination must terminate")
		}

		assert.Equal(t, posts, seen, "every entry must appear exactly once across the pages")
		assert.Greater(t, pages, 1, "a limit of 2 over 5 entries must produce several pages")
	})

	t.Run("should reject a tampered cursor", func(t *testing.T) {
		response, payload := get(t, ctx, server, "/v1/accounts/"+to.String()+"/statement?cursor=not-a-cursor")
		assert.Equal(t, nethttp.StatusUnprocessableEntity, response.StatusCode)
		assert.Equal(t, "invalid-entry", problemType(t, payload))
	})

	t.Run("should 404 an account that does not exist", func(t *testing.T) {
		response, payload := get(t, ctx, server, "/v1/accounts/"+uuid.NewString()+"/balance")
		assert.Equal(t, nethttp.StatusNotFound, response.StatusCode)
		assert.Equal(t, "account-not-found", problemType(t, payload))
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// newAccountWithHistory returns a fresh account carrying n entries, so a
// pagination test is not perturbed by whatever else the suite posted.
func newAccountWithHistory(t *testing.T, ctx context.Context, server *httptest.Server, n int) uuid.UUID {
	t.Helper()

	funder := newAccount(t, ctx, sharedPool, "INR", true)
	account := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	for range n {
		response, payload := post(t, ctx, server, "/v1/transactions", uuid.NewString(), transferBody(funder, account, 100))
		require.Equal(t, nethttp.StatusCreated, response.StatusCode, "body: %s", payload)
	}

	return account
}

func TestAPI_MalformedRequests(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)
	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", false)

	tests := []struct {
		name        string
		body        string
		wantStatus  int
		wantProblem string
	}{
		{
			name:        "should reject a body that is not JSON",
			body:        `{"type":`,
			wantStatus:  nethttp.StatusBadRequest,
			wantProblem: "malformed-body",
		},
		{
			// Rejected rather than resolved: parsers disagree about which wins,
			// so the document has no single meaning to fingerprint.
			name: "should reject duplicate JSON keys",
			body: fmt.Sprintf(`{"type":"TRANSFER","type":"PAYIN","entries":[
				{"account_id":%q,"direction":"DEBIT","amount":{"amount":"10","currency":"INR"}},
				{"account_id":%q,"direction":"CREDIT","amount":{"amount":"10","currency":"INR"}}]}`, from, to),
			wantStatus:  nethttp.StatusBadRequest,
			wantProblem: "malformed-body",
		},
		{
			// A typo that would otherwise post a zero-amount transaction.
			name: "should reject an unknown field",
			body: fmt.Sprintf(`{"type":"TRANSFER","ammount":"1250","entries":[
				{"account_id":%q,"direction":"DEBIT","amount":{"amount":"10","currency":"INR"}},
				{"account_id":%q,"direction":"CREDIT","amount":{"amount":"10","currency":"INR"}}]}`, from, to),
			wantStatus:  nethttp.StatusBadRequest,
			wantProblem: "malformed-body",
		},
		{
			// The check money.go exists for: a JSON number cannot be trusted to
			// have survived a JavaScript client intact.
			name: "should reject a numeric amount",
			body: fmt.Sprintf(`{"type":"TRANSFER","entries":[
				{"account_id":%q,"direction":"DEBIT","amount":{"amount":1250,"currency":"INR"}},
				{"account_id":%q,"direction":"CREDIT","amount":{"amount":1250,"currency":"INR"}}]}`, from, to),
			wantStatus:  nethttp.StatusBadRequest,
			wantProblem: "malformed-body",
		},
		{
			name:        "should reject an unknown transaction type",
			body:        strings.Replace(transferBody(from, to, 100), "TRANSFER", "TELEPORT", 1),
			wantStatus:  nethttp.StatusUnprocessableEntity,
			wantProblem: "invalid-transaction-type",
		},
		{
			name: "should reject a single-legged transaction",
			body: fmt.Sprintf(`{"type":"TRANSFER","entries":[
				{"account_id":%q,"direction":"DEBIT","amount":{"amount":"10","currency":"INR"}}]}`, from),
			wantStatus:  nethttp.StatusUnprocessableEntity,
			wantProblem: "too-few-entries",
		},
		{
			name: "should reject a transaction spanning two currencies",
			body: fmt.Sprintf(`{"type":"TRANSFER","entries":[
				{"account_id":%q,"direction":"DEBIT","amount":{"amount":"10","currency":"INR"}},
				{"account_id":%q,"direction":"CREDIT","amount":{"amount":"10","currency":"USD"}}]}`, from, to),
			wantStatus:  nethttp.StatusUnprocessableEntity,
			wantProblem: "mixed-currency",
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			response, payload := post(t, ctx, server, "/v1/transactions", uuid.NewString(), tc.body)
			assert.Equal(t, tc.wantStatus, response.StatusCode, "body: %s", payload)
			assert.Equal(t, tc.wantProblem, problemType(t, payload))
			assert.Equal(t, "application/problem+json", response.Header.Get("Content-Type"))
		})
	}

	assertGlobalInvariant(t, ctx, sharedPool)
}

func TestAPI_Health(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	server := newAPI(t, idempotency.DefaultLease)

	for _, path := range []string{"/healthz", "/readyz"} {
		response, payload := get(t, ctx, server, path)
		assert.Equal(t, nethttp.StatusOK, response.StatusCode, "%s body: %s", path, payload)
	}
}

// TestOpenAPI_MatchesTheRegisteredRoutes checks the specification in BOTH
// directions against the router.
//
// A specification nobody validates rots, and it rots silently: the first person
// to notice is a client integrating against a path that no longer exists, or
// missing one that does. Walking chi's own route table makes the check
// impossible to forget, because adding a route without documenting it fails
// here rather than in somebody's integration.
func TestOpenAPI_MatchesTheRegisteredRoutes(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "api", "openapi.yaml"))
	require.NoError(t, err, "api/openapi.yaml must exist")

	var spec struct {
		OpenAPI string                    `yaml:"openapi"`
		Paths   map[string]map[string]any `yaml:"paths"`
		Comps   map[string]map[string]any `yaml:"components"`
	}
	require.NoError(t, yaml.Unmarshal(raw, &spec), "api/openapi.yaml must parse")
	assert.True(t, strings.HasPrefix(spec.OpenAPI, "3.1"), "the spec declares OpenAPI 3.1")

	documented := map[string]struct{}{}
	for path, operations := range spec.Paths {
		for method := range operations {
			switch method {
			case "get", "post", "put", "patch", "delete":
				documented[strings.ToUpper(method)+" "+path] = struct{}{}
			}
		}
	}

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	service, _ := newRetryingLedgerService(t, sharedPool, false)
	mux := ledgerhttp.NewMux(ledgerhttp.Deps{
		Service:     "test",
		Logger:      logger,
		Metrics:     observability.NewMetrics("test"),
		Ledger:      service,
		Idempotency: newIdempotencyManager(t, sharedPool, idempotency.DefaultLease),
	})

	registered := map[string]struct{}{}
	require.NoError(t, chi.Walk(mux, func(method, route string, _ nethttp.Handler, _ ...func(nethttp.Handler) nethttp.Handler) error {
		// chi renders a sub-router mount as "/v1/transactions/*" alongside the
		// concrete pattern; only the concrete ones describe a real endpoint.
		if strings.HasSuffix(route, "/*") {
			return nil
		}
		// A trailing slash on a mounted sub-route is chi's rendering of the
		// collection itself.
		if len(route) > 1 {
			route = strings.TrimSuffix(route, "/")
		}
		registered[method+" "+route] = struct{}{}
		return nil
	}))

	assert.Equal(t, sortedKeys(documented), sortedKeys(registered),
		"every registered route must be documented and every documented path must exist")
}

func sortedKeys(m map[string]struct{}) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// TestOpenAPI_DocumentsEveryProblemType asserts that every machine-readable
// error identifier the service can emit appears in the specification.
//
// The `type` field is the one part of an error clients hard-code, so an
// undocumented one is a contract a client has to reverse-engineer from a
// production incident.
func TestOpenAPI_DocumentsEveryProblemType(t *testing.T) {
	t.Parallel()

	raw, err := os.ReadFile(filepath.Join("..", "internal", "http", "problem.go"))
	require.NoError(t, err)

	spec, err := os.ReadFile(filepath.Join("..", "api", "openapi.yaml"))
	require.NoError(t, err)

	// Pulled out of the mapping table rather than listed here, so a new problem
	// type cannot be added without either documenting it or failing this test.
	emitted := map[string]struct{}{}
	for _, line := range bytes.Split(raw, []byte("\n")) {
		text := string(line)
		if !strings.Contains(text, `return nethttp.Status`) {
			continue
		}
		parts := strings.Split(text, `"`)
		if len(parts) >= 2 {
			emitted[parts[1]] = struct{}{}
		}
	}
	require.NotEmpty(t, emitted, "the problem table must be parseable")

	var missing []string
	for kind := range emitted {
		if !bytes.Contains(spec, []byte(kind)) {
			missing = append(missing, kind)
		}
	}
	sort.Strings(missing)

	assert.Empty(t, missing, "these problem types are emitted but not documented in api/openapi.yaml")
}
