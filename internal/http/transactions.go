package http

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// LedgerService is the behaviour the handlers need.
//
// Declared here rather than imported as *ledger.Service so this package depends
// on the operations it calls rather than on the concrete type -- and so the
// interface reads as the API's contract, which is a different thing from the
// service's full surface.
type LedgerService interface {
	PostTransaction(ctx context.Context, req ledger.TransactionRequest) (*ledger.Transaction, error)
	ReverseTransactionIdempotent(ctx context.Context, id uuid.UUID, reason string, idem *ledger.Idempotent) (*ledger.Transaction, error)
	GetBalance(ctx context.Context, accountID uuid.UUID) (ledger.Balance, error)
	GetBalanceAsOf(ctx context.Context, accountID uuid.UUID, at time.Time) (ledger.Money, error)
	GetStatement(ctx context.Context, q ledger.StatementQuery) (ledger.Statement, error)
	GetTransaction(ctx context.Context, id uuid.UUID) (*ledger.Transaction, error)
	SearchTransactions(ctx context.Context, q ledger.TransactionQuery) (ledger.TransactionPage, error)
	GetAccount(ctx context.Context, id uuid.UUID) (*ledger.Account, error)
	SearchAccounts(ctx context.Context, q ledger.AccountQuery) (ledger.AccountPage, error)
}

// handlePostTransaction posts a balanced transaction.
//
// The handler does no SQL and holds no business rules; it decodes, delegates,
// and encodes. The one piece of real logic is the renderer it hands down, and
// that is not logic so much as an inversion: the response has to be durable at
// the same instant the journal entries are, so it must be produced before
// COMMIT rather than after. See ledger.ResponseRenderer.
func handlePostTransaction(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		state := idempotencyFrom(r.Context())

		var body postTransactionRequest
		if err := decodeJSON(r.Body, &body); err != nil {
			status, problem := writeProblem(w, r, err)
			state.reject(r.Context(), err, status, problem)
			return
		}

		request, err := body.toDomain()
		if err != nil {
			status, problem := writeProblem(w, r, err)
			state.reject(r.Context(), err, status, problem)
			return
		}

		// requireAuth has already run on this route, so state.principalID is
		// the caller this transaction and its idempotency key are scoped to.
		request.PrincipalID = state.principalID

		// The rendered response is what a retry will be handed, so it is built
		// here -- inside the transaction, via this callback -- and stored with
		// the journal entries it describes.
		request.Idempotency = &ledger.Idempotent{
			PrincipalID: state.principalID,
			Key:         state.key,
			Render:      renderCreated,
		}

		posted, err := service.PostTransaction(r.Context(), request)
		if err != nil {
			status, problem := writeProblem(w, r, err)
			state.reject(r.Context(), err, status, problem)
			return
		}

		// Committed, so the record is COMPLETED and nothing this layer does may
		// touch it again.
		state.settle()

		writeStored(w, posted)
	}
}

// handleReverseTransaction writes the mirrored transaction that undoes another.
//
// Idempotent for the same reason posting is, and arguably a stronger one: the
// status transition already makes a second reversal fail, but it fails with
// ErrAlreadyReversed, which a retrying client cannot distinguish from "somebody
// else reversed this behind my back". Replaying the original response answers
// the question the retry was actually asking.
func handleReverseTransaction(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		state := idempotencyFrom(r.Context())

		transactionID, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			err = fmt.Errorf("transaction id %q is not a UUID: %w", chi.URLParam(r, "id"), ledger.ErrTransactionNotFound)
			status, problem := writeProblem(w, r, err)
			state.reject(r.Context(), err, status, problem)
			return
		}

		var body reverseTransactionRequest
		if err := decodeJSON(r.Body, &body); err != nil {
			status, problem := writeProblem(w, r, err)
			state.reject(r.Context(), err, status, problem)
			return
		}

		reversal, err := service.ReverseTransactionIdempotent(r.Context(), transactionID, body.Reason,
			&ledger.Idempotent{PrincipalID: state.principalID, Key: state.key, Render: renderCreated})
		if err != nil {
			status, problem := writeProblem(w, r, err)
			state.reject(r.Context(), err, status, problem)
			return
		}

		state.settle()

		writeStored(w, reversal)
	}
}

// handleGetTransaction answers with one transaction and every one of its
// journal entries -- the ledger explorer's drill-down.
func handleGetTransaction(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, fmt.Errorf("transaction id %q is not a UUID: %w",
				chi.URLParam(r, "id"), ledger.ErrTransactionNotFound))
			return
		}

		transaction, err := service.GetTransaction(r.Context(), id)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, newTransactionResponse(transaction))
	}
}

// handleSearchTransactions answers with one keyset-paginated page of
// transaction headers matching the query parameters -- the ledger explorer's
// search. Entries are not included; a match is a candidate to open with
// handleGetTransaction, not the full journal for every one at once.
func handleSearchTransactions(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		params := r.URL.Query()

		now := time.Now().UTC()
		query := ledger.TransactionQuery{To: now, From: now.Add(-defaultStatementWindow)}

		if raw := params.Get("from"); raw != "" {
			from, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("from %q is not an RFC 3339 timestamp: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}
			query.From = from
		}
		if raw := params.Get("to"); raw != "" {
			to, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("to %q is not an RFC 3339 timestamp: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}
			query.To = to
		}
		if raw := params.Get("external_ref"); raw != "" {
			query.ExternalRef = &raw
		}
		if raw := params.Get("status"); raw != "" {
			query.Status = ledger.TransactionStatus(raw)
		}
		if raw := params.Get("type"); raw != "" {
			query.Type = ledger.TransactionType(raw)
		}
		if raw := params.Get("account_id"); raw != "" {
			accountID, err := uuid.Parse(raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("account_id %q is not a UUID: %w", raw, ledger.ErrInvalidEntry))
				return
			}
			query.AccountID = &accountID
		}
		if raw := params.Get("limit"); raw != "" {
			limit, err := strconv.Atoi(raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("limit %q is not an integer: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}
			query.Limit = limit
		}
		if raw := params.Get("cursor"); raw != "" {
			after, err := decodeIDCursor(raw)
			if err != nil {
				writeProblem(w, r, err)
				return
			}
			query.After = &after
		}

		page, err := service.SearchTransactions(r.Context(), query)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, newTransactionListResponse(page))
	}
}

// renderCreated produces the 201 body stored with the transaction.
//
// It runs inside the database transaction, while account row locks are held, so
// it does no I/O -- it marshals a struct already in memory and returns.
func renderCreated(t *ledger.Transaction) (int, []byte, error) {
	body, err := json.Marshal(newTransactionResponse(t))
	if err != nil {
		return 0, nil, fmt.Errorf("marshal transaction %s: %w", t.ID, err)
	}
	return nethttp.StatusCreated, body, nil
}

// writeStored sends the same bytes that were stored for replay.
//
// Re-rendering here would be the obvious thing and would be a slow-acting bug:
// the first response and the replayed one would then be produced by two
// different code paths, and would drift apart the first time either changed.
func writeStored(w nethttp.ResponseWriter, t *ledger.Transaction) {
	status, body, err := renderCreated(t)
	if err != nil {
		// The transaction is committed; only the rendering failed. Reporting
		// 500 is honest -- the caller genuinely does not know the outcome -- and
		// the idempotency record holds the response it can retry for.
		w.Header().Set("Content-Type", "application/problem+json")
		w.WriteHeader(nethttp.StatusInternalServerError)
		_, _ = w.Write([]byte(`{"type":"` + problemBase + `internal","title":"Internal error","status":500}`))
		return
	}

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	// body is the output of json.Marshal over a struct this package defines,
	// served as application/json, which is set immediately above. There is no
	// HTML context for the taint analysis to be worried about, and the bytes
	// are the ones already stored for replay -- re-encoding them here is what
	// would make the two responses drift.
	//nolint:gosec // G705: JSON response, not an HTML one; Content-Type is set.
	_, _ = w.Write(body)
}

// decodeJSON strictly decodes a request body.
//
// DisallowUnknownFields on purpose: a client that misspells an amount field has
// made a typo that would otherwise post a zero-amount transaction, and in a ledger
// the silent interpretation of a misspelled field is a defect rather than a
// convenience. It also matches the fingerprint's strictness, so a body that
// canonicalizes cleanly and a body that decodes cleanly are the same set.
func decodeJSON(r io.Reader, into any) error {
	decoder := json.NewDecoder(r)
	decoder.DisallowUnknownFields()

	if err := decoder.Decode(into); err != nil {
		return fmt.Errorf("decode request body: %w: %w", err, idempotency.ErrMalformedBody)
	}
	return nil
}
