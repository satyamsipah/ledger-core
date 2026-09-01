package http

import (
	"context"
	"fmt"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/saga"
	"github.com/satyamsipah/ledger-core/internal/saga/payout"
)

// defaultSagaListLimit bounds an unqualified saga listing. The stuck-saga view
// is a triage screen, not an export.
const defaultSagaListLimit = 100

// maxSagaListLimit caps what a client may ask for, so one query cannot pull the
// whole table into memory.
const maxSagaListLimit = 1000

// PayoutService starts payout sagas. Narrow on purpose: the HTTP layer may
// begin a saga and may read one, and has no business driving it -- that is the
// orchestrator's job, and a handler that advanced a saga would do it inside a
// request whose client can disconnect halfway.
type PayoutService interface {
	Start(ctx context.Context, p payout.Payload, principalID string, idempotencyKey *string) (*saga.Instance, error)
}

// SagaReader reads saga state for the dashboard.
type SagaReader interface {
	Get(ctx context.Context, id uuid.UUID) (*saga.Instance, error)
	Attempts(ctx context.Context, sagaID uuid.UUID) ([]saga.Attempt, error)
	ListByStatus(ctx context.Context, status saga.Status, limit int) ([]saga.Instance, error)
}

// handleStartPayout begins a payout saga and returns 202.
//
// 202 rather than 201, and the distinction is not pedantry: the money has not
// moved when this returns. A saga is a promise to reach a terminal state, and
// answering 201 Created would tell a client the payout exists in a sense it
// does not yet -- the wallet is untouched until the orchestrator claims it.
// The response carries the saga id so the client can watch it.
//
// The saga is NOT driven inline. A handler that ran the first step would tie a
// customer's money to an HTTP connection that can be cut at any moment, and the
// resulting ambiguity is precisely what this phase exists to avoid creating.
func handleStartPayout(service PayoutService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		// requireAuth has already run on this route (wired in router.go), so
		// the principal a saga is scoped to is never the caller's to assert.
		principalID := principalFrom(r.Context())

		// The same key rules as every other write route, through the same
		// parser: a payout that accepted a differently-shaped key would be a
		// second contract for clients to learn.
		//
		// The dedupe itself is NOT the idempotency middleware, though. That
		// machinery completes a key inside the ledger's transaction, and
		// starting a saga has no ledger transaction to complete it in --
		// nothing has moved yet. saga_instances.idempotency_key carries it
		// instead, on the row the saga actually creates, scoped by
		// principal_id alongside it -- see docs/DECISIONS.md D24.
		key, err := idempotency.ParseKey(r.Header.Get(idempotencyHeader))
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		var body startPayoutRequest
		if err = decodeJSON(r.Body, &body); err != nil {
			writeProblem(w, r, err)
			return
		}

		payload, err := body.toPayload()
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		instance, err := service.Start(r.Context(), payload, principalID, &key)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusAccepted, sagaResponseFrom(instance, nil))
	}
}

// handleGetSaga answers with a saga and its full attempt history.
//
// The history is included rather than paginated behind a second call because
// the question this endpoint answers is always "what happened to this payout",
// and the answer is the sequence of attempts. A saga has a handful of them by
// construction: the retry budgets are single digits.
func handleGetSaga(reader SagaReader) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		sagaID, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, fmt.Errorf("saga id %q is not a UUID: %w",
				chi.URLParam(r, "id"), saga.ErrSagaNotFound))
			return
		}

		instance, err := reader.Get(r.Context(), sagaID)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		attempts, err := reader.Attempts(r.Context(), sagaID)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, sagaResponseFrom(instance, attempts))
	}
}

// handleListSagas lists sagas in one status.
//
// This is what puts NEEDS_MANUAL_REVIEW in front of a human. The alert
// (ledger_saga_manual_review_total, and the SagaNeedsManualReview event) says
// that something needs attention; this says which sagas, with the amounts and
// the errors attached, so the response to being paged is a page rather than a
// SQL prompt.
func handleListSagas(reader SagaReader) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		status := saga.Status(r.URL.Query().Get("status"))
		if status == "" {
			status = saga.StatusNeedsManualReview
		}

		limit := defaultSagaListLimit
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > maxSagaListLimit {
				writeProblem(w, r, fmt.Errorf("limit %q must be between 1 and %d: %w",
					raw, maxSagaListLimit, ledger.ErrInvalidEntry))
				return
			}
			limit = parsed
		}

		instances, err := reader.ListByStatus(r.Context(), status, limit)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		items := make([]sagaResponse, 0, len(instances))
		for i := range instances {
			items = append(items, sagaResponseFrom(&instances[i], nil))
		}

		writeJSON(w, nethttp.StatusOK, sagaListResponse{Status: string(status), Sagas: items})
	}
}

// startPayoutRequest is the wire shape of a payout.
//
// Amounts are ledger.Money rather than bare integers, so the currency travels
// with every figure and the JSON encoding is the same one the rest of the API
// uses -- amount as a string, because a JSON number is a float in most parsers
// and money in a float is invariant 3's whole objection.
type startPayoutRequest struct {
	CustomerWalletID  string        `json:"customer_wallet_id"`
	SuspenseID        string        `json:"platform_suspense_id"`
	MerchantPayableID string        `json:"merchant_payable_id"`
	FeeRevenueID      string        `json:"fee_revenue_id"`
	Amount            ledger.Money  `json:"amount"`
	Fee               *ledger.Money `json:"fee,omitempty"`
	ExternalRef       string        `json:"external_ref,omitempty"`
}

func (b startPayoutRequest) toPayload() (payout.Payload, error) {
	ids := map[string]*uuid.UUID{}
	payload := payout.Payload{
		AmountMinor: b.Amount.AmountMinor(),
		Currency:    b.Amount.Currency(),
		ExternalRef: b.ExternalRef,
	}
	ids["customer_wallet_id"] = &payload.CustomerWalletID
	ids["platform_suspense_id"] = &payload.SuspenseID
	ids["merchant_payable_id"] = &payload.MerchantPayableID
	ids["fee_revenue_id"] = &payload.FeeRevenueID

	for field, raw := range map[string]string{
		"customer_wallet_id":   b.CustomerWalletID,
		"platform_suspense_id": b.SuspenseID,
		"merchant_payable_id":  b.MerchantPayableID,
		"fee_revenue_id":       b.FeeRevenueID,
	} {
		parsed, err := uuid.Parse(raw)
		if err != nil {
			return payout.Payload{}, fmt.Errorf("%s %q is not a UUID: %w",
				field, raw, ledger.ErrAccountNotFound)
		}
		*ids[field] = parsed
	}

	if b.Fee != nil {
		if b.Fee.Currency() != b.Amount.Currency() {
			return payout.Payload{}, fmt.Errorf("fee is %s but the payout is %s: %w",
				b.Fee.Currency(), b.Amount.Currency(), ledger.ErrMixedCurrency)
		}
		payload.FeeMinor = b.Fee.AmountMinor()
	}

	return payload, nil
}

// sagaResponse is one saga as the dashboard sees it.
type sagaResponse struct {
	ID          string            `json:"id"`
	SagaType    string            `json:"saga_type"`
	Status      string            `json:"status"`
	CurrentStep string            `json:"current_step"`
	RetryCount  int               `json:"retry_count"`
	LastError   string            `json:"last_error,omitempty"`
	CreatedAt   time.Time         `json:"created_at"`
	UpdatedAt   time.Time         `json:"updated_at"`
	Attempts    []attemptResponse `json:"attempts,omitempty"`
}

type attemptResponse struct {
	Step          string     `json:"step"`
	Direction     string     `json:"direction"`
	Attempt       int        `json:"attempt"`
	Status        string     `json:"status"`
	TransactionID string     `json:"transaction_id,omitempty"`
	Error         string     `json:"error,omitempty"`
	StartedAt     time.Time  `json:"started_at"`
	FinishedAt    *time.Time `json:"finished_at,omitempty"`
}

type sagaListResponse struct {
	Status string         `json:"status"`
	Sagas  []sagaResponse `json:"sagas"`
}

func sagaResponseFrom(in *saga.Instance, attempts []saga.Attempt) sagaResponse {
	response := sagaResponse{
		ID:          in.ID.String(),
		SagaType:    in.SagaType,
		Status:      string(in.Status),
		CurrentStep: string(in.CurrentStep),
		RetryCount:  in.RetryCount,
		LastError:   in.LastError,
		CreatedAt:   in.CreatedAt,
		UpdatedAt:   in.UpdatedAt,
	}

	for _, a := range attempts {
		item := attemptResponse{
			Step:       string(a.Step),
			Direction:  string(a.Direction),
			Attempt:    a.Number,
			Status:     string(a.Status),
			Error:      a.Error,
			StartedAt:  a.StartedAt,
			FinishedAt: a.FinishedAt,
		}
		if a.TransactionID != nil {
			item.TransactionID = a.TransactionID.String()
		}
		response.Attempts = append(response.Attempts, item)
	}

	return response
}
