package http

import (
	"context"
	"fmt"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/reconciliation"
)

// defaultReconciliationRunListLimit and maxReconciliationRunListLimit mirror
// the saga listing's own limits (sagas.go) for the same reason: this is a
// triage/audit view, not an export endpoint.
const (
	defaultReconciliationRunListLimit = 100
	maxReconciliationRunListLimit     = 1000
)

// ReconciliationReader reads reconciliation run reports for the dashboard.
//
// Narrow on purpose, like SagaReader: the HTTP layer only ever reads a report
// that cmd/reconciler already produced. Starting or resolving a run is not
// exposed here -- there is no client-triggered run in this phase, and a
// resolution workflow for OPEN exceptions is deliberately out of scope, the
// same way NEEDS_MANUAL_REVIEW has no resolution endpoint yet (D43).
type ReconciliationReader interface {
	ListRuns(ctx context.Context, limit int) ([]reconciliation.Run, error)
	GetRun(ctx context.Context, id uuid.UUID) (*reconciliation.Run, error)
	ListExceptions(ctx context.Context, runID uuid.UUID) ([]reconciliation.Exception, error)
}

// handleListReconciliationRuns lists recent runs, newest first.
func handleListReconciliationRuns(reader ReconciliationReader) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		limit := defaultReconciliationRunListLimit
		if raw := r.URL.Query().Get("limit"); raw != "" {
			parsed, err := strconv.Atoi(raw)
			if err != nil || parsed < 1 || parsed > maxReconciliationRunListLimit {
				writeProblem(w, r, fmt.Errorf("limit %q must be between 1 and %d: %w",
					raw, maxReconciliationRunListLimit, ledger.ErrInvalidEntry))
				return
			}
			limit = parsed
		}

		runs, err := reader.ListRuns(r.Context(), limit)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		items := make([]reconciliationRunResponse, 0, len(runs))
		for i := range runs {
			items = append(items, reconciliationRunResponseFrom(&runs[i], nil))
		}
		writeJSON(w, nethttp.StatusOK, reconciliationRunListResponse{Runs: items})
	}
}

// handleGetReconciliationRun answers with one run and every exception it
// raised -- the report the phase asks for, "a breakdown by category", is
// ByCategory on the run plus the exceptions themselves for the detail.
func handleGetReconciliationRun(reader ReconciliationReader) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		runID, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, fmt.Errorf("reconciliation run id %q is not a UUID: %w",
				chi.URLParam(r, "id"), reconciliation.ErrRunNotFound))
			return
		}

		run, err := reader.GetRun(r.Context(), runID)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		exceptions, err := reader.ListExceptions(r.Context(), runID)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, reconciliationRunResponseFrom(run, exceptions))
	}
}

type reconciliationRunResponse struct {
	ID                string                            `json:"id"`
	Source            string                            `json:"source"`
	StartedAt         time.Time                         `json:"started_at"`
	FinishedAt        *time.Time                        `json:"finished_at,omitempty"`
	Status            string                            `json:"status"`
	PSPRowCount       int                               `json:"psp_row_count"`
	MatchedCount      int                               `json:"matched_count"`
	AutoResolvedCount int                               `json:"auto_resolved_count"`
	ExceptionCount    int                               `json:"exception_count"`
	Error             string                            `json:"error,omitempty"`
	ByCategory        map[string]int                    `json:"by_category,omitempty"`
	Exceptions        []reconciliationExceptionResponse `json:"exceptions,omitempty"`
}

type reconciliationExceptionResponse struct {
	ID                  string         `json:"id"`
	ExternalRef         string         `json:"external_ref"`
	Category            string         `json:"category"`
	Status              string         `json:"status"`
	LedgerTransactionID string         `json:"ledger_transaction_id,omitempty"`
	SagaID              string         `json:"saga_id,omitempty"`
	LedgerAmountMinor   *int64         `json:"ledger_amount_minor,omitempty"`
	PSPAmountMinor      *int64         `json:"psp_amount_minor,omitempty"`
	Currency            string         `json:"currency,omitempty"`
	LedgerStatus        string         `json:"ledger_status,omitempty"`
	PSPStatus           string         `json:"psp_status,omitempty"`
	Details             map[string]any `json:"details,omitempty"`
	CreatedAt           time.Time      `json:"created_at"`
	ResolvedAt          *time.Time     `json:"resolved_at,omitempty"`
}

type reconciliationRunListResponse struct {
	Runs []reconciliationRunResponse `json:"runs"`
}

func reconciliationRunResponseFrom(run *reconciliation.Run, exceptions []reconciliation.Exception) reconciliationRunResponse {
	resp := reconciliationRunResponse{
		ID:                run.ID.String(),
		Source:            run.Source,
		StartedAt:         run.StartedAt,
		FinishedAt:        run.FinishedAt,
		Status:            string(run.Status),
		PSPRowCount:       run.PSPRowCount,
		MatchedCount:      run.MatchedCount,
		AutoResolvedCount: run.AutoResolvedCount,
		ExceptionCount:    run.ExceptionCount,
		Error:             run.Error,
	}
	if len(run.ByCategory) > 0 {
		resp.ByCategory = make(map[string]int, len(run.ByCategory))
		for category, count := range run.ByCategory {
			resp.ByCategory[string(category)] = count
		}
	}
	for _, exc := range exceptions {
		item := reconciliationExceptionResponse{
			ID:                exc.ID.String(),
			ExternalRef:       exc.ExternalRef,
			Category:          string(exc.Category),
			Status:            string(exc.Status),
			LedgerAmountMinor: exc.LedgerAmountMinor,
			PSPAmountMinor:    exc.PSPAmountMinor,
			Currency:          exc.Currency,
			LedgerStatus:      exc.LedgerStatus,
			PSPStatus:         exc.PSPStatus,
			Details:           exc.Details,
			CreatedAt:         exc.CreatedAt,
			ResolvedAt:        exc.ResolvedAt,
		}
		if exc.LedgerTransactionID != nil {
			item.LedgerTransactionID = exc.LedgerTransactionID.String()
		}
		if exc.SagaID != nil {
			item.SagaID = exc.SagaID.String()
		}
		resp.Exceptions = append(resp.Exceptions, item)
	}
	return resp
}
