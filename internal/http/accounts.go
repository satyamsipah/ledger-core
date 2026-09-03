package http

import (
	"fmt"
	nethttp "net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// defaultStatementWindow bounds a statement request that names no period.
//
// A default rather than "everything": the journal is append-only and unbounded,
// so an unqualified statement on an old account is a full scan of its entire
// history. Thirty days is the window a support agent actually wants, and any
// other period is one query parameter away.
const defaultStatementWindow = 30 * 24 * time.Hour

// handleGetBalance answers with the synchronous, authoritative balance, or with
// a reconstruction from the journal when as_of is given.
//
// Two behaviours on one endpoint rather than two paths, because they answer the
// same question at different instants and a client choosing between them should
// not have to change URLs. The distinction that matters is in the response: the
// as_of answer carries the instant back, since it is bounded-stale by design.
func handleGetBalance(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		accountID, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, fmt.Errorf("account id %q is not a UUID: %w",
				chi.URLParam(r, "id"), ledger.ErrAccountNotFound))
			return
		}

		if raw := r.URL.Query().Get("as_of"); raw != "" {
			at, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("as_of %q is not an RFC 3339 timestamp: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}

			balance, err := service.GetBalanceAsOf(r.Context(), accountID, at)
			if err != nil {
				writeProblem(w, r, err)
				return
			}

			writeJSON(w, nethttp.StatusOK, balanceAsOfResponse{
				AccountID: accountID.String(),
				AsOf:      at,
				Balance:   balance,
			})
			return
		}

		balance, err := service.GetBalance(r.Context(), accountID)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, balanceResponse{
			AccountID: balance.AccountID.String(),
			Available: balance.Available,
			Pending:   balance.Pending,
			Version:   balance.Version,
			UpdatedAt: balance.UpdatedAt,
		})
	}
}

// handleGetStatement returns one keyset-paginated page of an account's history.
//
// Keyset rather than offset, and the API shape follows from that: the client
// gets back an opaque cursor and sends it as `cursor`, rather than computing a
// page number. See encodeCursor for why the token is opaque, and
// Service.GetStatement for why offset pagination on an append-only journal both
// costs more and silently skips rows.
func handleGetStatement(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		accountID, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, fmt.Errorf("account id %q is not a UUID: %w",
				chi.URLParam(r, "id"), ledger.ErrAccountNotFound))
			return
		}

		query := ledger.StatementQuery{AccountID: accountID}

		now := time.Now().UTC()
		query.To = now
		query.From = now.Add(-defaultStatementWindow)

		if raw := r.URL.Query().Get("from"); raw != "" {
			from, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("from %q is not an RFC 3339 timestamp: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}
			query.From = from
		}
		if raw := r.URL.Query().Get("to"); raw != "" {
			to, err := time.Parse(time.RFC3339, raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("to %q is not an RFC 3339 timestamp: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}
			query.To = to
		}

		if raw := r.URL.Query().Get("limit"); raw != "" {
			limit, err := strconv.Atoi(raw)
			if err != nil {
				writeProblem(w, r, fmt.Errorf("limit %q is not an integer: %w: %w",
					raw, err, ledger.ErrInvalidEntry))
				return
			}
			// Out-of-range values are clamped by the service rather than
			// rejected here, so the bound lives in one place.
			query.Limit = limit
		}

		if raw := r.URL.Query().Get("cursor"); raw != "" {
			cursor, err := decodeCursor(raw)
			if err != nil {
				writeProblem(w, r, err)
				return
			}
			query.After = cursor
		}

		statement, err := service.GetStatement(r.Context(), query)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, newStatementResponse(statement))
	}
}

// handleGetAccount answers with one account's metadata -- the account view's
// entry point once a search result, or a known id, has been chosen.
func handleGetAccount(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		id, err := uuid.Parse(chi.URLParam(r, "id"))
		if err != nil {
			writeProblem(w, r, fmt.Errorf("account id %q is not a UUID: %w",
				chi.URLParam(r, "id"), ledger.ErrAccountNotFound))
			return
		}

		account, err := service.GetAccount(r.Context(), id)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, newAccountResponse(account))
	}
}

// handleSearchAccounts answers with one keyset-paginated page of accounts
// matching the query parameters. Sharded accounts never appear: see
// Repository.SearchAccounts.
func handleSearchAccounts(service LedgerService) nethttp.HandlerFunc {
	return func(w nethttp.ResponseWriter, r *nethttp.Request) {
		params := r.URL.Query()
		query := ledger.AccountQuery{}

		if raw := params.Get("external_ref"); raw != "" {
			query.ExternalRef = &raw
		}
		if raw := params.Get("owner_id"); raw != "" {
			query.OwnerID = &raw
		}
		if raw := params.Get("currency"); raw != "" {
			query.Currency = raw
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

		page, err := service.SearchAccounts(r.Context(), query)
		if err != nil {
			writeProblem(w, r, err)
			return
		}

		writeJSON(w, nethttp.StatusOK, newAccountListResponse(page))
	}
}
