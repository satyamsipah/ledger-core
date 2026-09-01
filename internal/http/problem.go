package http

import (
	"context"
	"encoding/json"
	"errors"
	nethttp "net/http"
	"strconv"

	chimiddleware "github.com/go-chi/chi/v5/middleware"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// problemBase namespaces the machine-readable error identifiers.
//
// A URI rather than a bare string because RFC 9457 says type is a URI, and
// because a namespaced identifier is what lets a client switch on the error
// without parsing prose. It does not need to resolve to a live document, and
// deliberately points at a path this service does not serve: the identifier is
// a name, and giving it a hostname the service answers on would invite clients
// to fetch it on every error.
const problemBase = "https://ledger-core.invalid/problems/"

// Problem is an RFC 9457 problem detail.
//
// The format is chosen rather than invented because error shapes are the part
// of an API clients hard-code most and change least willingly, and RFC 9457 is
// the one shape a generic HTTP client library already understands. Inventing
// {"error": "..."} would have cost nothing today and a breaking change the
// first time a caller needed a machine-readable discriminator.
type Problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`

	// Instance is the request path, so a problem pasted into a ticket says
	// which endpoint produced it.
	Instance string `json:"instance,omitempty"`

	// RequestID correlates the response a client is holding with the log line
	// and trace on this side. Its absence is the single most common reason a
	// support conversation about a payment takes days instead of minutes.
	RequestID string `json:"request_id,omitempty"`
}

// problemFor maps a domain error to its HTTP representation.
//
// This is the only place in the service that turns a domain condition into a
// status code, and it is a table rather than a chain of ifs so that adding a
// sentinel without deciding its status is a visible omission rather than a
// silent 500. Every entry is matched with errors.Is, so a sentinel wrapped
// through the repository and the service still maps correctly.
//
// The ordering within the function matters in one place only: idempotency
// errors are checked before ledger ones, because a request can carry both an
// idempotency conflict and a perfectly valid body, and the conflict is the
// thing the client has to fix.
func problemFor(err error) (int, string, string) {
	switch {
	// ---- Idempotency ----------------------------------------------------
	case errors.Is(err, idempotency.ErrMissingKey):
		return nethttp.StatusBadRequest, "missing-idempotency-key", "Idempotency key required"
	case errors.Is(err, idempotency.ErrInvalidKey):
		return nethttp.StatusBadRequest, "invalid-idempotency-key", "Idempotency key must be a UUID"
	case errors.Is(err, idempotency.ErrMalformedBody):
		return nethttp.StatusBadRequest, "malformed-body", "Request body is not valid JSON"

	// 422 rather than 409. The request is syntactically fine and the server
	// understood it; what is wrong is the semantic claim that this key names
	// this content, and only the client can resolve that.
	case errors.Is(err, idempotency.ErrIdempotencyConflict):
		return nethttp.StatusUnprocessableEntity, "idempotency-key-reused",
			"Idempotency key already used with a different request body"

	case errors.Is(err, idempotency.ErrRequestInProgress):
		return nethttp.StatusConflict, "request-in-progress",
			"A request with this idempotency key is still in progress"

	// The key names a transaction whose stored response has aged out. Refused
	// rather than re-executed: the TTL ends the ability to replay, never the
	// uniqueness of the key.
	case errors.Is(err, idempotency.ErrKeyExpired),
		errors.Is(err, ledger.ErrDuplicateIdempotencyKey):
		return nethttp.StatusConflict, "idempotency-key-expired",
			"This idempotency key already created a transaction, and its response is no longer available"

	case errors.Is(err, idempotency.ErrLeaseLost):
		return nethttp.StatusConflict, "request-in-progress",
			"Another request completed this idempotency key first"

	// ---- Ledger: the caller's request is wrong --------------------------
	case errors.Is(err, ledger.ErrInsufficientFunds):
		return nethttp.StatusUnprocessableEntity, "insufficient-funds",
			"The account has insufficient funds and does not permit a negative balance"
	case errors.Is(err, ledger.ErrUnbalancedTransaction):
		return nethttp.StatusUnprocessableEntity, "unbalanced-transaction",
			"Debits and credits do not sum to zero"
	case errors.Is(err, ledger.ErrTooFewEntries):
		return nethttp.StatusUnprocessableEntity, "too-few-entries",
			"A transaction needs at least two entries"
	case errors.Is(err, ledger.ErrMixedCurrency):
		return nethttp.StatusUnprocessableEntity, "mixed-currency",
			"All entries in a transaction must share one currency"
	case errors.Is(err, ledger.ErrCurrencyMismatch):
		return nethttp.StatusUnprocessableEntity, "currency-mismatch",
			"An entry's currency does not match the account it posts to"
	case errors.Is(err, ledger.ErrInvalidCurrency):
		return nethttp.StatusUnprocessableEntity, "invalid-currency",
			"Currency must be a three-letter ISO-4217 code"
	case errors.Is(err, ledger.ErrScaleMismatch):
		return nethttp.StatusUnprocessableEntity, "scale-mismatch",
			"The scale does not match the currency's ISO-4217 exponent"
	case errors.Is(err, ledger.ErrMoneyOverflow):
		return nethttp.StatusUnprocessableEntity, "amount-overflow",
			"The amount does not fit in a 64-bit integer of minor units"
	case errors.Is(err, ledger.ErrInvalidTransactionType):
		return nethttp.StatusUnprocessableEntity, "invalid-transaction-type", "Unknown transaction type"
	case errors.Is(err, ledger.ErrInvalidEntry):
		return nethttp.StatusUnprocessableEntity, "invalid-entry", "A journal entry is malformed"
	case errors.Is(err, ledger.ErrReversalReasonRequired):
		return nethttp.StatusBadRequest, "reversal-reason-required", "A reversal must carry a reason"
	case errors.Is(err, ledger.ErrAccountNotPostable):
		return nethttp.StatusUnprocessableEntity, "account-not-postable",
			"The account is frozen or closed and cannot be posted to"

	// ---- Ledger: state ---------------------------------------------------
	case errors.Is(err, ledger.ErrAccountNotFound):
		return nethttp.StatusNotFound, "account-not-found", "No such account"
	case errors.Is(err, ledger.ErrTransactionNotFound):
		return nethttp.StatusNotFound, "transaction-not-found", "No such transaction"
	case errors.Is(err, ledger.ErrAlreadyReversed):
		return nethttp.StatusConflict, "already-reversed", "The transaction has already been reversed"
	case errors.Is(err, ledger.ErrTransactionNotPosted):
		return nethttp.StatusUnprocessableEntity, "transaction-not-posted",
			"Only a posted transaction can be reversed"

	// ---- Ours ------------------------------------------------------------
	// A deadline that expired is a capacity problem, not a bug, and 503 with a
	// Retry-After is the answer a client can act on. Distinguishable from a
	// persistent abort only because the retrier wraps both the deadline and
	// the SQLSTATE; see internal/db/retry.go.
	case errors.Is(err, context.DeadlineExceeded):
		return nethttp.StatusServiceUnavailable, "timeout", "The request took too long and was abandoned"
	case errors.Is(err, context.Canceled):
		return nethttp.StatusServiceUnavailable, "canceled", "The request was canceled"

	// ErrBalanceVersionConflict lands here on purpose. Its doc comment says it
	// means a write path mutated a balance without holding its lock, which is
	// an internal invariant violation, not something a client did.
	default:
		return nethttp.StatusInternalServerError, "internal", "Internal error"
	}
}

// isDeterministic reports whether an error will produce the same answer however
// many times the request is retried.
//
// This is the decision that splits a FAILED record from a released key, and it
// is worth stating the rule rather than the list: an error is deterministic
// when it is a property of the REQUEST, and transient when it is a property of
// the WORLD at the moment the request arrived.
//
// A transaction whose debits do not equal its credits will never balance, so
// caching that rejection and replaying it is strictly better than re-running
// the work. Insufficient funds is the opposite: the account may be funded a
// second later, and burning the key permanently would force an honest client to
// mint a new one for what is, to them, the same operation. Frozen accounts get
// unfrozen and missing accounts get created, so both are treated as transient
// for the same reason.
//
// Getting this backwards is safe in one direction and merely annoying in the
// other: caching a transient failure costs availability, while releasing a
// deterministic one costs a little wasted work. Neither can double-post,
// because the release is guarded on IN_PROGRESS.
func isDeterministic(err error) bool {
	switch {
	case errors.Is(err, ledger.ErrUnbalancedTransaction),
		errors.Is(err, ledger.ErrTooFewEntries),
		errors.Is(err, ledger.ErrMixedCurrency),
		errors.Is(err, ledger.ErrCurrencyMismatch),
		errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrScaleMismatch),
		errors.Is(err, ledger.ErrMoneyOverflow),
		errors.Is(err, ledger.ErrInvalidTransactionType),
		errors.Is(err, ledger.ErrInvalidEntry),
		errors.Is(err, ledger.ErrReversalReasonRequired),
		errors.Is(err, ledger.ErrAlreadyReversed),
		errors.Is(err, ledger.ErrTransactionNotPosted),
		errors.Is(err, idempotency.ErrMalformedBody):
		return true
	default:
		return false
	}
}

// writeProblem renders an error as problem+json and returns what it wrote, so
// the caller can hand the same bytes to the idempotency record. Replaying a
// rejection has to reproduce it exactly, which means the stored copy and the
// sent copy must be one value rather than two renderings of one error.
func writeProblem(w nethttp.ResponseWriter, r *nethttp.Request, err error) (int, []byte) {
	status, kind, title := problemFor(err)

	problem := Problem{
		Type:      problemBase + kind,
		Title:     title,
		Status:    status,
		Instance:  r.URL.Path,
		RequestID: chimiddleware.GetReqID(r.Context()),
	}

	// Detail carries the wrapped error chain, which names the account or entry
	// at fault. Suppressed on 5xx: those messages describe our internals, and a
	// constraint name or a table name in a public error body is free
	// reconnaissance.
	if status < nethttp.StatusInternalServerError {
		problem.Detail = err.Error()
	}

	// A live lease knows when it frees up, so Retry-After is a real number
	// rather than a guess. Anything else that is worth retrying gets one
	// second, which is enough to not hot-loop and short enough to not stall.
	var inProgress *idempotency.InProgressError
	switch {
	case errors.As(err, &inProgress):
		w.Header().Set("Retry-After", strconv.Itoa(int(inProgress.RetryAfter.Seconds())))
	case status == nethttp.StatusServiceUnavailable:
		w.Header().Set("Retry-After", "1")
	}

	body, marshalErr := json.Marshal(problem)
	if marshalErr != nil {
		// Problem has no field that can fail to marshal, so this is
		// unreachable; the fallback exists so that an unreachable case is still
		// a valid HTTP response rather than a zero-length body.
		body = []byte(`{"type":"` + problemBase + `internal","title":"Internal error","status":500}`)
		status = nethttp.StatusInternalServerError
	}

	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_, _ = w.Write(body)

	return status, body
}
