package http

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	nethttp "net/http"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
)

// maxBodyBytes caps a request body before it is buffered.
//
// Fingerprinting requires the whole body in memory, so without a cap the
// idempotency middleware would be a memory exhaustion primitive that every
// unauthenticated caller can reach. One mebibyte is far above any legitimate
// transaction -- a thousand-leg transaction is a few hundred kilobytes -- and
// far below anything that threatens the process.
const maxBodyBytes = 1 << 20

// idempotencyHeader and replayHeader are the wire contract.
const (
	idempotencyHeader = "Idempotency-Key"
	replayHeader      = "Idempotent-Replay"
)

// contextKey is unexported so no other package can write to this request's
// idempotency state, deliberately or by colliding on a string key.
type contextKey struct{ name string }

var idempotencyContextKey = &contextKey{name: "idempotency"}

// idemState is the reservation this request holds, carried to the handler.
type idemState struct {
	key     string
	manager *idempotency.Manager
	logger  *slog.Logger

	// settled records that the key already reached a terminal state, so the
	// deferred rejection path does not release a record the ledger transaction
	// completed. It is set by the handler on success.
	settled bool
}

// requireIdempotency parses and resolves the Idempotency-Key before the handler
// runs.
//
// # WHY THE MIDDLEWARE IS ONLY HALF THE MECHANISM
//
// The familiar shape for this -- wrap the ResponseWriter, let the handler run,
// then persist whatever it wrote -- cannot work here. By the time a middleware
// sees the response the ledger transaction has committed, so storing the record
// afterwards is a second transaction, and a crash between the two leaves the
// key not knowing about money that has already moved. That is precisely the bug
// this phase exists to remove.
//
// So the work is split. This middleware owns the READ path, which is pure
// decision-making and touches nothing: replay a terminal record, refuse a live
// lease with 409, refuse a mismatched fingerprint with 422. On a miss it hands
// the reservation to the handler through the request context, and the WRITE
// path -- marking the key COMPLETED -- happens inside the ledger transaction,
// in internal/ledger. See internal/idempotency for the full argument.
func requireIdempotency(manager *idempotency.Manager, logger *slog.Logger) func(nethttp.Handler) nethttp.Handler {
	return func(next nethttp.Handler) nethttp.Handler {
		return nethttp.HandlerFunc(func(w nethttp.ResponseWriter, r *nethttp.Request) {
			key, err := idempotency.ParseKey(r.Header.Get(idempotencyHeader))
			if err != nil {
				writeProblem(w, r, err)
				return
			}

			body, err := readBody(w, r)
			if err != nil {
				writeProblem(w, r, err)
				return
			}

			// The route PATTERN, not the path: two reversals of different
			// transactions share it, which is correct because the transaction
			// id belongs to the resource rather than to the request's identity.
			fingerprint, err := idempotency.FingerprintOf(r.Method, routePattern(r), body)
			if err != nil {
				writeProblem(w, r, err)
				return
			}

			record, disposition, err := manager.Acquire(r.Context(), idempotency.Reservation{
				Key:         key,
				Fingerprint: fingerprint,
				Method:      r.Method,
				Route:       routePattern(r),
			})
			if err != nil {
				writeProblem(w, r, err)
				return
			}

			if disposition == idempotency.Replay {
				writeReplay(w, record)
				return
			}

			state := &idemState{key: key, manager: manager, logger: logger}
			ctx := context.WithValue(r.Context(), idempotencyContextKey, state)

			// The body was consumed to fingerprint it, so the handler gets a
			// fresh reader over the same bytes. Reading it twice from the
			// network is not an option, and re-reading is the one thing a
			// handler downstream of this middleware would otherwise expect to
			// be able to do.
			r = r.WithContext(ctx)
			r.Body = io.NopCloser(bytes.NewReader(body))

			next.ServeHTTP(w, r)
		})
	}
}

// readBody buffers the request body under the size cap.
func readBody(w nethttp.ResponseWriter, r *nethttp.Request) ([]byte, error) {
	r.Body = nethttp.MaxBytesReader(w, r.Body, maxBodyBytes)

	body, err := io.ReadAll(r.Body)
	if err != nil {
		var tooLarge *nethttp.MaxBytesError
		if errors.As(err, &tooLarge) {
			return nil, fmt.Errorf("request body exceeds %d bytes: %w", maxBodyBytes, idempotency.ErrMalformedBody)
		}
		return nil, fmt.Errorf("read request body: %w: %w", err, idempotency.ErrMalformedBody)
	}

	return body, nil
}

// writeReplay returns a stored response verbatim.
//
// Verbatim is the whole promise. The body is the bytes that were durable at the
// instant the transaction committed, not a re-rendering of the transaction as
// this build would describe it -- so a client retrying across a deploy gets the
// same answer it would have got before, which is the one thing an idempotent
// endpoint guarantees.
func writeReplay(w nethttp.ResponseWriter, record *idempotency.Record) {
	w.Header().Set("Content-Type", contentTypeFor(record.ResponseStatus))
	w.Header().Set(replayHeader, "true")
	w.WriteHeader(record.ResponseStatus)
	_, _ = w.Write(record.ResponseBody)
}

// contentTypeFor picks the media type a stored response was rendered as. A
// cached rejection is a problem document and a cached success is not, and
// replaying a 422 as application/json would break a client that switches on the
// content type.
func contentTypeFor(status int) string {
	if status >= nethttp.StatusBadRequest {
		return "application/problem+json"
	}
	return "application/json"
}

// idempotencyFrom returns the reservation this request holds, if any.
func idempotencyFrom(ctx context.Context) *idemState {
	state, _ := ctx.Value(idempotencyContextKey).(*idemState)
	return state
}

// settle marks the key as already terminal, so reject becomes a no-op.
//
// Called by the handler once the ledger transaction has committed, because at
// that point the record is COMPLETED and anything this layer did to it would be
// undoing a durable fact.
func (s *idemState) settle() {
	if s != nil {
		s.settled = true
	}
}

// reject disposes of a reservation whose request did not commit, either by
// caching the rejection or by handing the key back.
//
// THE SPLIT, AND WHY IT IS NOT ARBITRARY: an error that is a property of the
// REQUEST will produce the same answer forever, so it is cached and replayed --
// a client retrying an unbalanced transaction should be told no immediately
// rather than have the ledger re-derive it. An error that is a property of the
// WORLD may not: insufficient funds becomes sufficient when the account is
// funded, and a frozen account gets unfrozen. Burning the key on those would
// force an honest client to mint a new one for what is, to it, the same
// operation -- which is exactly the situation idempotency keys exist to avoid.
//
// Both branches are safe against the dangerous ordering. If the ledger
// transaction did commit and this ran anyway, the record is COMPLETED: the
// release is guarded on IN_PROGRESS and matches nothing, and Fail returns
// ErrLeaseLost rather than overwriting it.
func (s *idemState) reject(ctx context.Context, err error, status int, body []byte) {
	if s == nil || s.settled {
		return
	}

	// Detached from the request context on purpose. This runs on the failure
	// path, and the commonest failure is a cancelled or expired request -- the
	// context that would refuse to let us clean up is the very one whose
	// cancellation created the mess.
	ctx = context.WithoutCancel(ctx)

	if isDeterministic(err) {
		if failErr := s.manager.Fail(ctx, s.key, status, body); failErr != nil {
			s.logger.WarnContext(ctx, "could not record idempotent failure",
				slog.String("idempotency_key", s.key),
				slog.String("error", failErr.Error()))
		}
		return
	}

	// Best-effort. A release that never runs leaves a lease that expires on its
	// own, which costs a delay and nothing else.
	if releaseErr := s.manager.Release(ctx, s.key); releaseErr != nil {
		s.logger.WarnContext(ctx, "could not release idempotency key",
			slog.String("idempotency_key", s.key),
			slog.String("error", releaseErr.Error()))
	}
}
