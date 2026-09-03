// Package mock implements a payment gateway that can be made to fail, to be
// slow, and to become genuinely ambiguous, so the saga's failure paths can be
// exercised against a real HTTP server rather than a stubbed error.
//
// .claude/rules/testing.md requires that failure tests kill things rather than
// simulate failure with a boolean flag, and names the gateway specifically.
// What that rules out is a flag inside the ORCHESTRATOR that makes it pretend a
// call failed -- a test of Go's error handling that says nothing about whether
// the saga survives a real one. What this package provides instead is a real
// listener producing real failures: real connection resets when it is killed,
// real hangs that are indistinguishable from a slow network, and real HTTP
// status codes. The orchestrator carries no test-only branch at all.
package mock

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"net/http"
	"sync"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/gateway"
)

// HangMode says whether, and when, a request stops responding.
//
// The two hanging modes are separate because they leave the world in opposite
// states while looking identical to the caller, and a saga that handles one but
// not the other is broken in the more expensive direction.
type HangMode string

const (
	// HangNone responds normally.
	HangNone HangMode = ""

	// HangBeforeRecording blocks without accepting the payment. The caller
	// times out and NO payment exists: compensating is correct.
	HangBeforeRecording HangMode = "before"

	// HangAfterRecording accepts the payment and then blocks. The caller times
	// out and the payment DOES exist: compensating would refund a customer
	// whose money really left. This is the case that makes guessing dangerous,
	// and the reason the saga probes instead.
	HangAfterRecording HangMode = "after"
)

// Behaviour is the injectable failure and latency profile.
type Behaviour struct {
	// Outcome is "succeed" (default), "decline" or "error".
	Outcome string `json:"outcome"`

	// LatencyMS delays the response, for driving a real step-deadline expiry
	// without the absolute stop of a hang.
	LatencyMS int `json:"latency_ms"`

	// Hang stops the response entirely until the client gives up.
	Hang HangMode `json:"hang"`

	// FailureRatePercent, when > 0, rolls the dice on every request
	// independently of Outcome: with this probability (0-100) the request
	// resolves as "error" regardless of what Outcome says.
	//
	// Deliberately "error", never "decline". Outcome already gives a caller a
	// deterministic decline whenever that is what they want to test; what
	// FailureRatePercent exists for is a load profile that wants a fraction of
	// gateway calls to come back UNKNOWN -- a real 500, ambiguous about
	// whether the payment was recorded -- because that is the path that
	// exercises the saga's probe-then-decide machinery (gateway.go's own
	// three-valued outcome) rather than its ordinary decline handling, which a
	// deterministic Outcome already covers on its own.
	FailureRatePercent int `json:"failure_rate_percent"`
}

// Server is an in-memory payment gateway.
//
// Payments are held in memory rather than in Postgres, and that is a feature:
// killing the process loses them, which is the only faithful way to produce the
// worst case in the whole design -- a gateway that cannot tell you what it did.
// A saga meeting that must reach manual review, and this is how that gets
// tested.
type Server struct {
	logger *slog.Logger

	mu        sync.RWMutex
	behaviour Behaviour
	payments  map[string]gateway.Payment
}

// New builds a mock gateway that succeeds until told otherwise.
func New(logger *slog.Logger) *Server {
	return &Server{
		logger:   logger,
		payments: make(map[string]gateway.Payment),
	}
}

// SetBehaviour changes what subsequent requests do. Safe to call while requests
// are in flight; a request reads the behaviour once, at its start.
func (s *Server) SetBehaviour(b Behaviour) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.behaviour = b
}

// Payment returns the gateway's record for a key, for tests asserting what
// really happened as opposed to what the saga believes happened.
func (s *Server) Payment(key string) (gateway.Payment, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	p, ok := s.payments[key]
	return p, ok
}

// Count returns how many distinct payments exist. The assertion that a retried
// saga charged the customer exactly once is a Count of 1.
func (s *Server) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.payments)
}

// Handler returns the gateway's routes.
func (s *Server) Handler() http.Handler {
	r := chi.NewRouter()
	r.Post("/v1/payments", s.pay)
	r.Get("/v1/payments/{key}", s.probe)
	r.Post("/control/behaviour", s.setBehaviour)
	r.Get("/healthz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	r.Get("/readyz", func(w http.ResponseWriter, _ *http.Request) { _, _ = w.Write([]byte("ok")) })
	return r
}

type payRequest struct {
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	Reference   string `json:"reference"`
}

// pay accepts a payment, idempotently on the Idempotency-Key header.
//
// The idempotency is not decoration for realism: it is what makes a retry after
// an ambiguous outcome safe, and therefore what the saga's stable per-step key
// is relying on. A mock that charged twice for one key would let a broken saga
// pass its tests.
func (s *Server) pay(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" {
		writeError(w, http.StatusBadRequest, "the Idempotency-Key header is required")
		return
	}

	var req payRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeError(w, http.StatusBadRequest, "malformed request body")
		return
	}
	if req.AmountMinor <= 0 || req.Currency == "" {
		writeError(w, http.StatusUnprocessableEntity, "amount_minor must be positive and currency is required")
		return
	}

	// Replay before anything else, including latency and hangs. A second call
	// under a known key is a question about the past, and the gateway can
	// always answer it.
	if existing, ok := s.Payment(key); ok {
		s.write(w, existing)
		return
	}

	behaviour := s.current()

	if behaviour.LatencyMS > 0 {
		select {
		case <-time.After(time.Duration(behaviour.LatencyMS) * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}

	if behaviour.Hang == HangBeforeRecording {
		s.hang(r, key, "before recording")
		return
	}

	outcome := behaviour.Outcome
	//nolint:gosec // G404: fault-injection probability, not a security
	// primitive -- the same reasoning already established for shard
	// selection (internal/ledger/service.go) and retry jitter (internal/db/retry.go).
	if behaviour.FailureRatePercent > 0 && rand.IntN(100) < behaviour.FailureRatePercent {
		outcome = "error"
	}

	if outcome == "error" {
		// A 500 is UNKNOWN, not a decline: the gateway may have processed the
		// payment and failed to say so. Recording nothing here is what makes a
		// later probe informative.
		writeError(w, http.StatusInternalServerError, "gateway is having a bad day")
		return
	}

	payment := gateway.Payment{
		Key:         key,
		Reference:   "gw_" + uuid.NewString(),
		Status:      gateway.StatusSucceeded,
		AmountMinor: req.AmountMinor,
		Currency:    req.Currency,
		CreatedAt:   time.Now().UTC(),
	}
	if outcome == "decline" {
		payment.Status = gateway.StatusFailed
		payment.Reason = "insufficient funds at the destination bank"
	}

	s.mu.Lock()
	s.payments[key] = payment
	s.mu.Unlock()

	if behaviour.Hang == HangAfterRecording {
		s.hang(r, key, "after recording")
		return
	}

	s.write(w, payment)
}

// probe answers what became of a key. It never creates anything, which is what
// makes it safe to call repeatedly against a gateway whose state is unknown.
func (s *Server) probe(w http.ResponseWriter, r *http.Request) {
	key := chi.URLParam(r, "key")

	if behaviour := s.current(); behaviour.LatencyMS > 0 {
		select {
		case <-time.After(time.Duration(behaviour.LatencyMS) * time.Millisecond):
		case <-r.Context().Done():
			return
		}
	}

	payment, ok := s.Payment(key)
	if !ok {
		writeError(w, http.StatusNotFound, "no payment exists under this key")
		return
	}
	s.write(w, payment)
}

func (s *Server) setBehaviour(w http.ResponseWriter, r *http.Request) {
	var b Behaviour
	if err := json.NewDecoder(r.Body).Decode(&b); err != nil {
		writeError(w, http.StatusBadRequest, "malformed behaviour")
		return
	}
	s.SetBehaviour(b)
	s.logger.Info("gateway behaviour changed",
		slog.String("outcome", b.Outcome),
		slog.Int("latency_ms", b.LatencyMS),
		slog.String("hang", string(b.Hang)),
		slog.Int("failure_rate_percent", b.FailureRatePercent))
	w.WriteHeader(http.StatusNoContent)
}

// hang blocks until the client gives up.
//
// Blocking on the request context rather than sleeping means the handler
// returns the instant the caller's deadline fires, so a hanging gateway does
// not leak a goroutine per abandoned request -- and the caller experiences a
// genuine unanswered request rather than an error this server chose to send.
func (s *Server) hang(r *http.Request, key, when string) {
	s.logger.Warn("hanging deliberately",
		slog.String("idempotency_key", key), slog.String("when", when))
	<-r.Context().Done()
}

func (s *Server) current() Behaviour {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.behaviour
}

func (s *Server) write(w http.ResponseWriter, p gateway.Payment) {
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"idempotency_key": p.Key,
		"reference":       p.Reference,
		"status":          string(p.Status),
		"reason":          p.Reason,
		"amount_minor":    p.AmountMinor,
		"currency":        p.Currency,
		"created_at":      p.CreatedAt.Format(time.RFC3339Nano),
	})
}

func writeError(w http.ResponseWriter, status int, detail string) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_, _ = fmt.Fprintf(w, `{"detail":%q}`, detail)
}
