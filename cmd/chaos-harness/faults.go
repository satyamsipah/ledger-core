package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	nethttp "net/http"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// maxFaultDuration bounds every fault this harness can inject. A caller that
// asks for longer gets clamped rather than refused -- a chaos test's whole
// point is to run without a human watching it, and a fault that silently ran
// forever because of a typo'd duration would defeat that.
const maxFaultDuration = 5 * time.Minute

// harness holds what every fault handler needs. One struct rather than a
// closure per handler, because several faults share the same collaborators
// (the Docker client, the HTTP client for proxying to mock-gateway and the
// admin endpoints) and repeating them as separate package-level variables
// would be the global state CLAUDE.md forbids, dressed up as convenience.
type harness struct {
	cfg        config
	pool       *pgxpool.Pool
	logger     *slog.Logger
	httpClient *nethttp.Client
	docker     *dockerClient
}

// durationRequest is the shape every "block for this long" fault shares.
type durationRequest struct {
	DurationSeconds float64 `json:"duration_seconds"`
}

func (r durationRequest) duration() time.Duration {
	d := time.Duration(r.DurationSeconds * float64(time.Second))
	if d <= 0 {
		d = 5 * time.Second
	}
	if d > maxFaultDuration {
		d = maxFaultDuration
	}
	return d
}

func decodeBody(r *nethttp.Request, v any) error {
	defer r.Body.Close()
	if r.ContentLength == 0 {
		return nil
	}
	return json.NewDecoder(r.Body).Decode(v)
}

func writeResult(w nethttp.ResponseWriter, fault string, d time.Duration, err error) {
	w.Header().Set("Content-Type", "application/json")
	if err != nil {
		w.WriteHeader(nethttp.StatusInternalServerError)
		_ = json.NewEncoder(w).Encode(map[string]any{"fault": fault, "error": err.Error()})
		return
	}
	w.WriteHeader(nethttp.StatusOK)
	_ = json.NewEncoder(w).Encode(map[string]any{
		"fault": fault, "injected_for_seconds": d.Seconds(), "cleared": true,
	})
}

// handleDBDown pauses the postgres container for the requested duration, then
// unpauses it. Every connection this stack holds to Postgres -- the api's
// pool, the saga orchestrator's, the reconciler's -- goes genuinely
// unresponsive for exactly that long, the same mechanism docs/DECISIONS.md
// D36 already validated for TestOutboxPublish_KafkaOutage.
func (h *harness) handleDBDown(w nethttp.ResponseWriter, r *nethttp.Request) {
	var req durationRequest
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, "db-down", 0, fmt.Errorf("decode request: %w", err))
		return
	}
	d := req.duration()

	h.logger.WarnContext(r.Context(), "injecting fault: postgres unreachable", slog.Duration("duration", d))
	if err := h.docker.pause(r.Context(), h.cfg.postgresContainer); err != nil {
		writeResult(w, "db-down", d, fmt.Errorf("pause postgres: %w", err))
		return
	}

	time.Sleep(d)

	// Best-effort unpause even if the request context has since been
	// cancelled: leaving postgres frozen because a caller disconnected would
	// turn a bounded fault into an unbounded outage nobody asked for.
	if err := h.docker.unpause(context.Background(), h.cfg.postgresContainer); err != nil {
		writeResult(w, "db-down", d, fmt.Errorf("unpause postgres: %w", err))
		return
	}

	writeResult(w, "db-down", d, nil)
}

// handleKafkaDown is handleDBDown's twin for redpanda -- same mechanism,
// same reasoning, different container.
func (h *harness) handleKafkaDown(w nethttp.ResponseWriter, r *nethttp.Request) {
	var req durationRequest
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, "kafka-down", 0, fmt.Errorf("decode request: %w", err))
		return
	}
	d := req.duration()

	h.logger.WarnContext(r.Context(), "injecting fault: kafka unreachable", slog.Duration("duration", d))
	if err := h.docker.pause(r.Context(), h.cfg.redpandaContainer); err != nil {
		writeResult(w, "kafka-down", d, fmt.Errorf("pause redpanda: %w", err))
		return
	}

	time.Sleep(d)

	if err := h.docker.unpause(context.Background(), h.cfg.redpandaContainer); err != nil {
		writeResult(w, "kafka-down", d, fmt.Errorf("unpause redpanda: %w", err))
		return
	}

	writeResult(w, "kafka-down", d, nil)
}

// handleSlowQuery holds a real row lock on a real, genuinely hot account for
// the requested duration -- not a time.Sleep anywhere near this codebase's
// own transactions, but an actual competing transaction any real posting
// transaction touching the same account must actually queue behind. This is
// what makes it a slow QUERY fault rather than a slow HANDLER fault: the
// database itself is the thing made slow, honestly, for whichever other
// session asks for the same row.
func (h *harness) handleSlowQuery(w nethttp.ResponseWriter, r *nethttp.Request) {
	var req durationRequest
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, "slow-query", 0, fmt.Errorf("decode request: %w", err))
		return
	}
	d := req.duration()

	h.logger.WarnContext(r.Context(), "injecting fault: slow query (holding a real row lock)",
		slog.String("account_ref", h.cfg.hotAccountRef), slog.Duration("duration", d))

	// A fresh, dedicated connection: this transaction is held deliberately
	// across the whole sleep, and sharing the pool with anything else this
	// process might do would be its own small bug to chase later.
	tx, err := h.pool.Begin(r.Context())
	if err != nil {
		writeResult(w, "slow-query", d, fmt.Errorf("begin: %w", err))
		return
	}
	defer func() { _ = tx.Rollback(context.Background()) }()

	// FOR UPDATE on account_balances, keyed by the account's own
	// external_ref -- the same row the ordinary write path locks
	// (docs/DECISIONS.md D11), so any real transaction touching this
	// account genuinely queues behind this one rather than behind a lock
	// nothing else contends for.
	var accountID string
	err = tx.QueryRow(r.Context(), `
		SELECT ab.account_id
		  FROM account_balances ab
		  JOIN accounts a ON a.id = ab.account_id
		 WHERE a.external_ref = $1
		   FOR UPDATE OF ab`, h.cfg.hotAccountRef).Scan(&accountID)
	if err != nil {
		writeResult(w, "slow-query", d, fmt.Errorf("lock account %s: %w", h.cfg.hotAccountRef, err))
		return
	}

	// pg_sleep INSIDE the held transaction, not a Go time.Sleep after
	// releasing it: the lock and the delay must be the same interval, or
	// this is a slow HTTP handler wearing a slow query's name.
	if _, err := tx.Exec(r.Context(), `SELECT pg_sleep($1)`, d.Seconds()); err != nil {
		writeResult(w, "slow-query", d, fmt.Errorf("hold lock: %w", err))
		return
	}

	if err := tx.Commit(r.Context()); err != nil {
		writeResult(w, "slow-query", d, fmt.Errorf("commit: %w", err))
		return
	}

	writeResult(w, "slow-query", d, nil)
}

// gatewayBehaviourRequest mirrors mock-gateway's own Behaviour shape
// (internal/gateway/mock.Behaviour) closely enough to build one, without
// importing that package -- this binary has no other reason to depend on
// it, and the wire contract is what D45 already made the stable surface.
type gatewayBehaviourRequest struct {
	Outcome   string `json:"outcome"`
	LatencyMS int    `json:"latency_ms"`
}

// setGatewayBehaviour is the one call every gateway fault below shares:
// mock-gateway's own /control/behaviour (D45), which is already a REAL
// mechanism -- it changes what an HTTP server actually does, not a flag an
// orchestrator checks internally -- so this harness adds no new one, it
// only drives the existing one and restores it afterward.
func (h *harness) setGatewayBehaviour(ctx context.Context, b gatewayBehaviourRequest) error {
	body, err := json.Marshal(b)
	if err != nil {
		return fmt.Errorf("marshal gateway behaviour: %w", err)
	}
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost,
		h.cfg.mockGatewayURL+"/control/behaviour", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build gateway behaviour request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("set gateway behaviour: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("set gateway behaviour: unexpected status %d", resp.StatusCode)
	}
	return nil
}

// handleGatewayTimeout delays every mock-gateway response by LatencyMS for
// the requested duration, then restores instant, successful responses.
// Long enough latency drives a real saga step-deadline expiry rather than
// merely a slow response -- see internal/saga/payout's own timeout handling.
func (h *harness) handleGatewayTimeout(w nethttp.ResponseWriter, r *nethttp.Request) {
	var req struct {
		durationRequest
		LatencyMS int `json:"latency_ms"`
	}
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, "gateway-timeout", 0, fmt.Errorf("decode request: %w", err))
		return
	}
	d := req.duration()
	latency := req.LatencyMS
	if latency <= 0 {
		latency = 30_000
	}

	h.logger.WarnContext(r.Context(), "injecting fault: gateway latency",
		slog.Int("latency_ms", latency), slog.Duration("duration", d))

	if err := h.setGatewayBehaviour(r.Context(), gatewayBehaviourRequest{Outcome: "succeed", LatencyMS: latency}); err != nil {
		writeResult(w, "gateway-timeout", d, err)
		return
	}

	time.Sleep(d)

	if err := h.setGatewayBehaviour(context.Background(), gatewayBehaviourRequest{Outcome: "succeed", LatencyMS: 0}); err != nil {
		writeResult(w, "gateway-timeout", d, err)
		return
	}

	writeResult(w, "gateway-timeout", d, nil)
}

// handleGateway500 makes every mock-gateway response a decline for the
// requested duration -- "error" specifically, mock-gateway's own outcome
// for an unambiguous 5xx (see internal/gateway/mock.Server), which the
// saga's own gateway step treats as a conclusive, resolvable failure rather
// than the unresolved ambiguity a timeout or a dropped connection produces.
func (h *harness) handleGateway500(w nethttp.ResponseWriter, r *nethttp.Request) {
	var req durationRequest
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, "gateway-500", 0, fmt.Errorf("decode request: %w", err))
		return
	}
	d := req.duration()

	h.logger.WarnContext(r.Context(), "injecting fault: gateway 500s", slog.Duration("duration", d))

	if err := h.setGatewayBehaviour(r.Context(), gatewayBehaviourRequest{Outcome: "error"}); err != nil {
		writeResult(w, "gateway-500", d, err)
		return
	}

	time.Sleep(d)

	if err := h.setGatewayBehaviour(context.Background(), gatewayBehaviourRequest{Outcome: "succeed"}); err != nil {
		writeResult(w, "gateway-500", d, err)
		return
	}

	writeResult(w, "gateway-500", d, nil)
}

// clockSkewRequest names which process to skew. See internal/clock's own
// doc comment for why these two, and only these two, have a genuine
// clock-skew fault to offer at all -- every other timing decision in this
// codebase is computed by PostgreSQL's own now(), which this harness cannot
// meaningfully skew without actually changing the database container's
// system clock, a far more invasive and far less safely reversible action
// than this tool is willing to take. See docs/DECISIONS.md D51.
type clockSkewRequest struct {
	durationRequest
	Target        string  `json:"target"`
	OffsetSeconds float64 `json:"offset_seconds"`
}

// handleClockSkew skews the named target's clock for the requested duration,
// then resets it, by calling that process's own admin-only
// /internal/faults/clock-skew (internal/http.HandleClockSkew) -- a real
// mechanism this codebase built for this exact purpose, not a second one
// invented here.
func (h *harness) handleClockSkew(w nethttp.ResponseWriter, r *nethttp.Request) {
	var req clockSkewRequest
	if err := decodeBody(r, &req); err != nil {
		writeResult(w, "clock-skew", 0, fmt.Errorf("decode request: %w", err))
		return
	}
	d := req.duration()

	var targetURL string
	switch req.Target {
	case "api":
		targetURL = h.cfg.apiAdminURL
	case "saga-orchestrator":
		targetURL = h.cfg.sagaOrchestratorURL
	default:
		writeResult(w, "clock-skew", 0, fmt.Errorf("target must be \"api\" or \"saga-orchestrator\", got %q", req.Target))
		return
	}

	h.logger.WarnContext(r.Context(), "injecting fault: clock skew",
		slog.String("target", req.Target), slog.Float64("offset_seconds", req.OffsetSeconds), slog.Duration("duration", d))

	if err := h.setClockOffset(r.Context(), targetURL, req.OffsetSeconds); err != nil {
		writeResult(w, "clock-skew", d, err)
		return
	}

	time.Sleep(d)

	if err := h.setClockOffset(context.Background(), targetURL, 0); err != nil {
		writeResult(w, "clock-skew", d, err)
		return
	}

	writeResult(w, "clock-skew", d, nil)
}

func (h *harness) setClockOffset(ctx context.Context, targetURL string, offsetSeconds float64) error {
	body, err := json.Marshal(map[string]float64{"offset_seconds": offsetSeconds})
	if err != nil {
		return fmt.Errorf("marshal clock skew request: %w", err)
	}
	req, err := nethttp.NewRequestWithContext(ctx, nethttp.MethodPost,
		targetURL+"/internal/faults/clock-skew", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("build clock skew request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := h.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("set clock skew on %s: %w", targetURL, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("set clock skew on %s: unexpected status %d -- is LEDGER_FAULT_INJECTION_ENABLED set on that process?",
			targetURL, resp.StatusCode)
	}
	return nil
}
