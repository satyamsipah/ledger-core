// Package payout orchestrates the marketplace payout saga: reserve a
// customer's funds into platform suspense, call an external payment gateway,
// and settle to the merchant and to fee revenue -- compensating if any of that
// fails.
//
// ORCHESTRATION, NOT CHOREOGRAPHY. The state machine lives in one place and one
// component drives it. See docs/DECISIONS.md for the argument against the
// event-driven alternative; the short version is that a saga's hardest question
// is "what happened to the gateway call", and answering it requires a component
// that knows the whole sequence rather than participants that each know one
// edge of it.
//
// THREE STEPS, NOT FIVE. "Debit the customer" and "credit suspense" are the two
// legs of ONE transaction, because a transaction carrying only a debit sums to
// a non-zero value and the deferred balance trigger rejects it at COMMIT --
// invariant 1 is not negotiable and is not a thing a saga may step around.
// Settlement is likewise one transaction whose suspense debit balances the
// merchant and fee credits.
package payout

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/gateway"
	"github.com/satyamsipah/ledger-core/internal/ledger"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/saga"
)

// SagaType names this saga in saga_instances.saga_type.
const SagaType = "MARKETPLACE_PAYOUT"

// Metadata keys linking a ledger transaction back to the saga that posted it.
// The link is an audit trail, not a mechanism: nothing reads it to make a
// decision, exactly as D15 says of the reversal back-link.
const (
	MetadataKeySagaID = "saga_id"
	MetadataKeyStep   = "saga_step"
)

// Payload is the saga's input, stored in saga_instances.payload.
//
// IMMUTABLE once written. A resume after a crash must resume with the same
// inputs the earlier attempt used: a compensation derived from a mutated
// payload would reverse a different transfer than the one that was posted, and
// would balance perfectly while doing it.
type Payload struct {
	CustomerWalletID  uuid.UUID `json:"customer_wallet_id"`
	SuspenseID        uuid.UUID `json:"platform_suspense_id"`
	MerchantPayableID uuid.UUID `json:"merchant_payable_id"`
	FeeRevenueID      uuid.UUID `json:"fee_revenue_id"`

	AmountMinor int64  `json:"amount_minor"`
	FeeMinor    int64  `json:"fee_minor"`
	Currency    string `json:"currency"`

	ExternalRef string `json:"external_ref,omitempty"`
}

// validate rejects a payload before any money moves.
func (p Payload) validate() error {
	switch {
	case p.CustomerWalletID == uuid.Nil, p.SuspenseID == uuid.Nil,
		p.MerchantPayableID == uuid.Nil, p.FeeRevenueID == uuid.Nil:
		return fmt.Errorf("payout: every account id is required: %w", ledger.ErrInvalidEntry)
	case p.AmountMinor <= 0:
		return fmt.Errorf("payout: amount must be positive: %w", ledger.ErrInvalidEntry)
	case p.FeeMinor < 0:
		return fmt.Errorf("payout: fee must not be negative: %w", ledger.ErrInvalidEntry)
	case p.FeeMinor >= p.AmountMinor:
		// A fee equal to the payout leaves the merchant a zero-amount leg,
		// which journal_entries_amount_check rejects; a fee larger than it is
		// a pricing bug that would silently invert the transfer.
		return fmt.Errorf("payout: fee %d must be less than amount %d: %w",
			p.FeeMinor, p.AmountMinor, ledger.ErrInvalidEntry)
	case p.Currency == "":
		return fmt.Errorf("payout: currency is required: %w", ledger.ErrInvalidCurrency)
	}
	return nil
}

// merchantMinor is what the merchant actually receives.
func (p Payload) merchantMinor() int64 { return p.AmountMinor - p.FeeMinor }

// Config is the orchestrator's tuning, supplied by internal/config.
type Config struct {
	// SagaType is the value this orchestrator claims work under. Defaults to
	// SagaType.
	//
	// Configurable rather than hardcoded because claims are scoped by it: two
	// orchestrators sharing a database must not take each other's sagas hostage
	// for a lease at a time, and running the same definition under a second
	// name -- a canary, a per-tenant variant -- is then a config change rather
	// than a fork.
	SagaType string

	WorkerID                string
	ClaimInterval           time.Duration
	ClaimBatch              int
	Lease                   time.Duration
	StepTimeout             time.Duration
	MaxStepAttempts         int
	MaxCompensationAttempts int
	SweepInterval           time.Duration
	MaxProbes               int
}

// Orchestrator drives payout sagas.
//
// It holds no per-saga state. Two replicas racing on one saga are resolved by
// the lease and by the guarded transition, not by anything this struct could
// protect with a mutex -- which is what lets it scale horizontally with no
// leader election and no coordination service.
type Orchestrator struct {
	store   saga.Store
	ledger  *ledger.Service
	gateway gateway.Client
	logger  *slog.Logger
	metrics *observability.Metrics
	cfg     Config

	// beforeSettleCommit fires inside the settle transaction, after the journal
	// entries are inserted and before the saga transition. See WithCrashHook.
	beforeSettleCommit func()
}

// New wires an orchestrator to its dependencies.
func New(
	store saga.Store,
	ledgerService *ledger.Service,
	gatewayClient gateway.Client,
	logger *slog.Logger,
	metrics *observability.Metrics,
	cfg Config,
) *Orchestrator {
	if cfg.ClaimInterval <= 0 {
		cfg.ClaimInterval = 250 * time.Millisecond
	}
	if cfg.ClaimBatch <= 0 {
		cfg.ClaimBatch = 50
	}
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = 10 * time.Second
	}
	if cfg.MaxProbes <= 0 {
		cfg.MaxProbes = 6
	}
	if cfg.SagaType == "" {
		cfg.SagaType = SagaType
	}
	return &Orchestrator{
		store:   store,
		ledger:  ledgerService,
		gateway: gatewayClient,
		logger:  logger,
		metrics: metrics,
		cfg:     cfg,
	}
}

// WithCrashHook installs a function that runs inside the settle transaction,
// after its journal entries are inserted and before the saga is advanced.
//
// It exists so a test can kill this process's database connection at the one
// instant where a crash could plausibly duplicate a side effect, and prove that
// it does not. Exported and deliberately separate from Config, following the
// polling publisher's WithCrashHook: a field on Config could be set by ordinary
// configuration loading, whereas this has to be reached for by name, from a
// test, or not at all.
func (o *Orchestrator) WithCrashHook(fn func()) { o.beforeSettleCommit = fn }

// Start creates a payout saga. It does not drive it: the claim loop picks it
// up, so a crashed API process cannot leave a saga nobody owns.
//
// idempotencyKey, when set, makes a retried request return the saga it already
// started rather than starting a second one -- the saga-level counterpart of
// invariant 5.
func (o *Orchestrator) Start(ctx context.Context, p Payload, principalID string, idempotencyKey *string) (*saga.Instance, error) {
	if err := p.validate(); err != nil {
		return nil, err
	}

	payload, err := json.Marshal(p)
	if err != nil {
		return nil, fmt.Errorf("encode payout payload: %w", err)
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate saga id: %w", err)
	}

	created, err := o.store.Create(ctx, saga.Instance{
		ID:             id,
		SagaType:       o.cfg.SagaType,
		PrincipalID:    principalID,
		CurrentStep:    saga.StepReserve,
		Status:         saga.StatusPending,
		Payload:        payload,
		IdempotencyKey: idempotencyKey,
		StepDeadlineAt: time.Now().Add(o.cfg.StepTimeout),
	})
	if err != nil {
		return nil, err
	}

	o.logger.InfoContext(ctx, "payout saga started",
		slog.String("saga_id", created.ID.String()),
		slog.Int64("amount_minor", p.AmountMinor),
		slog.String("currency", p.Currency))

	return created, nil
}

// gatewayKey is the idempotency key sent to the external gateway.
//
// A pure function of the saga id, and therefore STABLE across every attempt,
// every retry and every restart. That is what makes re-submitting after an
// ambiguous outcome safe rather than a second charge, and it is also why the
// key survives a crash that loses everything this process held in memory: it
// can be recomputed from the saga's own identity.
//
// Deriving it from the attempt number instead would be the single most
// expensive bug available in this design -- every retry would become a fresh
// payment, and the customer would be charged once per timeout.
func gatewayKey(sagaID uuid.UUID) string { return sagaID.String() + ":GATEWAY" }

// stepKey is the ledger idempotency key for a saga's ledger step.
//
// It is the third independent defence, in the same sense as D20's: even if the
// lease, the guarded transition and this orchestrator's own logic were all
// wrong, transactions_idempotency_key_key would refuse the second post.
func stepKey(sagaID uuid.UUID, step saga.Step, direction saga.Direction) string {
	return fmt.Sprintf("%s:%s:%s", sagaID, step, direction)
}

// unmarshalPayload reads a saga's immutable input.
func unmarshalPayload(in *saga.Instance) (Payload, error) {
	var p Payload
	if err := json.Unmarshal(in.Payload, &p); err != nil {
		return Payload{}, fmt.Errorf("decode payload of saga %s: %w", in.ID, err)
	}
	return p, nil
}
