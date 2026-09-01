// Package gateway is the client for the external payment gateway the payout
// saga calls in its third step.
//
// The whole package is shaped by one fact: this is the only participant in the
// saga that is not in the ledger's database transaction, and therefore the only
// one whose outcome can be genuinely UNKNOWN. Every type here exists to keep
// that third answer expressible. A client that returned (bool, error) would
// force a timeout to be reported as one or the other, and both are wrong: a
// timeout reported as failure refunds payments that really went out, and a
// timeout reported as success pays merchants for payments that never happened.
package gateway

import (
	"context"
	"errors"
	"time"
)

// Status is a payment's outcome as the gateway reports it. There are only two
// values, because an unknown outcome is not a status the gateway ever returns
// -- it is the absence of an answer, and it travels as ErrGatewayUnavailable.
type Status string

// The two outcomes a gateway can report. Both are CONCLUSIVE: reaching either
// one means the question has been answered and the saga may act on it.
const (
	StatusSucceeded Status = "SUCCEEDED"
	StatusFailed    Status = "FAILED"
)

// Payment is the gateway's record of one payment.
type Payment struct {
	// Key is the idempotency key the payment was submitted under. It is stable
	// across every attempt of a saga's gateway step, which is what makes a
	// retry after an ambiguous outcome the same logical call rather than a
	// second charge.
	Key string

	// Reference is the gateway's own identifier, for support and reconciliation.
	Reference string

	Status      Status
	Reason      string
	AmountMinor int64
	Currency    string
	CreatedAt   time.Time
}

// Succeeded reports whether the gateway confirmed the payment.
func (p *Payment) Succeeded() bool { return p != nil && p.Status == StatusSucceeded }

// PaymentRequest is a payout submission.
type PaymentRequest struct {
	// IdempotencyKey must be derived from the saga and the step, never from the
	// attempt. An attempt-scoped key would make every retry a fresh payment,
	// which is the double-charge this field exists to prevent.
	IdempotencyKey string

	AmountMinor int64
	Currency    string
	Reference   string
}

// Client is the port the saga calls out through.
type Client interface {
	// Pay submits a payment. A conclusive decline is ErrPaymentDeclined; an
	// inconclusive result -- timeout, connection failure, 5xx -- is
	// ErrGatewayUnavailable and MUST NOT be read as either outcome.
	Pay(ctx context.Context, req PaymentRequest) (*Payment, error)

	// Probe asks what became of a payment submitted under key.
	//
	// This is the resolution path for an ambiguous outcome, and it is the only
	// legitimate one. It is a GET: it cannot itself cause a payment, so it is
	// safe to call repeatedly against a gateway whose state is unknown, which a
	// re-submission would not be.
	Probe(ctx context.Context, key string) (*Payment, error)
}

var (
	// ErrPaymentDeclined means the gateway conclusively refused the payment.
	// Conclusive is the operative word: this is the gateway saying no, not this
	// process failing to hear it.
	ErrPaymentDeclined = errors.New("gateway: payment was declined")

	// ErrPaymentNotFound means the gateway has no record of the key.
	//
	// THE ASSUMPTION THIS ENCODES, stated plainly because it is load-bearing
	// and not free: a probe returning "no such payment" is treated as
	// conclusive evidence that no payment was made, so the saga compensates.
	// That is sound only while the gateway's record of accepted payments is
	// durable and complete -- if it accepted a payment and then lost the
	// record, this answer is a lie and the saga refunds a customer whose money
	// really did leave. Real gateways provide that durability and it is the
	// basis on which every payments integration reconciles. It is recorded here
	// so that the day a gateway is found not to provide it, the consequence is
	// already written down rather than rediscovered from a support ticket.
	ErrPaymentNotFound = errors.New("gateway: no payment exists under this key")

	// ErrGatewayUnavailable means the outcome is UNKNOWN.
	//
	// Timeouts, connection failures and 5xx responses all land here, and they
	// land here together because they are indistinguishable in the only way
	// that matters: the request may or may not have been processed. Nothing may
	// be concluded from this error except that the question must be asked
	// again.
	ErrGatewayUnavailable = errors.New("gateway: outcome is unknown")

	// ErrInvalidRequest means the gateway rejected the request as malformed.
	// Deterministic, so retrying it is pointless; the saga compensates.
	ErrInvalidRequest = errors.New("gateway: request was rejected as invalid")
)

// Conclusive reports whether an error from Pay or Probe settles the question.
//
// A saga may only act on a conclusive answer. Everything else leaves it in
// GATEWAY_PENDING to probe again, and eventually escalates to manual review --
// which is the correct end state for "we still do not know", however
// unsatisfying.
func Conclusive(err error) bool {
	switch {
	case err == nil:
		return true
	case errors.Is(err, ErrPaymentDeclined),
		errors.Is(err, ErrPaymentNotFound),
		errors.Is(err, ErrInvalidRequest):
		return true
	default:
		return false
	}
}
