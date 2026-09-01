package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"time"
)

// Compile-time proof that the HTTP client satisfies the port.
var _ Client = (*HTTPClient)(nil)

// maxErrorBody bounds how much of a failing response is read before giving up,
// so a gateway answering an error with a gigabyte does not become this
// process's memory problem.
const maxErrorBody = 4 << 10

// HTTPClient talks to a gateway over HTTP.
type HTTPClient struct {
	baseURL      string
	http         *http.Client
	payTimeout   time.Duration
	probeTimeout time.Duration
}

// NewHTTPClient builds a client against a gateway's base URL.
//
// The two timeouts are separate because the two calls have opposite risk
// profiles. Pay may create a charge, so its timeout firing leaves an ambiguity
// somebody has to resolve -- it should be generous. Probe cannot create
// anything, so a short timeout on it costs nothing but another attempt.
func NewHTTPClient(baseURL string, payTimeout, probeTimeout time.Duration) *HTTPClient {
	return &HTTPClient{
		baseURL: baseURL,
		// No global Timeout on the http.Client: the per-call context deadline
		// is what bounds these requests, and a client-level timeout would
		// silently shorten a caller that asked for longer.
		http:         &http.Client{},
		payTimeout:   payTimeout,
		probeTimeout: probeTimeout,
	}
}

// Pay submits a payment under the request's idempotency key.
func (c *HTTPClient) Pay(ctx context.Context, req PaymentRequest) (*Payment, error) {
	ctx, cancel := context.WithTimeout(ctx, c.payTimeout)
	defer cancel()

	body, err := json.Marshal(map[string]any{
		"amount_minor": req.AmountMinor,
		"currency":     req.Currency,
		"reference":    req.Reference,
	})
	if err != nil {
		return nil, fmt.Errorf("encode payment request %s: %w", req.IdempotencyKey, err)
	}

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodPost,
		c.baseURL+"/v1/payments", bytes.NewReader(body))
	if err != nil {
		return nil, fmt.Errorf("build payment request %s: %w", req.IdempotencyKey, err)
	}
	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Idempotency-Key", req.IdempotencyKey)

	return c.do(httpReq, req.IdempotencyKey)
}

// Probe asks the gateway what became of a key.
func (c *HTTPClient) Probe(ctx context.Context, key string) (*Payment, error) {
	ctx, cancel := context.WithTimeout(ctx, c.probeTimeout)
	defer cancel()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet,
		c.baseURL+"/v1/payments/"+url.PathEscape(key), nil)
	if err != nil {
		return nil, fmt.Errorf("build probe request %s: %w", key, err)
	}

	return c.do(httpReq, key)
}

// do issues a request and maps the response onto the three-valued outcome.
//
// The mapping is the whole point of this function, so it is written as an
// explicit table rather than as a status-code range check:
//
//	200        -> the gateway's answer, succeeded or failed
//	404        -> ErrPaymentNotFound  (conclusive: no payment exists)
//	400/422    -> ErrInvalidRequest   (conclusive: deterministic rejection)
//	5xx, 429   -> ErrGatewayUnavailable (UNKNOWN: may or may not have happened)
//	transport  -> ErrGatewayUnavailable (UNKNOWN: this is the timeout case)
//
// Anything unrecognised falls to ErrGatewayUnavailable, and that default is
// deliberate: an unclassified response is one this code does not understand,
// and the safe reading of a response you do not understand is "I do not know",
// never "it failed".
func (c *HTTPClient) do(req *http.Request, key string) (*Payment, error) {
	resp, err := c.http.Do(req)
	if err != nil {
		// Includes context.DeadlineExceeded. A request that timed out may have
		// been fully processed by the gateway before the answer was lost, which
		// is exactly why this cannot be reported as a failure.
		return nil, fmt.Errorf("call gateway for %s: %w: %w", key, ErrGatewayUnavailable, err)
	}
	defer func() { _ = resp.Body.Close() }()

	switch {
	case resp.StatusCode == http.StatusOK, resp.StatusCode == http.StatusCreated:
		return decodePayment(resp.Body, key)
	case resp.StatusCode == http.StatusNotFound:
		return nil, fmt.Errorf("gateway key %s: %w", key, ErrPaymentNotFound)
	case resp.StatusCode == http.StatusBadRequest, resp.StatusCode == http.StatusUnprocessableEntity:
		return nil, fmt.Errorf("gateway key %s: %w: %s", key, ErrInvalidRequest, snippet(resp.Body))
	default:
		return nil, fmt.Errorf("gateway key %s returned %d: %w: %s",
			key, resp.StatusCode, ErrGatewayUnavailable, snippet(resp.Body))
	}
}

// paymentResponse is the gateway's wire shape.
type paymentResponse struct {
	Key         string `json:"idempotency_key"`
	Reference   string `json:"reference"`
	Status      string `json:"status"`
	Reason      string `json:"reason"`
	AmountMinor int64  `json:"amount_minor"`
	Currency    string `json:"currency"`
	CreatedAt   string `json:"created_at"`
}

func decodePayment(body io.Reader, key string) (*Payment, error) {
	var wire paymentResponse
	if err := json.NewDecoder(body).Decode(&wire); err != nil {
		// A 200 whose body cannot be parsed is not a failure: the gateway said
		// something, and this process could not read it. Unknown, therefore.
		return nil, fmt.Errorf("decode gateway response for %s: %w: %w", key, ErrGatewayUnavailable, err)
	}

	payment := &Payment{
		Key:         wire.Key,
		Reference:   wire.Reference,
		Status:      Status(wire.Status),
		Reason:      wire.Reason,
		AmountMinor: wire.AmountMinor,
		Currency:    wire.Currency,
	}
	if payment.Key == "" {
		payment.Key = key
	}
	if t, err := time.Parse(time.RFC3339Nano, wire.CreatedAt); err == nil {
		payment.CreatedAt = t
	}

	switch payment.Status {
	case StatusSucceeded:
		return payment, nil
	case StatusFailed:
		return payment, fmt.Errorf("gateway key %s: %w: %s", key, ErrPaymentDeclined, payment.Reason)
	default:
		return nil, fmt.Errorf("gateway key %s returned status %q: %w",
			key, wire.Status, ErrGatewayUnavailable)
	}
}

// snippet reads a bounded prefix of an error body for the log line.
func snippet(body io.Reader) string {
	b, err := io.ReadAll(io.LimitReader(body, maxErrorBody))
	if err != nil {
		return ""
	}
	return string(bytes.TrimSpace(b))
}

// IsUnknown reports whether err leaves the payment's outcome undecided, for
// call sites that read better as a question than as a negated Conclusive.
func IsUnknown(err error) bool { return errors.Is(err, ErrGatewayUnavailable) }
