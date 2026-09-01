package http

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// postTransactionRequest is the body of POST /v1/transactions.
//
// Every field is a plain wire type rather than a domain type, and the mapping
// into the domain is explicit below. That is more code than tagging the domain
// structs, and it buys the thing worth having: the JSON a client sends is
// pinned here, so renaming a field in internal/ledger cannot silently break
// every integration.
type postTransactionRequest struct {
	Type        string             `json:"type"`
	ExternalRef *string            `json:"external_ref,omitempty"`
	Metadata    map[string]any     `json:"metadata,omitempty"`
	Entries     []entryRequestJSON `json:"entries"`
}

type entryRequestJSON struct {
	AccountID string       `json:"account_id"`
	Direction string       `json:"direction"`
	Amount    ledger.Money `json:"amount"`
}

// toDomain converts and validates the shape of the request.
//
// It checks only what is wrong with the JSON itself -- an unparseable UUID, a
// missing field. Everything about whether the transaction is legal is left to
// ledger.TransactionRequest.validate, which owns those rules and states them
// once. A handler re-checking "at least two entries" here would be a second
// copy of a rule that is already enforced in two places.
func (r postTransactionRequest) toDomain() (ledger.TransactionRequest, error) {
	entries := make([]ledger.EntryRequest, len(r.Entries))
	for i, e := range r.Entries {
		accountID, err := uuid.Parse(e.AccountID)
		if err != nil {
			return ledger.TransactionRequest{}, fmt.Errorf("entry %d: account_id %q is not a UUID: %w: %w",
				i, e.AccountID, err, ledger.ErrInvalidEntry)
		}
		entries[i] = ledger.EntryRequest{
			AccountID: accountID,
			Direction: ledger.Direction(e.Direction),
			Amount:    e.Amount,
		}
	}

	return ledger.TransactionRequest{
		Type:        ledger.TransactionType(r.Type),
		ExternalRef: r.ExternalRef,
		Metadata:    r.Metadata,
		Entries:     entries,
	}, nil
}

// reverseTransactionRequest is the body of POST /v1/transactions/{id}/reverse.
type reverseTransactionRequest struct {
	Reason string `json:"reason"`
}

// transactionResponse is a transaction and its legs.
type transactionResponse struct {
	ID             string          `json:"id"`
	Type           string          `json:"type"`
	Status         string          `json:"status"`
	IdempotencyKey *string         `json:"idempotency_key,omitempty"`
	ExternalRef    *string         `json:"external_ref,omitempty"`
	Metadata       map[string]any  `json:"metadata,omitempty"`
	CreatedAt      time.Time       `json:"created_at"`
	PostedAt       *time.Time      `json:"posted_at,omitempty"`
	Entries        []entryResponse `json:"entries"`
}

type entryResponse struct {
	ID        string       `json:"id"`
	AccountID string       `json:"account_id"`
	Direction string       `json:"direction"`
	Amount    ledger.Money `json:"amount"`
	EntrySeq  int          `json:"entry_seq"`
	CreatedAt time.Time    `json:"created_at"`
}

func newTransactionResponse(t *ledger.Transaction) transactionResponse {
	entries := make([]entryResponse, len(t.Entries))
	for i, e := range t.Entries {
		entries[i] = entryResponse{
			ID:        e.ID.String(),
			AccountID: e.AccountID.String(),
			Direction: string(e.Direction),
			Amount:    e.Amount,
			EntrySeq:  e.EntrySeq,
			CreatedAt: e.CreatedAt,
		}
	}

	return transactionResponse{
		ID:             t.ID.String(),
		Type:           string(t.Type),
		Status:         string(t.Status),
		IdempotencyKey: t.IdempotencyKey,
		ExternalRef:    t.ExternalRef,
		Metadata:       t.Metadata,
		CreatedAt:      t.CreatedAt,
		PostedAt:       t.PostedAt,
		Entries:        entries,
	}
}

// balanceResponse is the synchronous, authoritative balance.
type balanceResponse struct {
	AccountID string       `json:"account_id"`
	Available ledger.Money `json:"available"`
	Pending   ledger.Money `json:"pending,omitempty"`
	Version   int64        `json:"version"`
	UpdatedAt time.Time    `json:"updated_at"`
}

// balanceAsOfResponse answers the temporal query.
//
// It carries `as_of` back rather than only the amount, because the answer is
// bounded-stale by design (D16): entries are stamped with transaction START
// time, so the same question asked twice can give different answers while an
// older transaction is still in flight. Echoing the instant makes it obvious
// which question was answered.
type balanceAsOfResponse struct {
	AccountID string       `json:"account_id"`
	AsOf      time.Time    `json:"as_of"`
	Balance   ledger.Money `json:"balance"`
}

// statementResponse is one keyset page of an account's history.
type statementResponse struct {
	AccountID string       `json:"account_id"`
	Currency  string       `json:"currency"`
	From      time.Time    `json:"from"`
	To        time.Time    `json:"to"`
	Opening   ledger.Money `json:"opening"`
	Closing   ledger.Money `json:"closing"`

	Lines []statementLineResponse `json:"lines"`

	// NextCursor is null on the last page. Opaque on purpose: see encodeCursor.
	NextCursor *string `json:"next_cursor"`
}

type statementLineResponse struct {
	Entry          entryResponse `json:"entry"`
	Signed         ledger.Money  `json:"signed"`
	RunningBalance ledger.Money  `json:"running_balance"`
}

func newStatementResponse(s ledger.Statement) statementResponse {
	lines := make([]statementLineResponse, len(s.Lines))
	for i, line := range s.Lines {
		lines[i] = statementLineResponse{
			Entry: entryResponse{
				ID:        line.Entry.ID.String(),
				AccountID: line.Entry.AccountID.String(),
				Direction: string(line.Entry.Direction),
				Amount:    line.Entry.Amount,
				EntrySeq:  line.Entry.EntrySeq,
				CreatedAt: line.Entry.CreatedAt,
			},
			Signed:         line.Signed,
			RunningBalance: line.RunningBalance,
		}
	}

	response := statementResponse{
		AccountID: s.AccountID.String(),
		Currency:  s.Currency,
		From:      s.From,
		To:        s.To,
		Opening:   s.Opening,
		Closing:   s.Closing,
		Lines:     lines,
	}
	if s.NextCursor != nil {
		cursor := encodeCursor(*s.NextCursor)
		response.NextCursor = &cursor
	}

	return response
}

// encodeCursor renders a keyset position as one opaque token.
//
// Opaque rather than two query parameters, and the reason is not tidiness. The
// cursor is a (created_at, id) pair because timestamps tie -- see
// StatementCursor -- and a client that could see those two fields would
// eventually construct its own, get the tie-breaking subtly wrong, and skip
// entries from a statement. An opaque token makes the only supported cursor the
// one this service issued.
//
// Base64 of a fixed two-field encoding, not JSON: it is short, and it has no
// structure to tempt anyone into parsing it.
func encodeCursor(c ledger.StatementCursor) string {
	raw := c.CreatedAt.UTC().Format(time.RFC3339Nano) + "|" + c.EntryID.String()
	return base64.RawURLEncoding.EncodeToString([]byte(raw))
}

// decodeCursor parses a token this service issued.
//
// Every failure is the same error, deliberately: a client that tampered with a
// cursor does not need to know which half it got wrong, and distinguishing the
// cases would only help someone probing the format.
func decodeCursor(token string) (*ledger.StatementCursor, error) {
	raw, err := base64.RawURLEncoding.DecodeString(token)
	if err != nil {
		return nil, fmt.Errorf("cursor is not valid: %w", ledger.ErrInvalidEntry)
	}

	at, id, ok := strings.Cut(string(raw), "|")
	if !ok {
		return nil, fmt.Errorf("cursor is not valid: %w", ledger.ErrInvalidEntry)
	}

	createdAt, err := time.Parse(time.RFC3339Nano, at)
	if err != nil {
		return nil, fmt.Errorf("cursor is not valid: %w", ledger.ErrInvalidEntry)
	}
	entryID, err := uuid.Parse(id)
	if err != nil {
		return nil, fmt.Errorf("cursor is not valid: %w", ledger.ErrInvalidEntry)
	}

	return &ledger.StatementCursor{CreatedAt: createdAt, EntryID: entryID}, nil
}
