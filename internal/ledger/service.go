package ledger

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"sort"
	"time"

	"github.com/google/uuid"

	"github.com/satyamsipah/ledger-core/internal/idempotency"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/internal/outbox"
)

// Event types name what happened. They are stable strings, not versioned in
// their own spelling -- EventVersion (below) is what a consumer compares when
// deciding whether it understands a payload, replacing the ".v1"-suffix scheme
// Phase 3 used. A consumer branching on an integer does not need to parse a
// string suffix to do it.
const (
	EventTypeTransactionPosted   = "TransactionPosted"
	EventTypeTransactionReversed = "TransactionReversed"
	EventTypeAccountCreated      = "AccountCreated"
	EventTypeBalanceUpdated      = "BalanceUpdated"
)

// eventVersion is the schema version stamped on every event this package
// emits. One constant rather than one per event type: every payload below
// changed together, in this phase, so they start together at 1. A future
// change to just one payload shape earns that one event type its own bumped
// version instead of moving this constant, which is why this is not named
// eventVersion1 or otherwise implied to be the only version that will ever
// exist.
const eventVersion int16 = 1

// Metadata keys used by reversals.
//
// The link from a reversal back to the transaction it undoes lives in
// transactions.metadata rather than in a dedicated column, because the schema
// has no such column and adding one was deliberately deferred. Nothing depends
// on it for correctness -- double reversal is prevented by the status
// transition, not by this key -- so it is an audit trail, not a mechanism.
const (
	MetadataKeyReverses = "reverses_transaction_id"
	MetadataKeyReason   = "reversal_reason"
)

// Statement page sizing. The maximum exists because a statement page is
// materialised in memory on both sides of the API.
const (
	DefaultStatementLimit = 100
	MaxStatementLimit     = 1000
)

// Search page sizing, for SearchTransactions and SearchAccounts. Shared
// between the two rather than each defining its own: both are dashboard list
// views with the identical reason for the cap DefaultStatementLimit already
// states.
const (
	DefaultSearchLimit = 100
	MaxSearchLimit     = 1000
)

// EntryRequest is one requested leg of a transaction.
type EntryRequest struct {
	AccountID uuid.UUID
	Direction Direction
	Amount    Money
}

// ResponseRenderer turns a transaction that is about to commit into the exact
// HTTP response a later retry will be handed.
//
// It is a callback, and the inversion is deliberate. The response has to be
// durable at the same instant the journal entries are, which means it must be
// produced *before* COMMIT -- so the usual middleware shape, where the response
// is captured after the handler returns, cannot supply it. Rendering is an HTTP
// concern and stays in the HTTP layer; the transaction boundary is a service
// concern and stays here. A callback is the honest way to have both.
//
// It runs inside the database transaction, so it must not perform I/O.
type ResponseRenderer func(*Transaction) (status int, body []byte, err error)

// Recorder is caller-supplied work that runs inside the ledger's transaction,
// after the entries and balance deltas are applied and before COMMIT.
//
// It exists so a saga's state transition can be made durable at the same
// instant as the money it describes. The alternative -- post, then record in a
// second transaction -- is the shape docs/DECISIONS.md D20 rejected on the
// write path, and it is worse here: the step being replayed after a crash is a
// debit against a customer's wallet rather than a duplicate response.
//
// Like ResponseRenderer it runs while account row locks are held, so it must
// not perform I/O. Unlike ResponseRenderer it is handed the transaction, which
// is what lets it write through Tx's own narrow set of statements rather than
// through a pool. There is deliberately no way to reach the raw pgx.Tx from
// here: a caller that could would be able to take locks outside the single
// global ordering D11 depends on, which is the one thing the pgledger package
// exists to prevent.
type Recorder func(ctx context.Context, tx Tx, posted *Transaction) error

// Idempotent binds a request to an idempotency key that has already been
// reserved by the caller.
//
// Key and Render travel together because neither is useful alone: a key with no
// renderer would leave the record IN_PROGRESS over a committed transaction --
// precisely the state the design guarantees is unreachable -- and a renderer
// with no key has nothing to write. Making them one struct means the broken
// combination cannot be expressed.
type Idempotent struct {
	// PrincipalID names the caller this key is scoped to. It travels
	// separately from TransactionRequest.PrincipalID because the two answer
	// different questions -- this one picks which idempotency_keys row gets
	// completed, that one stamps who the ledger attributes the transaction to
	// -- even though both are set from the same authenticated caller on every
	// HTTP request today. See docs/DECISIONS.md D24.
	PrincipalID string

	// Key is a reservation this caller already holds in IN_PROGRESS. The
	// service does not acquire it; by the time a request reaches here, the HTTP
	// layer has already resolved replay, conflict and in-progress.
	Key string

	// Render produces the response stored with the transaction.
	Render ResponseRenderer
}

// TransactionRequest is a complete transaction to post. It is validated as a
// whole before anything touches the database, so a malformed request never
// takes a row lock.
type TransactionRequest struct {
	// PrincipalID attributes this transaction to an authenticated caller.
	// Empty for internal callers with no principal -- sagas stamp it from
	// their own saga.Instance.PrincipalID; nothing else does yet.
	PrincipalID string

	Type        TransactionType
	ExternalRef *string
	Metadata    map[string]any
	Entries     []EntryRequest

	// IdempotencyKey populates transactions.idempotency_key, whose partial
	// unique index is the database's own defence of invariant 5. It is set from
	// Idempotency.Key when that is present, and may be set alone by internal
	// callers that want the column populated without a replay record.
	IdempotencyKey *string

	// Idempotency, when set, completes the reserved key inside the same
	// database transaction as the journal entries. This is the mechanism behind
	// invariant 5; see internal/idempotency for why the atomicity is the whole
	// guarantee.
	Idempotency *Idempotent

	// Record, when set, runs inside the same database transaction as the
	// journal entries. The saga orchestrator uses it to advance a saga's status
	// and write its audit row atomically with the money movement they describe.
	Record Recorder
}

// Service posts and reverses transactions and answers balance questions.
//
// It holds no state beyond its repository: concurrency safety here comes from
// database row locks, not from anything this struct could protect with a mutex,
// and a service that carried per-account state in memory would be wrong the
// moment a second replica started.
type Service struct {
	repo    Repository
	metrics *observability.Metrics
}

// NewService wires a service to its repository.
//
// metrics may be nil -- callers that only need PostTransaction's return
// value and do not care about ledger_transactions_posted_total, chiefly
// tests, are not required to construct a registry just to satisfy this
// signature. Every read of it below is guarded accordingly, the same
// nil-safe convention internal/db.Retrier already uses for the identical
// reason.
func NewService(repo Repository, metrics *observability.Metrics) *Service {
	return &Service{repo: repo, metrics: metrics}
}

// CreateAccountRequest is a new account to open.
type CreateAccountRequest struct {
	ExternalRef   string
	Type          AccountType
	Currency      string
	OwnerID       *string
	AllowNegative bool
}

// CreateAccount opens an account and emits AccountCreated, inside one database
// transaction.
//
// There is no row lock to take here -- a brand-new UUID has no concurrent
// writer to serialise against -- so the only reason this runs inside
// s.repo.InTx at all is invariant 6: the account row and the event describing
// it must commit together, and InTx is the one place in this package that
// makes that true. NormalBalance is derived from Type
// (AccountType.NormalBalance in types.go) rather than accepted from the
// caller, which is what makes accounts_normal_balance_matches_type_check
// unreachable through this path rather than merely satisfied by a caller who
// remembered to align the two.
//
// The account_balances row is not created here. Migration 000009's trigger
// does it, unconditionally, on every INSERT into accounts regardless of which
// code path performed it -- and that is deliberate: a lock on a balance row
// that does not exist locks nothing and reports no error while doing so (see
// that migration's comment), so guaranteeing the row's existence is a
// correctness property of the schema, not a convenience this method could
// also provide by hand and might one day forget to.
func (s *Service) CreateAccount(ctx context.Context, req CreateAccountRequest) (*Account, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate account id: %w", err)
	}

	var created *Account
	err = s.repo.InTx(ctx, func(ctx context.Context, tx Tx) error {
		account := &Account{
			ID:            id,
			ExternalRef:   req.ExternalRef,
			Type:          req.Type,
			NormalBalance: req.Type.NormalBalance(),
			Currency:      req.Currency,
			OwnerID:       req.OwnerID,
			AllowNegative: req.AllowNegative,
			Status:        AccountStatusActive,
		}
		if err := tx.InsertAccount(ctx, account); err != nil {
			return err
		}

		payload, err := json.Marshal(accountCreatedEvent{
			AccountID:     account.ID,
			ExternalRef:   account.ExternalRef,
			Type:          account.Type,
			NormalBalance: account.NormalBalance,
			Currency:      account.Currency,
			OwnerID:       account.OwnerID,
			AllowNegative: account.AllowNegative,
		})
		if err != nil {
			return fmt.Errorf("marshal %s event for account %s: %w", EventTypeAccountCreated, account.ID, err)
		}

		if err := tx.AppendEvent(ctx, outbox.Event{
			AggregateType: outbox.AggregateAccount,
			AggregateID:   account.ID.String(),
			EventType:     EventTypeAccountCreated,
			EventVersion:  eventVersion,
			OccurredAt:    account.CreatedAt,
			Payload:       payload,
		}); err != nil {
			return err
		}

		created = account
		return nil
	})
	if err != nil {
		return nil, err
	}

	return created, nil
}

// accountCreatedEvent is the payload for EventTypeAccountCreated.
type accountCreatedEvent struct {
	AccountID     uuid.UUID   `json:"account_id"`
	ExternalRef   string      `json:"external_ref"`
	Type          AccountType `json:"account_type"`
	NormalBalance Direction   `json:"normal_balance"`
	Currency      string      `json:"currency"`
	OwnerID       *string     `json:"owner_id,omitempty"`
	AllowNegative bool        `json:"allow_negative"`
}

// validate checks everything that can be known without touching the database,
// on the same principle as TransactionRequest.validate: the database enforces
// these constraints regardless, but a caller should get a domain error rather
// than a raw constraint violation for a mistake this cheap to catch first.
func (r CreateAccountRequest) validate() error {
	if r.ExternalRef == "" {
		return fmt.Errorf("create account: external_ref is required: %w", ErrInvalidEntry)
	}
	if !r.Type.Valid() {
		return fmt.Errorf("create account: account type %q: %w", r.Type, ErrInvalidAccountType)
	}
	if _, err := NewMoney(0, r.Currency); err != nil {
		return fmt.Errorf("create account: %w", err)
	}
	return nil
}

// PostTransaction writes a balanced transaction and moves the affected
// balances, all inside one database transaction.
//
// The ordering below is not arbitrary. Locks come first so that every later
// read is of state nobody else can change; the deferred balance trigger fires
// last, at COMMIT, on entries that are all present by then; and the outbox row
// commits with the journal it describes, which is what makes invariant 6 hold
// without a distributed transaction.
func (s *Service) PostTransaction(ctx context.Context, req TransactionRequest) (*Transaction, error) {
	start := time.Now()
	posted, err := s.postTransaction(ctx, req)
	s.observeTransaction(req.Type, start, err)
	return posted, err
}

// observeTransaction records ledger_transactions_posted_total and
// ledger_transaction_duration_seconds for one PostTransaction or reverse
// call, success or failure alike.
//
// Recorded here -- the funnel both the HTTP API and the saga orchestrator's
// direct calls into this package pass through -- rather than in the HTTP
// layer, because ledger_http_requests_total already exists for HTTP traffic
// and would both double-count it and miss every saga-driven post entirely,
// which never travels through an HTTP handler at all.
func (s *Service) observeTransaction(t TransactionType, start time.Time, err error) {
	if s.metrics == nil {
		return
	}

	label := string(t)
	if !t.Valid() {
		// validate() runs inside postTransaction, after this label is read in
		// the caller above, so an invalid type can reach here. Folding every
		// invalid value into one label keeps a client sending garbage types
		// from minting a new Prometheus series per string it tries.
		label = "invalid"
	}
	status := "success"
	if err != nil {
		status = "error"
	}

	s.metrics.TransactionsPosted.WithLabelValues(label, status).Inc()
	s.metrics.TransactionDuration.WithLabelValues(label).Observe(time.Since(start).Seconds())
}

func (s *Service) postTransaction(ctx context.Context, req TransactionRequest) (*Transaction, error) {
	if err := req.validate(); err != nil {
		return nil, err
	}

	// The key reaches the transactions row whichever way the caller supplied it,
	// so transactions_idempotency_key_key -- the defence that owes nothing to
	// this package's correctness -- is armed either way.
	if req.Idempotency != nil {
		key := req.Idempotency.Key
		req.IdempotencyKey = &key
	}

	// Generated once, OUTSIDE any retry the caller may have wrapped around this
	// call, and reused by every attempt. A fresh id per attempt would be simpler
	// and is wrong in one specific way: if an attempt ever did commit and was
	// then reported as aborted, a second id would post the money twice, where a
	// reused one collides on the primary key and says so.
	txID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate transaction id: %w", err)
	}

	var posted *Transaction
	err = s.repo.InTx(ctx, func(ctx context.Context, tx Tx) error {
		// Routing happens first, inside the transaction and before any lock,
		// because the accounts that get locked are the ones it picks.
		entryRequests, err := routeToShards(ctx, tx, req.Entries)
		if err != nil {
			return err
		}

		accountIDs := sortedAccountIDs(entryRequests)

		accounts, err := lockAndValidate(ctx, tx, accountIDs, entryRequests)
		if err != nil {
			return err
		}

		header := &Transaction{
			ID:             txID,
			IdempotencyKey: req.IdempotencyKey,
			PrincipalID:    req.PrincipalID,
			Type:           req.Type,
			Status:         TransactionStatusPosted,
			ExternalRef:    req.ExternalRef,
			Metadata:       req.Metadata,
		}
		if err := tx.InsertTransaction(ctx, header); err != nil {
			return err
		}

		entries, err := buildEntries(txID, entryRequests)
		if err != nil {
			return err
		}
		if err := tx.InsertEntries(ctx, entries); err != nil {
			return err
		}
		header.Entries = entries

		balances, err := applyEntries(ctx, tx, entries, accounts, accountIDs)
		if err != nil {
			return err
		}

		if err := appendTransactionEvent(ctx, tx, EventTypeTransactionPosted, header, balances, nil, ""); err != nil {
			return err
		}

		// Before the idempotency completion, so that a Recorder returning an
		// error aborts the whole transaction with the journal entries still
		// uncommitted. A saga whose transition is refused -- because another
		// orchestrator took its lease and moved it on -- must not leave the
		// money moved.
		if req.Record != nil {
			if err := req.Record(ctx, tx, header); err != nil {
				return err
			}
		}

		// Last, and inside this transaction. Everything the response describes
		// is now present in this transaction's snapshot, so the stored body is
		// a description of state that either becomes durable with it or
		// disappears with it. There is no third outcome, and that absence is
		// invariant 5.
		if err := completeIdempotency(ctx, tx, req.Idempotency, header); err != nil {
			return err
		}

		posted = header
		return nil
	})
	if err != nil {
		return nil, err
	}

	return posted, nil
}

// completeIdempotency renders the response and writes the terminal record,
// inside the caller's transaction.
//
// Rendering happens here rather than after COMMIT because a response rendered
// afterwards would have to be stored afterwards, and a store afterwards is a
// second transaction -- which is the failure this whole phase is built to make
// unreachable. The cost is that Render runs while account row locks are held,
// so it must not do I/O; the ResponseRenderer doc comment says so.
func completeIdempotency(ctx context.Context, tx Tx, idem *Idempotent, header *Transaction) error {
	if idem == nil {
		return nil
	}
	if idem.Render == nil {
		return fmt.Errorf("idempotency key %s: %w", idem.Key, ErrMissingRenderer)
	}

	status, body, err := idem.Render(header)
	if err != nil {
		return fmt.Errorf("render response for transaction %s: %w", header.ID, err)
	}

	return tx.CompleteIdempotency(ctx, idempotency.Completion{
		PrincipalID:    idem.PrincipalID,
		Key:            idem.Key,
		ResponseStatus: status,
		ResponseBody:   body,
		TransactionID:  header.ID,
	})
}

// ReverseTransaction undoes a posted transaction by writing a new one whose
// legs are mirrored, and marks the original REVERSED.
//
// The original's entries are never touched. That is not merely because the
// database rejects the UPDATE -- it is because a ledger whose history can be
// edited cannot answer "what did we believe on Tuesday?", which is the question
// every dispute, audit and reconciliation eventually reduces to. The correction
// is itself a fact, with its own timestamp, sitting after the mistake.
//
// Reversal can legitimately fail with ErrInsufficientFunds: undoing a transfer
// moves money back out of the receiving account, and that account may have
// spent it. The caller has to resolve that, and no amount of retrying will.
func (s *Service) ReverseTransaction(ctx context.Context, txID uuid.UUID, reason string) (*Transaction, error) {
	return s.reverse(ctx, txID, reason, nil, nil)
}

// ReverseTransactionIdempotent reverses a transaction under a reserved
// idempotency key.
//
// A reversal needs idempotency at least as much as a posting does, and arguably
// more: the status transition already makes a second reversal fail loudly, but
// it fails with ErrAlreadyReversed, which a retrying client cannot distinguish
// from "somebody else reversed this behind my back". Replaying the original
// response answers the question the retry was actually asking.
func (s *Service) ReverseTransactionIdempotent(
	ctx context.Context,
	txID uuid.UUID,
	reason string,
	idem *Idempotent,
) (*Transaction, error) {
	return s.reverse(ctx, txID, reason, idem, nil)
}

// ReverseTransactionRecorded reverses a transaction and runs record inside the
// same database transaction.
//
// This is how a saga compensates. The compensation's journal entries and the
// saga's move to COMPENSATED become durable together, so an orchestrator that
// dies immediately after compensating cannot come back and compensate again --
// and a compensation that the saga's own state refuses cannot leave money
// moved.
func (s *Service) ReverseTransactionRecorded(
	ctx context.Context,
	txID uuid.UUID,
	reason string,
	record Recorder,
) (*Transaction, error) {
	return s.reverse(ctx, txID, reason, nil, record)
}

func (s *Service) reverse(ctx context.Context, txID uuid.UUID, reason string, idem *Idempotent, record Recorder) (*Transaction, error) {
	start := time.Now()
	reversal, err := s.doReverse(ctx, txID, reason, idem, record)
	s.observeTransaction(TransactionTypeReversal, start, err)
	return reversal, err
}

func (s *Service) doReverse(ctx context.Context, txID uuid.UUID, reason string, idem *Idempotent, record Recorder) (*Transaction, error) {
	if reason == "" {
		return nil, ErrReversalReasonRequired
	}

	reversalID, err := uuid.NewV7()
	if err != nil {
		return nil, fmt.Errorf("generate reversal id: %w", err)
	}

	var reversal *Transaction
	err = s.repo.InTx(ctx, func(ctx context.Context, tx Tx) error {
		// The header lock is taken before any account lock, and PostTransaction
		// never locks a transactions row at all -- it inserts its own, which
		// nobody else can see yet. So the only cycle this ordering could create
		// would need two reversals of the same transaction, and those contend
		// on this very row: one wins, the other finds it REVERSED.
		original, err := tx.LoadTransactionForUpdate(ctx, txID)
		if err != nil {
			return err
		}

		switch original.Status {
		case TransactionStatusReversed:
			return fmt.Errorf("reverse transaction %s: %w", txID, ErrAlreadyReversed)
		case TransactionStatusPosted:
		default:
			return fmt.Errorf("reverse transaction %s in status %s: %w",
				txID, original.Status, ErrTransactionNotPosted)
		}

		if len(original.Entries) == 0 {
			return fmt.Errorf("reverse transaction %s: no journal entries: %w", txID, ErrTooFewEntries)
		}

		// Mirroring is deliberately mechanical: same accounts, same amounts,
		// opposite directions. Because a reversal is a per-leg mirror it stays
		// correct for a multi-currency transaction too, each currency balancing
		// on its own exactly as it did originally.
		//
		// NOT re-routed through routeToShards, and that is load-bearing rather
		// than an omission. The original's entries already name physical
		// accounts -- the shards its money actually went to -- so mirroring
		// them takes the money back out of those same shards. Routing a
		// reversal afresh would pick shards at random and could drive one
		// negative while a sibling held the funds, turning a correction into an
		// insufficient-funds failure on an account that plainly has the money.
		mirrored := make([]EntryRequest, len(original.Entries))
		for i, e := range original.Entries {
			mirrored[i] = EntryRequest{
				AccountID: e.AccountID,
				Direction: e.Direction.Opposite(),
				Amount:    e.Amount,
			}
		}

		accountIDs := sortedAccountIDs(mirrored)
		accounts, err := lockAndValidate(ctx, tx, accountIDs, mirrored)
		if err != nil {
			return err
		}

		metadata := map[string]any{
			MetadataKeyReverses: txID.String(),
			MetadataKeyReason:   reason,
		}

		header := &Transaction{
			ID:       reversalID,
			Type:     TransactionTypeReversal,
			Status:   TransactionStatusPosted,
			Metadata: metadata,
		}
		if idem != nil {
			key := idem.Key
			header.IdempotencyKey = &key
			header.PrincipalID = idem.PrincipalID
		}
		if err := tx.InsertTransaction(ctx, header); err != nil {
			return err
		}

		entries, err := buildEntries(reversalID, mirrored)
		if err != nil {
			return err
		}
		if err := tx.InsertEntries(ctx, entries); err != nil {
			return err
		}
		header.Entries = entries

		balances, err := applyEntries(ctx, tx, entries, accounts, accountIDs)
		if err != nil {
			return err
		}

		// Status only. The header row's other columns, and every one of its
		// journal entries, are left exactly as they were.
		if err := tx.MarkReversed(ctx, txID); err != nil {
			return err
		}

		if err := appendTransactionEvent(ctx, tx, EventTypeTransactionReversed, header, balances, &txID, reason); err != nil {
			return err
		}

		// Same ordering as the posting path: the recorder runs before the
		// idempotency completion, so refusing the saga transition takes the
		// compensation's journal entries down with it.
		if record != nil {
			if err := record(ctx, tx, header); err != nil {
				return err
			}
		}

		if err := completeIdempotency(ctx, tx, idem, header); err != nil {
			return err
		}

		reversal = header
		return nil
	})
	if err != nil {
		return nil, err
	}

	return reversal, nil
}

// GetBalance reads the synchronous balance maintained by the posting path.
//
// This is the authoritative balance, not the Kafka-driven projection: it is
// updated inside the posting transaction under the account's row lock, which is
// what lets the overdraft CHECK mean anything. The projection is a separate
// read model, and the two disagreeing is precisely the signal the
// reconciliation engine exists to catch.
func (s *Service) GetBalance(ctx context.Context, accountID uuid.UUID) (Balance, error) {
	return s.repo.GetBalance(ctx, accountID)
}

// GetBalanceAsOf recomputes an account's balance from the journal as it stood
// at an instant.
//
// BOUNDED STALENESS, DELIBERATELY ACCEPTED: journal_entries.created_at defaults
// to now(), which in PostgreSQL is transaction START time, not commit time. A
// transaction that begins at 12:00:00 and commits at 12:00:03 writes entries
// stamped 12:00:00, so a query asking for the balance at 12:00:01 -- run at
// 12:00:02, before that commit -- misses entries it will include if asked
// again later. The answer is therefore monotonic only once no transaction older
// than the requested instant is still in flight.
//
// The alternative, clock_timestamp(), was rejected: it would give the legs of a
// single transaction different timestamps, so a statement could show half a
// transfer, and the atomicity the rest of the system works to guarantee would
// stop being visible in the one place users actually look. A genuinely
// monotonic temporal view needs a commit-ordered sequence, which belongs with
// the reconciliation engine. See docs/DECISIONS.md, Phase 2.
func (s *Service) GetBalanceAsOf(ctx context.Context, accountID uuid.UUID, at time.Time) (Money, error) {
	return s.repo.GetBalanceAsOf(ctx, accountID, at)
}

// GetStatement returns one page of an account's history with a running balance.
//
// Pagination is keyset rather than OFFSET because the journal is append-only
// and constantly growing: OFFSET 50000 makes the database walk 50,000 rows it
// then discards, and any entry inserted between two page requests shifts every
// subsequent offset by one, so a client paging through a busy account both
// pays more per page and silently skips rows.
func (s *Service) GetStatement(ctx context.Context, q StatementQuery) (Statement, error) {
	if q.AccountID == uuid.Nil {
		return Statement{}, fmt.Errorf("statement: account id is required: %w", ErrAccountNotFound)
	}
	if q.To.Before(q.From) {
		return Statement{}, fmt.Errorf("statement: period ends (%s) before it starts (%s): %w",
			q.To, q.From, ErrInvalidEntry)
	}

	switch {
	case q.Limit <= 0:
		q.Limit = DefaultStatementLimit
	case q.Limit > MaxStatementLimit:
		q.Limit = MaxStatementLimit
	}

	return s.repo.GetStatement(ctx, q)
}

// GetTransaction reads one transaction and its entries.
//
// This is a plain read of history that, once POSTED, the journal's own
// append-only guarantee already makes immutable -- so unlike
// LoadTransactionForUpdate there is no lock to take and nothing this method
// needs to serialise against.
func (s *Service) GetTransaction(ctx context.Context, id uuid.UUID) (*Transaction, error) {
	return s.repo.GetTransaction(ctx, id)
}

// SearchTransactions returns one keyset-paginated page of transaction
// headers matching q, newest first.
func (s *Service) SearchTransactions(ctx context.Context, q TransactionQuery) (TransactionPage, error) {
	if q.To.IsZero() {
		q.To = time.Now().UTC()
	}
	if q.To.Before(q.From) {
		return TransactionPage{}, fmt.Errorf("search transactions: period ends (%s) before it starts (%s): %w",
			q.To, q.From, ErrInvalidEntry)
	}

	switch {
	case q.Limit <= 0:
		q.Limit = DefaultSearchLimit
	case q.Limit > MaxSearchLimit:
		q.Limit = MaxSearchLimit
	}

	return s.repo.SearchTransactions(ctx, q)
}

// GetAccount reads one account's metadata.
func (s *Service) GetAccount(ctx context.Context, id uuid.UUID) (*Account, error) {
	return s.repo.GetAccount(ctx, id)
}

// SearchAccounts returns one keyset-paginated page of accounts matching q,
// newest first.
func (s *Service) SearchAccounts(ctx context.Context, q AccountQuery) (AccountPage, error) {
	switch {
	case q.Limit <= 0:
		q.Limit = DefaultSearchLimit
	case q.Limit > MaxSearchLimit:
		q.Limit = MaxSearchLimit
	}

	return s.repo.SearchAccounts(ctx, q)
}

// ---------------------------------------------------------------------------
// Request validation
// ---------------------------------------------------------------------------

// validate checks everything that can be known without touching the database.
//
// It duplicates constraints the database also enforces, on purpose. The
// database is what makes the invariants true; this is what makes the failures
// legible -- a caller gets ErrTooFewEntries rather than a check_violation
// raised by a trigger at COMMIT, several layers away from the mistake.
func (r TransactionRequest) validate() error {
	if !r.Type.Valid() {
		return fmt.Errorf("transaction type %q: %w", r.Type, ErrInvalidTransactionType)
	}
	if len(r.Entries) < 2 {
		return fmt.Errorf("%d entries: %w", len(r.Entries), ErrTooFewEntries)
	}

	currency := r.Entries[0].Amount.Currency()

	debits, err := NewMoney(0, currency)
	if err != nil {
		return err
	}
	credits := debits

	for i, e := range r.Entries {
		if e.AccountID == uuid.Nil {
			return fmt.Errorf("entry %d: account id is required: %w", i, ErrInvalidEntry)
		}
		if !e.Direction.Valid() {
			return fmt.Errorf("entry %d: direction %q: %w", i, e.Direction, ErrInvalidEntry)
		}
		if err := e.Amount.Validate(); err != nil {
			return fmt.Errorf("entry %d: %w", i, err)
		}
		if e.Amount.AmountMinor() <= 0 {
			return fmt.Errorf("entry %d: amount %s must be positive: %w", i, e.Amount, ErrInvalidEntry)
		}

		// Phase 2 posts one currency per transaction. The schema is more
		// permissive -- the deferred trigger balances per currency so that an FX
		// transaction can carry both legs -- but nothing here yet decides an
		// exchange rate or where the sub-unit residue lands, and a transaction
		// spanning currencies without those answers is a rounding bug waiting
		// for a quiet moment.
		if e.Amount.Currency() != currency {
			return fmt.Errorf("entry %d is %s, transaction is %s: %w",
				i, e.Amount.Currency(), currency, ErrMixedCurrency)
		}

		if e.Direction == DirectionDebit {
			debits, err = debits.Add(e.Amount)
		} else {
			credits, err = credits.Add(e.Amount)
		}
		if err != nil {
			return fmt.Errorf("total entry %d: %w", i, err)
		}
	}

	if !debits.Equal(credits) {
		return fmt.Errorf("debits %s, credits %s: %w", debits, credits, ErrUnbalancedTransaction)
	}

	return nil
}

// ---------------------------------------------------------------------------
// Posting mechanics shared by PostTransaction and ReverseTransaction
// ---------------------------------------------------------------------------

// sortedAccountIDs returns the distinct accounts a set of entries touches, in
// ascending id order.
//
// DEADLOCK PREVENTION -- and precisely which part of it happens here.
//
// The hazard: two concurrent transfers, A->B and B->A, each locking its
// accounts in the order the client listed them. One holds A and waits for B,
// the other holds B and waits for A. PostgreSQL breaks the cycle by killing one
// of them about a second later, and under load that becomes a steady drip of
// failed payments that retrying cannot fix, because the retries deadlock too.
//
// The fix is a single global order on the lock hierarchy, so that a cycle --
// which requires two transactions acquiring the same pair in opposite orders --
// cannot be constructed. That order is established in two places, and it is
// worth being exact about which does what:
//
//   - The ORDER BY id in pgledger's LockAccounts query is what actually
//     sequences lock ACQUISITION. It is the mechanism.
//   - This sort is what makes the order deterministic everywhere else: the
//     balance UPDATEs in applyEntries run in this order, and passing a
//     pre-sorted array keeps the Go-side sequence identical to the SQL-side one
//     rather than merely compatible with it. Those UPDATEs cannot deadlock on
//     their own -- every row is already locked by then -- but a second ordering
//     that disagreed with the first is exactly the kind of detail that becomes
//     a deadlock the day someone splits the locking query in two.
//
// TestPostTransaction_ConcurrentOppositeTransfersDoNotDeadlock covers the
// mechanism end to end; it fails with real 40P01 deadlocks if the ordering is
// removed from either place.
func sortedAccountIDs(entries []EntryRequest) []uuid.UUID {
	seen := make(map[uuid.UUID]struct{}, len(entries))
	ids := make([]uuid.UUID, 0, len(entries))
	for _, e := range entries {
		if _, ok := seen[e.AccountID]; ok {
			continue
		}
		seen[e.AccountID] = struct{}{}
		ids = append(ids, e.AccountID)
	}

	sort.Slice(ids, func(i, j int) bool {
		return ids[i].String() < ids[j].String()
	})
	return ids
}

// routeToShards rewrites entries naming a sharded account to name one of its
// shards instead.
//
// # WHY RANDOM, AND WHY ONE SHARD PER ACCOUNT PER TRANSACTION
//
// Random because the goal is to spread row-lock contention evenly, and any
// deterministic key would cluster. Hashing on the counterparty account, for
// instance, sends every payment from one busy merchant to the same shard --
// which is the original problem with extra steps. Random has no such structure
// to be unlucky about.
//
// One shard per logical account per transaction, because a transaction touching
// the same account twice would otherwise take two shard locks for one logical
// movement: twice the lock footprint, and two balance rows moved where the
// aggregation in applyEntries expects one.
//
// # WHAT THIS COSTS
//
// A debit is checked against the chosen shard's balance, not the logical total,
// so a sharded account can refuse a debit it could plainly afford -- see
// migration 000012 and D24. Safety survives (every shard is non-negative, so
// their sum is), liveness does not. That is why sharding is only correct on
// accounts whose traffic is effectively one-directional.
func routeToShards(ctx context.Context, tx Tx, entries []EntryRequest) ([]EntryRequest, error) {
	shards, err := tx.ResolveShards(ctx, sortedAccountIDs(entries))
	if err != nil {
		return nil, err
	}
	if len(shards) == 0 {
		// Nothing here is sharded, which is the overwhelmingly common case.
		// Returning the caller's slice untouched keeps the ordinary posting
		// path free of any allocation this feature added.
		return entries, nil
	}

	chosen := make(map[uuid.UUID]uuid.UUID, len(shards))
	for accountID, ids := range shards {
		// math/rand rather than crypto/rand: this spreads write contention
		// across shards, and predicting which shard a payment lands on reveals
		// nothing an attacker could use. Every shard is an equally valid home
		// for the entry.
		//nolint:gosec // G404: shard selection is load balancing, not a secret.
		chosen[accountID] = ids[rand.IntN(len(ids))]
	}

	routed := make([]EntryRequest, len(entries))
	for i, e := range entries {
		routed[i] = e
		if shard, ok := chosen[e.AccountID]; ok {
			routed[i].AccountID = shard
		}
	}

	return routed, nil
}

// lockAndValidate takes the row locks and checks everything that depends on
// account state, which can only be known once nobody else can change it.
func lockAndValidate(ctx context.Context, tx Tx, accountIDs []uuid.UUID, entries []EntryRequest) (map[uuid.UUID]LockedAccount, error) {
	locked, err := tx.LockAccounts(ctx, accountIDs)
	if err != nil {
		return nil, err
	}

	accounts := make(map[uuid.UUID]LockedAccount, len(locked))
	for _, a := range locked {
		accounts[a.ID] = a
	}

	for _, id := range accountIDs {
		account, ok := accounts[id]
		if !ok {
			return nil, fmt.Errorf("account %s: %w", id, ErrAccountNotFound)
		}
		if !account.Status.Postable() {
			return nil, fmt.Errorf("account %s is %s: %w", id, account.Status, ErrAccountNotPostable)
		}
	}

	for i, e := range entries {
		account := accounts[e.AccountID]
		if e.Amount.Currency() != account.Currency {
			return nil, fmt.Errorf("entry %d posts %s to account %s which holds %s: %w",
				i, e.Amount.Currency(), e.AccountID, account.Currency, ErrCurrencyMismatch)
		}
	}

	return accounts, nil
}

// buildEntries turns validated requests into journal entries, numbering
// entry_seq from zero in request order so a transaction's legs read back the
// way they were submitted.
func buildEntries(txID uuid.UUID, requests []EntryRequest) ([]JournalEntry, error) {
	entries := make([]JournalEntry, len(requests))
	for i, r := range requests {
		id, err := uuid.NewV7()
		if err != nil {
			return nil, fmt.Errorf("generate entry id %d: %w", i, err)
		}
		entries[i] = JournalEntry{
			ID:            id,
			TransactionID: txID,
			AccountID:     r.AccountID,
			Direction:     r.Direction,
			Amount:        r.Amount,
			EntrySeq:      i,
		}
	}
	return entries, nil
}

// applyEntries aggregates entries into one delta per account and writes them in
// the same order the locks were taken.
//
// The overdraft check here produces ErrInsufficientFunds, a typed domain error
// a handler can turn into a 422. The identical check in
// account_balances_no_overdraft_check is what actually guarantees the
// invariant. Neither makes the other redundant: this one exists to give a good
// answer, that one exists to be true even when a future code path forgets to
// ask.
func applyEntries(
	ctx context.Context,
	tx Tx,
	entries []JournalEntry,
	accounts map[uuid.UUID]LockedAccount,
	order []uuid.UUID,
) ([]eventBalance, error) {
	deltas := make(map[uuid.UUID]int64, len(order))
	lastEntry := make(map[uuid.UUID]uuid.UUID, len(order))

	for _, e := range entries {
		account := accounts[e.AccountID]

		signed, err := signedAmount(e.Direction, account.NormalBalance, e.Amount.AmountMinor())
		if err != nil {
			return nil, fmt.Errorf("entry %s: %w", e.ID, err)
		}

		total, ok := addInt64(deltas[e.AccountID], signed)
		if !ok {
			return nil, fmt.Errorf("aggregate entries for account %s: %w", e.AccountID, ErrMoneyOverflow)
		}
		deltas[e.AccountID] = total
		lastEntry[e.AccountID] = e.ID
	}

	balances := make([]eventBalance, 0, len(order))
	for _, id := range order {
		account := accounts[id]
		delta := deltas[id]

		projected, ok := addInt64(account.AvailableMinor, delta)
		if !ok {
			return nil, fmt.Errorf("apply %d to account %s balance %d: %w",
				delta, id, account.AvailableMinor, ErrMoneyOverflow)
		}
		if projected < 0 && !account.AllowNegative {
			return nil, fmt.Errorf("account %s would fall to %d minor units: %w",
				id, projected, ErrInsufficientFunds)
		}

		balance, err := tx.ApplyBalanceDelta(ctx, BalanceDelta{
			AccountID:       id,
			DeltaMinor:      delta,
			ExpectedVersion: account.Version,
			LastEntryID:     lastEntry[id],
		})
		if err != nil {
			return nil, err
		}

		balances = append(balances, eventBalance{
			AccountID: id,
			Available: balance.Available,
			Version:   balance.Version,
		})
	}

	return balances, nil
}

// ---------------------------------------------------------------------------
// Outbox events
// ---------------------------------------------------------------------------

// transactionEvent is the payload published for a posted or reversed
// transaction.
//
// It carries the resulting balances and their versions, not just the entries,
// so the Kafka-driven projector can apply an event exactly once: a consumer
// that has already seen version 7 for an account can discard a redelivery of it
// without re-deriving anything. Delivery is at-least-once, so that check is not
// optional.
type transactionEvent struct {
	TransactionID uuid.UUID         `json:"transaction_id"`
	Type          TransactionType   `json:"type"`
	Status        TransactionStatus `json:"status"`
	Currency      string            `json:"currency,omitempty"`
	CreatedAt     time.Time         `json:"created_at"`
	PostedAt      *time.Time        `json:"posted_at,omitempty"`
	Reverses      *uuid.UUID        `json:"reverses_transaction_id,omitempty"`
	Reason        string            `json:"reason,omitempty"`
	Entries       []eventEntry      `json:"entries"`
	Balances      []eventBalance    `json:"balances"`
}

type eventEntry struct {
	EntryID   uuid.UUID `json:"entry_id"`
	AccountID uuid.UUID `json:"account_id"`
	Direction Direction `json:"direction"`
	Amount    Money     `json:"amount"`
	EntrySeq  int       `json:"entry_seq"`
}

type eventBalance struct {
	AccountID uuid.UUID `json:"account_id"`
	Available Money     `json:"available"`
	Version   int64     `json:"version"`
}

func appendTransactionEvent(
	ctx context.Context,
	tx Tx,
	eventType string,
	header *Transaction,
	balances []eventBalance,
	reverses *uuid.UUID,
	reason string,
) error {
	entries := make([]eventEntry, len(header.Entries))
	for i, e := range header.Entries {
		entries[i] = eventEntry{
			EntryID:   e.ID,
			AccountID: e.AccountID,
			Direction: e.Direction,
			Amount:    e.Amount,
			EntrySeq:  e.EntrySeq,
		}
	}

	payload, err := json.Marshal(transactionEvent{
		TransactionID: header.ID,
		Type:          header.Type,
		Status:        header.Status,
		Currency:      uniformCurrency(header.Entries),
		CreatedAt:     header.CreatedAt,
		PostedAt:      header.PostedAt,
		Reverses:      reverses,
		Reason:        reason,
		Entries:       entries,
		Balances:      balances,
	})
	if err != nil {
		return fmt.Errorf("marshal %s event for %s: %w", eventType, header.ID, err)
	}

	// PostedAt over CreatedAt when both exist: it is the more precise answer to
	// "when did this take effect" for a transaction that spent time PENDING
	// before posting. PostTransaction always posts synchronously so the two
	// are identical there, but reversal and any future saga-posted path can
	// legitimately have them differ, and this is correct for both without a
	// caller having to know which.
	occurredAt := header.CreatedAt
	if header.PostedAt != nil {
		occurredAt = *header.PostedAt
	}

	if err := tx.AppendEvent(ctx, outbox.Event{
		AggregateType: outbox.AggregateTransaction,
		AggregateID:   header.ID.String(),
		EventType:     eventType,
		EventVersion:  eventVersion,
		OccurredAt:    occurredAt,
		Payload:       payload,
	}); err != nil {
		return err
	}

	return appendBalanceUpdatedEvents(ctx, tx, header, balances, occurredAt)
}

// appendBalanceUpdatedEvents emits one BalanceUpdated event per account this
// transaction touched, in addition to the single TransactionPosted or
// TransactionReversed event appendTransactionEvent already wrote.
//
// THIS IS WHERE "PARTITION KEY = ACCOUNT_ID" ACTUALLY APPLIES. A transaction
// event is keyed by transaction id because a transaction inherently touches
// two or more accounts and cannot honestly be keyed by one of them (see
// AggregateTransaction's doc comment). A balance update is, by construction,
// about exactly one account, so it is the natural place to key by account_id
// and get the real per-account ordering guarantee that gives: every
// BalanceUpdated event that ever mentions this account, across every
// transaction that has touched it, is delivered to one consumer in commit
// order.
//
// WHAT THIS DOES NOT GIVE, AND WHY THE PROJECTOR DOES NOT NEED IT ANYWAY: the
// debit-side and credit-side BalanceUpdated events of one transfer land on two
// different accounts' partitions, with no ordering relationship between them
// -- a consumer that has processed one cannot assume the other has arrived.
// The projector sidesteps this by consuming TransactionPosted instead (which
// carries every touched account's resulting balance in one message) and by
// applying balances as a version compare-and-set rather than a delta, which
// converges to the correct state regardless of arrival order. BalanceUpdated
// exists for consumers that want a lighter, per-account message and do not
// need the whole transaction shape -- a cache invalidator, a notification
// service -- and for them, the per-account ordering above is the real and
// useful guarantee.
func appendBalanceUpdatedEvents(ctx context.Context, tx Tx, header *Transaction, balances []eventBalance, occurredAt time.Time) error {
	for _, b := range balances {
		payload, err := json.Marshal(balanceUpdatedEvent{
			AccountID:     b.AccountID,
			TransactionID: header.ID,
			Available:     b.Available,
			Version:       b.Version,
		})
		if err != nil {
			return fmt.Errorf("marshal %s event for account %s: %w", EventTypeBalanceUpdated, b.AccountID, err)
		}

		if err := tx.AppendEvent(ctx, outbox.Event{
			AggregateType: outbox.AggregateAccount,
			AggregateID:   b.AccountID.String(),
			EventType:     EventTypeBalanceUpdated,
			EventVersion:  eventVersion,
			OccurredAt:    occurredAt,
			Payload:       payload,
		}); err != nil {
			return err
		}
	}
	return nil
}

// balanceUpdatedEvent is the payload for EventTypeBalanceUpdated.
//
// Available is the resulting balance, not a delta -- the same design choice
// transactionEvent.Balances already made, and for the identical reason: a
// consumer applying "set to this value at this version" converges to the
// correct state under redelivery and out-of-order arrival alike, where a delta
// would not.
type balanceUpdatedEvent struct {
	AccountID     uuid.UUID `json:"account_id"`
	TransactionID uuid.UUID `json:"transaction_id"`
	Available     Money     `json:"available"`
	Version       int64     `json:"version"`
}

// uniformCurrency returns the shared currency of a set of entries, or the empty
// string when they span several. Empty rather than a guess: a consumer reading
// one currency off a multi-currency transaction would convert the wrong legs.
func uniformCurrency(entries []JournalEntry) string {
	if len(entries) == 0 {
		return ""
	}
	currency := entries[0].Amount.Currency()
	for _, e := range entries[1:] {
		if e.Amount.Currency() != currency {
			return ""
		}
	}
	return currency
}
