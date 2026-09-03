package test

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/satyamsipah/ledger-core/internal/ledger"
)

// TestGetTransaction_ReturnsHeaderAndEntries covers the ledger explorer's
// drill-down: the header plus every leg, in submission order.
func TestGetTransaction_ReturnsHeaderAndEntries(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	from := newAccount(t, ctx, sharedPool, "INR", true)
	to := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)
	posted := transfer(t, ctx, svc, from, to, 500_00)

	got, err := svc.GetTransaction(ctx, posted.ID)
	require.NoError(t, err)

	assert.Equal(t, posted.ID, got.ID)
	assert.Equal(t, ledger.TransactionTypeTransfer, got.Type)
	assert.Equal(t, ledger.TransactionStatusPosted, got.Status)
	require.Len(t, got.Entries, 2)
	assert.Equal(t, from, got.Entries[0].AccountID)
	assert.Equal(t, ledger.DirectionDebit, got.Entries[0].Direction)
	assert.Equal(t, int64(500_00), got.Entries[0].Amount.AmountMinor())
	assert.Equal(t, to, got.Entries[1].AccountID)
	assert.Equal(t, ledger.DirectionCredit, got.Entries[1].Direction)

	t.Run("should reject an unknown transaction id", func(t *testing.T) {
		t.Parallel()

		_, err := svc.GetTransaction(ctx, mustUUIDv7(t))
		require.ErrorIs(t, err, ledger.ErrTransactionNotFound)
	})
}

// TestSearchTransactions_FiltersAndPaginates covers the ledger explorer's
// search: every filter, and the id-descending keyset pagination that backs
// it.
//
// Every assertion scopes to accounts this test alone created, rather than to
// a shared time window, because the suite's tests run in parallel against one
// shared database and never truncate anything -- see main_test.go.
func TestSearchTransactions_FiltersAndPaginates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	float := newAccount(t, ctx, sharedPool, "INR", true)
	wallet := newAccount(t, ctx, sharedPool, "INR", true)

	marker := "search-" + mustUUIDv7(t).String()
	posted, err := svc.PostTransaction(ctx, ledger.TransactionRequest{
		Type:        ledger.TransactionTypeTransfer,
		ExternalRef: &marker,
		Entries: []ledger.EntryRequest{
			{AccountID: float, Direction: ledger.DirectionDebit, Amount: ledger.MustNewMoney(100_00, "INR")},
			{AccountID: wallet, Direction: ledger.DirectionCredit, Amount: ledger.MustNewMoney(100_00, "INR")},
		},
	})
	require.NoError(t, err)

	reversal, err := svc.ReverseTransaction(ctx, posted.ID, "test reversal")
	require.NoError(t, err)

	window := ledger.TransactionQuery{
		From: posted.CreatedAt.Add(-time.Minute),
		To:   time.Now().Add(time.Minute),
	}

	t.Run("should find a transaction by a substring of its external_ref", func(t *testing.T) {
		t.Parallel()

		q := window
		substr := marker[:len(marker)-4]
		q.ExternalRef = &substr

		page, err := svc.SearchTransactions(ctx, q)
		require.NoError(t, err)
		require.Len(t, page.Transactions, 1)
		assert.Equal(t, posted.ID, page.Transactions[0].ID)
		assert.Equal(t, ledger.TransactionStatusReversed, page.Transactions[0].Status,
			"the original is REVERSED, not POSTED, once it has been reversed")
		assert.Empty(t, page.Transactions[0].Entries, "search results carry no entries")
	})

	t.Run("should filter by status", func(t *testing.T) {
		t.Parallel()

		q := window
		q.ExternalRef = &marker
		q.Status = ledger.TransactionStatusPosted

		page, err := svc.SearchTransactions(ctx, q)
		require.NoError(t, err)
		assert.Empty(t, page.Transactions, "the original transaction is REVERSED, not POSTED, by now")
	})

	t.Run("should filter by account_id and find both legs of the reversal", func(t *testing.T) {
		t.Parallel()

		q := window
		q.AccountID = &wallet

		page, err := svc.SearchTransactions(ctx, q)
		require.NoError(t, err)
		require.Len(t, page.Transactions, 2)

		types := map[uuid.UUID]ledger.TransactionType{}
		for _, tx := range page.Transactions {
			types[tx.ID] = tx.Type
		}
		assert.Equal(t, ledger.TransactionTypeTransfer, types[posted.ID])
		assert.Equal(t, ledger.TransactionTypeReversal, types[reversal.ID])
	})

	t.Run("should filter by type", func(t *testing.T) {
		t.Parallel()

		q := window
		q.AccountID = &wallet
		q.Type = ledger.TransactionTypeReversal

		page, err := svc.SearchTransactions(ctx, q)
		require.NoError(t, err)
		require.Len(t, page.Transactions, 1)
		assert.Equal(t, reversal.ID, page.Transactions[0].ID)
	})

	t.Run("should paginate with no repeats and no gaps", func(t *testing.T) {
		t.Parallel()

		var (
			seen   []uuid.UUID
			cursor *uuid.UUID
			pages  int
		)
		for {
			q := window
			q.AccountID = &wallet
			q.Limit = 1
			q.After = cursor

			page, err := svc.SearchTransactions(ctx, q)
			require.NoError(t, err)
			pages++
			require.LessOrEqual(t, pages, 5, "pagination must terminate")

			if len(page.Transactions) == 0 {
				break
			}
			seen = append(seen, page.Transactions[0].ID)

			if page.NextCursor == nil {
				break
			}
			cursor = page.NextCursor
		}

		// Not asserting which of the two comes first: both were posted well
		// within the same millisecond in a fast test run, and UUIDv7 gives
		// D3's "usable rough time ordering", never a strict guarantee at that
		// resolution. What pagination promises -- and what this proves -- is
		// that paging returns each match exactly once.
		assert.ElementsMatch(t, []uuid.UUID{posted.ID, reversal.ID}, seen)
	})

	t.Run("should reject a period that runs backwards", func(t *testing.T) {
		t.Parallel()

		now := time.Now()
		_, err := svc.SearchTransactions(ctx, ledger.TransactionQuery{From: now, To: now.Add(-time.Hour)})
		require.ErrorIs(t, err, ledger.ErrInvalidEntry)
	})

	assertGlobalInvariant(t, ctx, sharedPool)
}

// TestGetAccount_ReturnsMetadata covers the account view's entry point.
func TestGetAccount_ReturnsMetadata(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)
	id := newTypedAccount(t, ctx, sharedPool, ledger.AccountTypeLiability, "INR", true)

	got, err := svc.GetAccount(ctx, id)
	require.NoError(t, err)
	assert.Equal(t, id, got.ID)
	assert.Equal(t, ledger.AccountTypeLiability, got.Type)
	assert.Equal(t, ledger.DirectionCredit, got.NormalBalance)
	assert.Equal(t, "INR", got.Currency)
	assert.Equal(t, ledger.AccountStatusActive, got.Status)

	t.Run("should reject an unknown account id", func(t *testing.T) {
		t.Parallel()

		_, err := svc.GetAccount(ctx, mustUUIDv7(t))
		require.ErrorIs(t, err, ledger.ErrAccountNotFound)
	})
}

// TestSearchAccounts_FiltersAndPaginates covers every filter, the exclusion
// of sharded accounts, and pagination.
func TestSearchAccounts_FiltersAndPaginates(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	svc := newLedgerService(sharedPool)

	marker := "acct-search-" + mustUUIDv7(t).String()
	owner := "owner-" + mustUUIDv7(t).String()

	ids := make([]uuid.UUID, 3)
	for i := range ids {
		ids[i] = newAccountWithRef(t, ctx, sharedPool, fmt.Sprintf("%s-%d", marker, i), &owner, "INR")
	}

	// Shares the marker but not the owner, to prove owner_id narrows results
	// rather than external_ref alone deciding the match.
	otherOwner := "owner-other-" + mustUUIDv7(t).String()
	newAccountWithRef(t, ctx, sharedPool, marker+"-other-owner", &otherOwner, "INR")

	t.Run("should find every account whose external_ref contains the marker", func(t *testing.T) {
		t.Parallel()

		page, err := svc.SearchAccounts(ctx, ledger.AccountQuery{ExternalRef: &marker, Limit: 50})
		require.NoError(t, err)
		assert.Len(t, page.Accounts, 4)
	})

	t.Run("should narrow to one owner", func(t *testing.T) {
		t.Parallel()

		page, err := svc.SearchAccounts(ctx, ledger.AccountQuery{ExternalRef: &marker, OwnerID: &owner, Limit: 50})
		require.NoError(t, err)
		require.Len(t, page.Accounts, 3)
		for _, a := range page.Accounts {
			require.NotNil(t, a.OwnerID)
			assert.Equal(t, owner, *a.OwnerID)
		}
	})

	t.Run("should paginate with no repeats and no gaps", func(t *testing.T) {
		t.Parallel()

		var (
			seen   []uuid.UUID
			cursor *uuid.UUID
			pages  int
		)
		for {
			page, err := svc.SearchAccounts(ctx, ledger.AccountQuery{
				ExternalRef: &marker, OwnerID: &owner, Limit: 1, After: cursor,
			})
			require.NoError(t, err)
			pages++
			require.LessOrEqual(t, pages, 5, "pagination must terminate")

			if len(page.Accounts) == 0 {
				break
			}
			seen = append(seen, page.Accounts[0].ID)

			if page.NextCursor == nil {
				break
			}
			cursor = page.NextCursor
		}

		require.Len(t, seen, 3)
		assert.ElementsMatch(t, ids, seen)
	})

	t.Run("should exclude shards even when their external_ref matches", func(t *testing.T) {
		t.Parallel()

		parent := newAccount(t, ctx, sharedPool, "USD", true)
		shardMarker := "shard-search-" + mustUUIDv7(t).String()
		_, err := sharedPool.Exec(ctx, `UPDATE accounts SET external_ref = $2 WHERE id = $1`, parent, shardMarker)
		require.NoError(t, err)

		// ledger_shard_account names each shard "<parent's external_ref>#shard-N",
		// so a substring search for shardMarker matches every shard's own
		// external_ref too -- which is what makes this a real test of the
		// parent_account_id IS NULL filter rather than a search term that
		// merely happens not to hit any shard.
		shardAccount(t, ctx, sharedPool, parent, 2)

		page, err := svc.SearchAccounts(ctx, ledger.AccountQuery{ExternalRef: &shardMarker, Limit: 50})
		require.NoError(t, err)
		require.Len(t, page.Accounts, 1, "only the logical parent, never its shards")
		assert.Equal(t, parent, page.Accounts[0].ID)
	})
}

// newAccountWithRef inserts an ACTIVE asset account with a caller-chosen
// external_ref and owner_id, for tests that need to search on them -- unlike
// newAccount/newTypedAccount, whose external_ref is a fixed "test-<uuid>"
// pattern with no owner.
func newAccountWithRef(
	t *testing.T,
	ctx context.Context,
	pool *pgxpool.Pool,
	externalRef string,
	ownerID *string,
	currency string,
) uuid.UUID {
	t.Helper()

	id := mustUUIDv7(t)
	_, err := pool.Exec(ctx, `
		INSERT INTO accounts (id, external_ref, account_type, normal_balance,
		                      currency, owner_id, allow_negative, status)
		VALUES ($1, $2, 'ASSET', 'DEBIT', $3, $4, TRUE, 'ACTIVE')`,
		id, externalRef, currency, ownerID)
	require.NoError(t, err, "insert account with external_ref %q", externalRef)

	return id
}
