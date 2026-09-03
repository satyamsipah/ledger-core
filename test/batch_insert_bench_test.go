package test

import (
	"context"
	"strconv"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/stretchr/testify/require"
)

// Quantifies the batch-insert decision docs/DECISIONS.md's Phase 7 entry
// records: internal/ledger/pgledger/repository.go already inserts every
// transaction's journal entries in ONE statement via
// `INSERT ... SELECT * FROM unnest($1::uuid[], ...)`, not a loop of
// single-row INSERTs. Phase 7 asked for a batch-insert optimisation with a
// measured before/after; since the "after" already shipped, what remained
// honest to do was measure the "before" this codebase never actually ran in
// production, rather than reintroduce it as a real code path just to revert
// it.
//
// Every iteration runs inside BEGIN/ROLLBACK: the deferred balance trigger
// (migration 000005) only fires at COMMIT, so entries never need to balance
// for this to measure real insert cost, and rolling back means this
// benchmark leaves no trace in the database it runs against -- the same
// technique used for the index-effectiveness EXPLAIN ANALYZE comparison
// (docs/DECISIONS.md's Phase 7 entry) and for the identical reason.
//
//	go test -run '^$' -bench BenchmarkJournalEntryInsert -benchtime=200x ./test/...

// benchAccount creates a throwaway ASSET/INR account, inline rather than via
// testdb.go's own newTypedAccount: that helper is typed against *testing.T,
// and require's assertions need the identical account-creation SQL run
// against *testing.B here regardless. The Testcontainers-backed sharedPool
// this benchmark runs against is freshly migrated but never seeded (Phase 1
// deliberately keeps deploy/seed/seed.sql out of the test container, see
// test/testdb.go), so a seeded account like platform-bank-inr does not exist
// here to reuse.
func benchAccount(b *testing.B, ctx context.Context) uuid.UUID {
	b.Helper()
	id := uuid.New()
	_, err := sharedPool.Exec(ctx, `
		INSERT INTO accounts (id, external_ref, account_type, normal_balance, currency, owner_id, allow_negative, status)
		VALUES ($1, $2, 'ASSET', 'DEBIT', 'INR', NULL, TRUE, 'ACTIVE')`,
		id, "bench-"+id.String())
	require.NoError(b, err)
	return id
}

// insertNaiveLoop inserts n journal entries one row per round trip -- the
// shape this codebase never shipped, kept only as the "before" this
// benchmark exists to measure against.
func insertNaiveLoop(ctx context.Context, tx pgx.Tx, transactionID, accountID uuid.UUID, n int) error {
	for i := 0; i < n; i++ {
		if _, err := tx.Exec(ctx, `
			INSERT INTO journal_entries (id, transaction_id, account_id, direction, amount_minor, currency, entry_seq)
			VALUES ($1, $2, $3, 'DEBIT', 1, 'INR', $4)`,
			uuid.New(), transactionID, accountID, i,
		); err != nil {
			return err
		}
	}
	return nil
}

// insertUnnestBatch is the exact shape pgledger's own journal-entry insert
// uses: one statement, N rows, via unnest over parallel arrays.
func insertUnnestBatch(ctx context.Context, tx pgx.Tx, transactionID, accountID uuid.UUID, n int) error {
	ids := make([]uuid.UUID, n)
	txIDs := make([]uuid.UUID, n)
	accountIDs := make([]uuid.UUID, n)
	directions := make([]string, n)
	amounts := make([]int64, n)
	currencies := make([]string, n)
	seqs := make([]int32, n)
	for i := 0; i < n; i++ {
		ids[i] = uuid.New()
		txIDs[i] = transactionID
		accountIDs[i] = accountID
		directions[i] = "DEBIT"
		amounts[i] = 1
		currencies[i] = "INR"
		seqs[i] = int32(i)
	}
	_, err := tx.Exec(ctx, `
		INSERT INTO journal_entries (id, transaction_id, account_id, direction,
		                             amount_minor, currency, entry_seq)
		SELECT * FROM unnest($1::uuid[], $2::uuid[], $3::uuid[], $4::text[],
		                     $5::bigint[], $6::text[], $7::int[])`,
		ids, txIDs, accountIDs, directions, amounts, currencies, seqs)
	return err
}

func BenchmarkJournalEntryInsert(b *testing.B) {
	ctx := context.Background()
	accountID := benchAccount(b, ctx)

	run := func(b *testing.B, n int, insert func(context.Context, pgx.Tx, uuid.UUID, uuid.UUID, int) error) {
		for i := 0; i < b.N; i++ {
			b.StopTimer()
			tx, err := sharedPool.Begin(ctx)
			require.NoError(b, err)
			txID := uuid.New()
			_, err = tx.Exec(ctx, `INSERT INTO transactions (id, transaction_type) VALUES ($1, 'TRANSFER')`, txID)
			require.NoError(b, err)
			b.StartTimer()

			require.NoError(b, insert(ctx, tx, txID, accountID, n))

			b.StopTimer()
			require.NoError(b, tx.Rollback(ctx))
		}
	}

	for _, n := range []int{2, 10, 50} {
		label := strconv.Itoa(n)
		b.Run("unnest_batch/n="+label, func(b *testing.B) { run(b, n, insertUnnestBatch) })
		b.Run("naive_loop/n="+label, func(b *testing.B) { run(b, n, insertNaiveLoop) })
	}
}
