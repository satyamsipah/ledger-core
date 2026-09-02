// Package pgauth implements the auth store against PostgreSQL.
package pgauth

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/satyamsipah/ledger-core/internal/auth"
)

// Compile-time proof that this package satisfies the port it claims to.
var _ auth.Store = (*Store)(nil)

// Store is the PostgreSQL-backed key store.
type Store struct {
	pool    *pgxpool.Pool
	timeout time.Duration
}

// New builds a store over an existing pool.
func New(pool *pgxpool.Pool, timeout time.Duration) *Store {
	return &Store{pool: pool, timeout: timeout}
}

// Authenticate hashes the presented key and looks it up by that hash alone.
//
// The comparison is an indexed equality check inside PostgreSQL, never a
// byte-by-byte comparison in this process against attacker-controlled input --
// the same pattern every major API-key issuer uses, and the reason it is safe
// without a constant-time compare in Go: there is nothing here for a timing
// side channel to measure. The database returns a row or it does not: it
// never reveals how close an incorrect hash came to matching.
func (s *Store) Authenticate(ctx context.Context, rawKey string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	hash := auth.HashKey(rawKey)

	var principalID string
	err := s.pool.QueryRow(ctx, `
		SELECT principal_id FROM api_keys
		 WHERE key_hash = $1 AND status = 'ACTIVE'`, hash[:]).
		Scan(&principalID)
	if errors.Is(err, pgx.ErrNoRows) {
		return "", auth.ErrInvalidAPIKey
	}
	if err != nil {
		return "", fmt.Errorf("authenticate api key: %w", err)
	}
	return principalID, nil
}

// Issue mints a new key for a principal.
func (s *Store) Issue(ctx context.Context, principalID string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, s.timeout)
	defer cancel()

	raw, hash, err := auth.GenerateKey()
	if err != nil {
		return "", err
	}

	id, err := uuid.NewV7()
	if err != nil {
		return "", fmt.Errorf("generate api key id: %w", err)
	}

	_, err = s.pool.Exec(ctx, `
		INSERT INTO api_keys (id, principal_id, key_hash, status)
		VALUES ($1, $2, $3, 'ACTIVE')`,
		id, principalID, hash[:])
	if err != nil {
		return "", fmt.Errorf("issue api key for %s: %w", principalID, err)
	}
	return raw, nil
}
