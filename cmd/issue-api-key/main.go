// Command issue-api-key mints one API key for one principal and exits.
//
// A one-shot binary, on the same principle as migrate and kafka-init: this is
// the smallest thing that makes authentication real rather than theoretical.
// It is deliberately NOT an admin API -- there is no listing, no revocation,
// no rotation here. Those are real admin-surface features and belong with the
// admin dashboard when it exists; this exists only to close docs/DECISIONS.md
// D24, which needed principals to be possible to create, not a key-management
// product.
//
// The raw key is printed to stdout exactly once. It is never stored anywhere
// -- api_keys.key_hash is the one-way digest -- so losing this output means
// issuing a new key, not recovering the old one.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/satyamsipah/ledger-core/internal/auth/pgauth"
	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/db"
	"github.com/satyamsipah/ledger-core/internal/observability"
)

const serviceName = "issue-api-key"

func main() {
	principal := flag.String("principal", "", "the principal id this key authenticates as (required)")
	flag.Parse()

	if err := run(*principal); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run(principal string) error {
	if principal == "" {
		return fmt.Errorf("-principal is required")
	}

	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Observability, serviceName, cfg.Env)

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	pool, err := db.NewPool(ctx, cfg.Postgres, logger)
	if err != nil {
		return fmt.Errorf("init postgres: %w", err)
	}
	defer pool.Close()

	store := pgauth.New(pool.Pool, cfg.Postgres.QueryTimeout)

	rawKey, err := store.Issue(ctx, principal)
	if err != nil {
		return fmt.Errorf("issue key for %s: %w", principal, err)
	}

	fmt.Printf("principal: %s\nkey:       %s\n\n", principal, rawKey)
	fmt.Fprintln(os.Stderr, "This key is shown once and is not recoverable. Store it now.")
	return nil
}
