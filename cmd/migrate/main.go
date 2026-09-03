// Command migrate applies every pending schema migration and exits.
//
// A one-shot binary, on the same principle as issue-api-key and kafka-init:
// applying schema changes is a separate deployment step, never something a
// service does at its own startup -- N replicas racing to migrate the same
// database is exactly the failure mode migrations/embed.go's own doc comment
// already rules out. This is that separate step, packaged so it needs
// nothing beyond what every other image here already has: it links
// migrations.FS directly rather than shipping a second copy of the SQL
// files or depending on the standalone golang-migrate CLI image
// (migrate/migrate:v4.17.1, what deploy/docker-compose.yml and
// docker-compose.prod.yml use) being available in a Kubernetes cluster that
// has no local bind mount to hand it the migrations/ directory the way a
// single host's docker compose does.
//
// Used as deploy/helm/ledger-core's pre-install/pre-upgrade hook Job -- see
// templates/migration-job.yaml -- so `helm upgrade` cannot roll a new image
// out to a schema it does not match: the hook must exit 0 before Helm
// proceeds to the Deployments at all.
package main

import (
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/satyamsipah/ledger-core/internal/config"
	"github.com/satyamsipah/ledger-core/internal/observability"
	"github.com/satyamsipah/ledger-core/migrations"
)

const serviceName = "migrate"

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "fatal: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	cfg, err := config.Load(serviceName)
	if err != nil {
		return err
	}

	logger := observability.NewLogger(cfg.Observability, serviceName, cfg.Env)
	logger.Info("starting", slog.String("env", cfg.Env))

	source, err := iofs.New(migrations.FS, ".")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}

	m, err := migrate.NewWithSourceInstance("iofs", source, migrateURL(cfg.Postgres.DSN))
	if err != nil {
		return fmt.Errorf("create migrator: %w", err)
	}
	defer func() {
		sourceErr, dbErr := m.Close()
		if sourceErr != nil {
			logger.Warn("close migration source", slog.String("error", sourceErr.Error()))
		}
		if dbErr != nil {
			logger.Warn("close migration db connection", slog.String("error", dbErr.Error()))
		}
	}()

	before, _, err := m.Version()
	if err != nil && !errors.Is(err, migrate.ErrNilVersion) {
		return fmt.Errorf("read current schema version: %w", err)
	}

	if err := m.Up(); err != nil {
		if errors.Is(err, migrate.ErrNoChange) {
			logger.Info("schema already up to date", slog.Uint64("version", uint64(before)))
			return nil
		}
		return fmt.Errorf("apply migrations: %w", err)
	}

	after, _, err := m.Version()
	if err != nil {
		return fmt.Errorf("read new schema version: %w", err)
	}
	logger.Info("migrations applied", slog.Uint64("from_version", uint64(before)), slog.Uint64("to_version", uint64(after)))

	return nil
}

// migrateURL rewrites a pgx DSN to the scheme golang-migrate's pgx/v5 driver
// registers itself under -- the same rewrite test/testdb.go's newMigrator
// applies, so this binary and the test suite migrate identically.
func migrateURL(dsn string) string {
	return "pgx5://" + strings.TrimPrefix(dsn, "postgres://")
}
