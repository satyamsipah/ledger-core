// Package migrations embeds the SQL migration files so that tests can apply the
// exact same schema the deployed stack runs, without depending on a path
// relative to the working directory.
//
// Nothing in the service binaries imports this package: migrations are applied
// by a separate step in the deployment, never by the application at startup.
// Applying schema changes from inside a service means N replicas racing to
// migrate the same database.
package migrations

import "embed"

// FS holds every .sql migration in this directory, ordered by filename as
// golang-migrate expects.
//
//go:embed *.sql
var FS embed.FS
