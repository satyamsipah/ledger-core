package reconciliation

import "errors"

// Domain errors are sentinels, matching the convention every other package in
// this codebase follows (see .claude/rules/go-style.md): callers branch on
// errors.Is rather than matching strings.
var (
	// ErrEmptyStatement means a PSP file parsed to zero rows. Not a malformed
	// file -- a genuinely empty settlement file is a real, if unusual, state
	// -- but running a match against it would silently produce a report
	// claiming everything the ledger posted is MISSING_IN_PSP, which is almost
	// always a sign the wrong file was pointed at rather than a quiet day.
	ErrEmptyStatement = errors.New("reconciliation: PSP statement has no rows")

	// ErrMalformedRecord means one row of the PSP file could not be parsed --
	// a non-integer amount, an unparsable timestamp, a missing external_ref.
	// Wrapped with the row number and column at the point it is returned.
	ErrMalformedRecord = errors.New("reconciliation: malformed PSP statement row")

	// ErrRunNotFound means no reconciliation_runs row exists for the given id.
	ErrRunNotFound = errors.New("reconciliation: run not found")
)
