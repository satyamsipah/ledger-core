// Package clock makes this codebase's few genuine dependencies on a
// process's own wall clock injectable, for exactly one purpose: a
// fault-injection harness needs to simulate clock skew, and every other
// candidate for "what should the skew fault actually skew" turned out, on
// inspection, to already be deliberately immune to it.
//
// Almost every timing decision in this codebase is computed by PostgreSQL's
// own now() -- saga step deadlines after the first (docs/DECISIONS.md's own
// Transition.StepTimeout comment explains why: "deadlines computed from a Go
// process's wall clock and compared against the database's would drift"),
// idempotency lease expiry, reconciliation timestamps. Skewing a Go
// process's clock would affect none of them, because none of them read it.
//
// Two places genuinely do read a Go process's wall clock for a decision that
// matters, and both compare it against a value POSTGRES computed with ITS
// OWN clock -- exactly the cross-clock comparison a real skew would corrupt:
//
//   - internal/idempotency.Manager.resolveExisting compares time.Now()
//     against idempotency_keys.expires_at (server-computed) to decide
//     whether a replay record has aged out.
//   - internal/saga/payout.Orchestrator.Start computes a brand new saga's
//     FIRST step_deadline_at as time.Now().Add(stepTimeout) -- every
//     SUBSEQUENT deadline is server-computed (D-notes above), but this one,
//     the very first, is not.
//
// Both now call clock.Now() instead of time.Now(). Everywhere else in this
// codebase keeps calling time.Now() directly and is correctly unaffected by
// this package -- a metrics timer or a log timestamp skewing along with an
// injected fault would be noise, not signal.
package clock

import (
	"sync/atomic"
	"time"
)

// offsetNanos is read on every call, so it must be an atomic rather than a
// plain field: the fault-injection HTTP handler that sets it runs on a
// different goroutine than every request calling Now().
var offsetNanos atomic.Int64

// Now returns the real wall clock time, shifted by whatever offset
// SetOffset last set. Zero offset -- the default, and the only value any
// process not deliberately running under fault injection will ever see --
// makes this identical to time.Now().
func Now() time.Time {
	return time.Now().Add(time.Duration(offsetNanos.Load()))
}

// SetOffset sets the process-wide skew. Idempotent and safe to call
// concurrently with Now(); the new offset applies to the very next call.
func SetOffset(d time.Duration) {
	offsetNanos.Store(int64(d))
}

// Offset returns the currently configured skew, for a status endpoint or a
// test assertion to read back what is actually in effect.
func Offset() time.Duration {
	return time.Duration(offsetNanos.Load())
}
