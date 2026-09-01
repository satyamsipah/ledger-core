package outbox

import "errors"

// ErrMissingOccurredAt means Append was called with a zero OccurredAt.
//
// A zero time.Time silently marshals to "0001-01-01T00:00:00Z" rather than
// failing, which is exactly the kind of wrong-but-plausible value that survives
// code review and is discovered by a confused consumer months later. Rejected
// outright instead: every event describes something that happened at a real,
// database-generated instant, and a caller that has not looked that instant up
// has not finished building the event.
var ErrMissingOccurredAt = errors.New("outbox: event occurred_at is required")
