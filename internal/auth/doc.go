// Package auth authenticates API callers and names the principal they act as.
//
// It exists to close docs/DECISIONS.md D24, not to be a full auth product:
// there is no expiry, no scope, and no admin surface for rotation or listing
// here, because building those is real work that belongs with the admin
// dashboard when it exists, not folded into closing a namespace-collision bug.
// What this package provides is the one thing that bug needed and did not
// have -- a principal a caller cannot merely assert, because possessing it
// requires a secret this service issued and never stored.
package auth
