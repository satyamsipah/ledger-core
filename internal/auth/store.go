package auth

import "context"

// Store is the persistence port for API keys.
type Store interface {
	// Authenticate resolves a raw presented key to the principal it names.
	// Returns ErrInvalidAPIKey for anything that does not authenticate --
	// unknown, revoked, or malformed -- with no distinction visible to the
	// caller between those cases.
	Authenticate(ctx context.Context, rawKey string) (principalID string, err error)

	// Issue mints a new key for a principal and returns the raw value.
	//
	// Deliberately the only write this port exposes. Revocation, listing, and
	// rotation are real admin-surface features this service does not have yet;
	// building them here to make this package feel complete would be Phase 6
	// work wearing a Phase 3 bugfix's clothes.
	Issue(ctx context.Context, principalID string) (rawKey string, err error)
}
