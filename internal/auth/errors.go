package auth

import "errors"

var (
	// ErrMissingAPIKey means the Authorization header was absent on a route
	// that requires one.
	ErrMissingAPIKey = errors.New("auth: the Authorization header is required")

	// ErrInvalidAPIKey means a key was presented and did not authenticate --
	// malformed, unknown, or revoked. Deliberately one error for all three: a
	// caller probing for which keys exist learns nothing from the difference
	// between "wrong shape" and "right shape, wrong secret", because there is
	// no difference in the response.
	ErrInvalidAPIKey = errors.New("auth: the presented API key did not authenticate")
)
