package idempotency

import (
	"crypto/sha256"
	"crypto/subtle"
	"fmt"

	"github.com/google/uuid"
)

// FingerprintLen is the SHA-256 digest length, mirroring
// idempotency_keys_fingerprint_len_check in migration 000007.
const FingerprintLen = sha256.Size

// Fingerprint is the digest of a canonicalized request.
type Fingerprint [FingerprintLen]byte

// Bytes returns the digest in the form the bytea column stores.
func (f Fingerprint) Bytes() []byte { return f[:] }

// Equal compares two fingerprints in constant time.
//
// Constant time because a fingerprint mismatch is the boundary between
// replaying somebody's stored response and refusing to, and an attacker who can
// measure how quickly the comparison fails learns how many leading bytes they
// guessed correctly. That is a slow attack and a cheap defence.
func (f Fingerprint) Equal(other Fingerprint) bool {
	return subtle.ConstantTimeCompare(f[:], other[:]) == 1
}

// FingerprintOf digests a request into the value stored against its key.
//
// # THE INPUTS, AND WHY EACH IS THERE
//
// The canonical body is the obvious one: it is what "the same request" means.
//
// The method and route pattern are there because a key namespace is global. A
// client that reuses one key across POST /v1/transactions and
// POST /v1/transactions/{id}/reverse would otherwise have the second request
// replay the first one's response -- a 201 describing a transaction that is not
// the one it just asked to reverse. Binding the endpoint into the digest turns
// that into a 422, which is a bug report rather than a wrong answer.
//
// The route PATTERN rather than the path, so that the two reversal requests
// /v1/transactions/A/reverse and /v1/transactions/B/reverse share a
// fingerprint input. That is correct: the transaction id is already part of the
// canonical body for endpoints that take one, and for those that do not, the id
// belongs to the resource rather than to the request's identity.
//
// NOT INCLUDED, AND THIS IS A KNOWN GAP: the authenticated principal. There is
// no authentication in the service yet, so there is no principal to bind. The
// consequence is that the key namespace is shared across every caller -- see
// docs/DECISIONS.md D23 for what that permits and what has to change when auth
// lands. It is recorded there rather than papered over with a placeholder,
// because a constant standing in for a principal is not a security boundary,
// it only looks like one.
func FingerprintOf(method, route string, body []byte) (Fingerprint, error) {
	canonical, err := Canonicalize(body)
	if err != nil {
		return Fingerprint{}, err
	}

	// Newline-separated with the length-free fields first: method and route are
	// both drawn from a fixed alphabet that excludes newlines, so no separator
	// injection is possible and no length prefix is needed to keep the
	// concatenation unambiguous.
	digest := sha256.New()
	digest.Write([]byte(method))
	digest.Write([]byte{'\n'})
	digest.Write([]byte(route))
	digest.Write([]byte{'\n'})
	digest.Write(canonical)

	var fingerprint Fingerprint
	copy(fingerprint[:], digest.Sum(nil))
	return fingerprint, nil
}

// FingerprintFromBytes rebuilds a fingerprint read back from the database.
func FingerprintFromBytes(raw []byte) (Fingerprint, error) {
	if len(raw) != FingerprintLen {
		return Fingerprint{}, fmt.Errorf("stored fingerprint is %d bytes, want %d: %w",
			len(raw), FingerprintLen, ErrCorruptRecord)
	}
	var fingerprint Fingerprint
	copy(fingerprint[:], raw)
	return fingerprint, nil
}

// ParseKey validates a client-supplied Idempotency-Key.
//
// A UUID is required rather than any opaque string, which is stricter than most
// APIs. The reason is collision, not tidiness: keys share one namespace, and a
// client that picks "order-1" as its key will collide with a different client
// that picked the same obvious value, and the second one will be handed the
// first one's response. Requiring a UUID makes an accidental collision
// impossible; it does not make a deliberate one impossible, which is what D23
// is about.
//
// The parsed value is normalised back to its canonical lower-case hyphenated
// form, so the same key in upper case, in braces, or as a bare 32 hex digits is
// one key rather than four.
func ParseKey(raw string) (string, error) {
	if raw == "" {
		return "", ErrMissingKey
	}
	parsed, err := uuid.Parse(raw)
	if err != nil {
		return "", fmt.Errorf("idempotency key %q: %w: %w", raw, err, ErrInvalidKey)
	}
	return parsed.String(), nil
}
