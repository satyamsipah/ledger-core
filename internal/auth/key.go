package auth

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
)

// keyBytes is the raw secret's length before encoding. 32 bytes is 256 bits of
// entropy -- far more than a UUID's 122, because this credential does not
// merely need to avoid an accidental collision, it needs to resist a
// deliberate attacker searching the whole keyspace.
const keyBytes = 32

// KeyPrefix marks a value as one of this service's keys, in the shape callers
// see from Stripe, GitHub and most other API-key issuers. It exists so a key
// accidentally pasted into a log line, a support ticket or a public repository
// is recognisable at a glance -- both by an operator triaging a leak and by a
// secret scanner looking for one.
const KeyPrefix = "lk_live_"

// GenerateKey issues a new raw key and the hash that authenticates it.
//
// The raw value is returned exactly once, here, and is never reconstructible
// from what gets stored -- Hash is a one-way SHA-256 digest, matching the
// idempotency fingerprint's own reasoning: a database that is read (a backup,
// a replica, a careless SELECT *) must not hand out a working credential.
func GenerateKey() (raw string, hash [sha256.Size]byte, err error) {
	secret := make([]byte, keyBytes)
	if _, err := rand.Read(secret); err != nil {
		return "", hash, fmt.Errorf("generate api key: %w", err)
	}

	raw = KeyPrefix + hex.EncodeToString(secret)
	hash = sha256.Sum256([]byte(raw))
	return raw, hash, nil
}

// HashKey digests a presented key for lookup. Exported so the HTTP middleware
// and any future admin tooling compute the hash identically to how it was
// issued, rather than each reimplementing crypto/sha256.Sum256.
func HashKey(raw string) [sha256.Size]byte {
	return sha256.Sum256([]byte(raw))
}
