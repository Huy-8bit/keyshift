// Package fingerprint produces privacy-preserving identifiers for Redis keys.
//
// Fingerprints are truncated HMAC-SHA256 digests. With a shared, explicitly
// configured secret every SDK instance in the same administrative scope
// produces the same fingerprint for the same key, which lets the controller
// aggregate keys across pods without ever receiving raw key names. Without a
// configured secret each process generates a random secret, so fingerprints
// are only comparable within that process.
//
// HMAC is a one-way keyed function: fingerprints cannot be reversed without
// the secret. They can, however, be confirmed by anyone holding the secret
// who can guess candidate key names (a dictionary attack), so the secret must
// be protected like any other credential. See docs/security-model.md.
package fingerprint

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"hash"
	"sync"
)

// Size is the fingerprint length in bytes (hex-encoded length is 2*Size).
const Size = 16

// MinSecretBytes is the minimum accepted length for a configured secret.
const MinSecretBytes = 16

// Scope describes how widely fingerprints are comparable.
type Scope string

const (
	// ScopeShared means fingerprints are produced with a configured secret and
	// are comparable across all instances sharing that secret.
	ScopeShared Scope = "shared"
	// ScopeInstance means a random per-process secret is used.
	ScopeInstance Scope = "instance"
)

// Hasher computes key fingerprints. It is safe for concurrent use.
type Hasher struct {
	scope Scope
	keyID string
	pool  sync.Pool
}

// New returns a Hasher. If secret is empty a random per-process secret is
// generated and the scope is ScopeInstance.
func New(secret []byte) (*Hasher, error) {
	scope := ScopeShared
	if len(secret) == 0 {
		secret = make([]byte, 32)
		if _, err := rand.Read(secret); err != nil {
			return nil, err
		}
		scope = ScopeInstance
	} else if len(secret) < MinSecretBytes {
		return nil, errors.New("fingerprint: secret must be at least 16 bytes")
	}
	key := append([]byte(nil), secret...)
	// The key ID identifies which secret produced a fingerprint without
	// revealing it, so the controller can detect mismatched secrets.
	idMac := hmac.New(sha256.New, key)
	idMac.Write([]byte("keyshift/fingerprint-key-id/v1"))
	h := &Hasher{scope: scope, keyID: hex.EncodeToString(idMac.Sum(nil)[:4])}
	h.pool.New = func() any { return hmac.New(sha256.New, key) }
	return h, nil
}

// Scope reports how widely the fingerprints are comparable.
func (h *Hasher) Scope() Scope { return h.scope }

// KeyID returns a short, non-reversible identifier of the secret in use.
func (h *Hasher) KeyID() string { return h.keyID }

// Fingerprint returns the hex-encoded fingerprint of key.
func (h *Hasher) Fingerprint(key string) string {
	m := h.pool.Get().(hash.Hash)
	m.Reset()
	m.Write([]byte(key))
	var buf [sha256.Size]byte
	sum := m.Sum(buf[:0])
	h.pool.Put(m)
	return hex.EncodeToString(sum[:Size])
}

// Valid reports whether s is a syntactically valid fingerprint.
func Valid(s string) bool {
	if len(s) != 2*Size {
		return false
	}
	for i := 0; i < len(s); i++ {
		c := s[i]
		if !(c >= '0' && c <= '9' || c >= 'a' && c <= 'f') {
			return false
		}
	}
	return true
}
