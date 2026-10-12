package iam

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// signingKey is the public half of one service-account key. The provider that
// created the key owns its private half and its lifecycle; the engine only
// verifies, so a service such as Storage can check a signature without
// depending on that provider.
type signingKey struct {
	public *rsa.PublicKey
	owner  string
	scope  state.Scope
}

// PublishSigningKey records the public key a service account signs with.
// ClearScope with the same owner and scope removes it.
func (e *Engine) PublishSigningKey(owner string, scope state.Scope, account, keyID string, public *rsa.PublicKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.signingKeys[account] == nil {
		e.signingKeys[account] = map[string]signingKey{}
	}
	e.signingKeys[account][keyID] = signingKey{public: public, owner: owner, scope: scope}
}

// ForgetSigningKeys removes one key of an account, or every key when keyID is empty.
func (e *Engine) ForgetSigningKeys(account, keyID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if keyID == "" {
		delete(e.signingKeys, account)
		return
	}
	delete(e.signingKeys[account], keyID)
}

// VerifySignature reports whether one of the account's published keys made an
// RSASSA-PKCS1-v1_5 SHA-256 signature over message, and the account's scope.
func (e *Engine) VerifySignature(account string, message, signature []byte) (state.Scope, bool) {
	digest := sha256.Sum256(message)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, key := range e.signingKeys[account] {
		if rsa.VerifyPKCS1v15(key.public, crypto.SHA256, digest[:], signature) == nil {
			return key.scope, true
		}
	}
	return state.Scope{}, false
}

// clearSigningKeys drops the keys one owner published in one scope.
func (e *Engine) clearSigningKeys(owner string, scope state.Scope) {
	for account, keys := range e.signingKeys {
		for id, key := range keys {
			if key.owner == owner && key.scope == scope {
				delete(keys, id)
			}
		}
		if len(keys) == 0 {
			delete(e.signingKeys, account)
		}
	}
}
