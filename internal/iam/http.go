package iam

import (
	"encoding/hex"
	"encoding/json"
	"net/http"
	"strings"
)

func (e *Engine) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	const prefix = "/_local/iam/decisions/"
	if r.Method != http.MethodGet || !strings.HasPrefix(r.URL.Path, prefix) {
		http.NotFound(w, r)
		return
	}
	id := strings.TrimPrefix(r.URL.Path, prefix)
	decoded, err := hex.DecodeString(id)
	if err != nil || len(decoded) != 16 {
		http.NotFound(w, r)
		return
	}
	decision, ok := e.Decision(id)
	if !ok {
		http.NotFound(w, r)
		return
	}
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	_ = json.NewEncoder(w).Encode(decision)
}
