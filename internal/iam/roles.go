// Package iam evaluates the explicitly supported permission plans and policies.
package iam

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
)

//go:embed roles/*.json
var roleFiles embed.FS

// Role snapshot sentinels.
var (
	errRoleChecksum = errors.New("predefined role snapshot checksum mismatch")
	errRoleSnapshot = errors.New("invalid predefined role snapshot")
)

func loadRoles() (map[string]map[string]bool, error) {
	var manifest struct {
		Roles []struct{ File, Name, SHA256 string }
	}
	raw, err := roleFiles.ReadFile("roles/manifest.json")
	if err != nil {
		return nil, fmt.Errorf("read role manifest: %w", err)
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return nil, fmt.Errorf("decode role manifest: %w", err)
	}
	roles := map[string]map[string]bool{}
	for _, entry := range manifest.Roles {
		raw, err := roleFiles.ReadFile("roles/" + entry.File)
		if err != nil {
			return nil, fmt.Errorf("read role snapshot %s: %w", entry.File, err)
		}
		sum := sha256.Sum256(raw)
		if hex.EncodeToString(sum[:]) != entry.SHA256 {
			return nil, errRoleChecksum
		}
		var role struct {
			Name, Stage         string
			IncludedPermissions []string
		}
		if err := json.Unmarshal(raw, &role); err != nil {
			return nil, fmt.Errorf("decode role snapshot %s: %w", entry.File, err)
		}
		if role.Name != entry.Name || role.Stage != "GA" || len(role.IncludedPermissions) == 0 {
			return nil, errRoleSnapshot
		}
		permissions := map[string]bool{}
		for _, permission := range role.IncludedPermissions {
			permissions[permission] = true
		}
		roles[role.Name] = permissions
	}
	return roles, nil
}
