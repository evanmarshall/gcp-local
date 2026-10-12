// Package iam evaluates the explicitly supported permission plans and policies.
package iam

import (
	"crypto/sha256"
	"embed"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"regexp"
	"slices"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

//go:embed roles
var roleFiles embed.FS

// Role snapshot sentinels.
var (
	errRoleChecksum = errors.New("predefined role snapshot checksum mismatch")
	errRoleSnapshot = errors.New("invalid predefined role snapshot")
)

type roleEntry struct {
	File   string `json:"file"`
	Name   string `json:"name"`
	SHA256 string `json:"sha256"`
}

// roleCatalog is everything the role snapshots pin: each predefined role's
// permission set and recorded definition, and the custom-role support level
// of every permission testable on a project.
type roleCatalog struct {
	permissions map[string]map[string]bool
	definitions map[string]json.RawMessage
	testable    map[string]string
}

func loadRoles() (roleCatalog, error) {
	var manifest struct {
		Roles       []roleEntry `json:"roles"`
		Permissions roleEntry   `json:"permissions"`
	}
	raw, err := roleFiles.ReadFile("roles/manifest.json")
	if err != nil {
		return roleCatalog{}, fmt.Errorf("read role manifest: %w", err)
	}
	if err = json.Unmarshal(raw, &manifest); err != nil {
		return roleCatalog{}, fmt.Errorf("decode role manifest: %w", err)
	}
	catalog := roleCatalog{permissions: map[string]map[string]bool{}, definitions: map[string]json.RawMessage{}}
	for _, entry := range manifest.Roles {
		raw, permissions, err := loadRole(entry)
		if err != nil {
			return roleCatalog{}, err
		}
		catalog.permissions[entry.Name] = permissions
		catalog.definitions[entry.Name] = raw
	}
	if catalog.testable, err = loadTestable(manifest.Permissions); err != nil {
		return roleCatalog{}, err
	}
	return catalog, nil
}

// readSnapshot reads one checksummed snapshot file.
func readSnapshot(entry roleEntry) ([]byte, error) {
	raw, err := roleFiles.ReadFile("roles/" + entry.File)
	if err != nil {
		return nil, fmt.Errorf("read role snapshot %s: %w", entry.File, err)
	}
	sum := sha256.Sum256(raw)
	if hex.EncodeToString(sum[:]) != entry.SHA256 {
		return nil, errRoleChecksum
	}
	return raw, nil
}

// loadRole reads one checksummed role snapshot and returns it with its
// permission set.
func loadRole(entry roleEntry) (json.RawMessage, map[string]bool, error) {
	raw, err := readSnapshot(entry)
	if err != nil {
		return nil, nil, err
	}
	var role struct {
		Name                string   `json:"name"`
		Stage               string   `json:"stage"`
		IncludedPermissions []string `json:"includedPermissions"`
	}
	if err := json.Unmarshal(raw, &role); err != nil {
		return nil, nil, fmt.Errorf("decode role snapshot %s: %w", entry.File, err)
	}
	// Predefined roles such as cloudtasks.enqueuer are still BETA in GCP;
	// deprecated, disabled, and alpha roles stay rejected.
	stable := role.Stage == "GA" || role.Stage == "BETA"
	if role.Name != entry.Name || !stable || len(role.IncludedPermissions) == 0 {
		return nil, nil, errRoleSnapshot
	}
	permissions := map[string]bool{}
	for _, permission := range role.IncludedPermissions {
		permissions[permission] = true
	}
	return raw, permissions, nil
}

// loadTestable reads the recorded queryTestablePermissions result for a
// project: each permission's custom-role support level.
func loadTestable(entry roleEntry) (map[string]string, error) {
	raw, err := readSnapshot(entry)
	if err != nil {
		return nil, err
	}
	var snapshot struct {
		Levels map[string][]string `json:"customRolesSupportLevel"`
	}
	if err := json.Unmarshal(raw, &snapshot); err != nil {
		return nil, fmt.Errorf("decode testable permissions: %w", err)
	}
	testable := map[string]string{}
	for level, permissions := range snapshot.Levels {
		for _, permission := range permissions {
			testable[permission] = level
		}
	}
	if len(snapshot.Levels[supportLevelSupported]) == 0 {
		return nil, errRoleSnapshot
	}
	return testable, nil
}

// Custom-role support levels of queryTestablePermissions.
const (
	supportLevelSupported    = "SUPPORTED"
	supportLevelNotSupported = "NOT_SUPPORTED"
)

// PredefinedRole returns the recorded definition of a pinned predefined role,
// as GCP's roles.get returned it.
func (e *Engine) PredefinedRole(name string) (json.RawMessage, bool) {
	raw, ok := e.definitions[name]
	return raw, ok
}

// PredefinedRoleNames lists the pinned predefined roles in name order.
func (e *Engine) PredefinedRoleNames() []string {
	names := make([]string, 0, len(e.definitions))
	for name := range e.definitions {
		names = append(names, name)
	}
	slices.Sort(names)
	return names
}

// CheckCustomRolePermission validates one includedPermissions entry with
// GCP's errors: a permission a project can't test is "not valid", and one
// GCP excludes from custom roles is "not supported". Validity is known only
// for project-level roles; an organization role naming a permission outside
// the project catalog is refused as a coverage gap.
func (e *Engine) CheckCustomRolePermission(permission string, organization bool) error {
	level, ok := e.testable[permission]
	switch {
	case !ok && organization:
		return status.Errorf(codes.Unimplemented, "Permission %s is not testable on a project; organization-level custom role support is not recorded", permission)
	case !ok:
		return status.Errorf(codes.InvalidArgument, "Permission %s is not valid.", permission)
	case level == supportLevelNotSupported:
		return status.Errorf(codes.InvalidArgument, "Permission %s is not supported in custom roles.", permission)
	}
	return nil
}

// CustomRole is the part of a custom role the engine evaluates: its
// permissions and whether it is deleted. A deleted role grants nothing.
type CustomRole struct {
	Permissions map[string]bool
	Deleted     bool
}

// CustomRoleLookup finds a custom role by its full name. It must not call
// back into the engine: the engine calls it while holding its own lock.
type CustomRoleLookup func(name string) (CustomRole, bool)

// UseCustomRoles installs the custom-role owner's lookup. Without one, no
// custom role exists.
func (e *Engine) UseCustomRoles(lookup CustomRoleLookup) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.customRoles = lookup
}

// customRoleName matches projects/P/roles/R and organizations/O/roles/R.
var customRoleName = regexp.MustCompile(`^((?:projects|organizations)/[^/]+)/roles/[a-zA-Z0-9_.]{3,64}$`)

// rolePermission reports whether a role grants a permission. It runs under
// e.mu. A custom role grants only while it exists and isn't deleted.
func (e *Engine) rolePermission(role, permission string) bool {
	if permissions, ok := e.roles[role]; ok {
		return permissions[permission]
	}
	if e.customRoles == nil || !customRoleName.MatchString(role) {
		return false
	}
	custom, found := e.customRoles(role)
	return found && !custom.Deleted && custom.Permissions[permission]
}

// checkRole validates one binding's role with GCP's setIamPolicy errors. A
// custom role must exist in the resource's own hierarchy, and a deleted one
// takes no new members; members it already had are kept, so a policy read
// back and written unchanged still succeeds. A predefined role outside the
// pinned snapshots is a coverage gap.
func (e *Engine) checkRole(binding Binding, roles roleContext) error {
	if strings.HasPrefix(binding.Role, "roles/") {
		if _, ok := e.roles[binding.Role]; !ok {
			return status.Error(codes.Unimplemented, "Role is outside the pinned supported role set")
		}
		return nil
	}
	match := customRoleName.FindStringSubmatch(binding.Role)
	if match == nil {
		return status.Error(codes.InvalidArgument, `The role name must be in the form "roles/{role}", "organizations/{organization_id}/roles/{role}", or "projects/{project_id}/roles/{role}".`)
	}
	// GCP answers a role in a project that doesn't exist as if the caller
	// lacked access; an unknown organization is only "does not exist".
	if strings.HasPrefix(match[1], "projects/") {
		if _, known := e.index.Get("//cloudresourcemanager.googleapis.com/" + match[1]); !known {
			return status.Error(codes.PermissionDenied, "Permission denied on resource (or it may not exist).")
		}
	}
	custom, ok := e.customRoleInHierarchy(binding.Role, "//cloudresourcemanager.googleapis.com/"+match[1], roles)
	if !ok {
		return status.Errorf(codes.InvalidArgument, "Role (%s) does not exist in the resource's hierarchy.", binding.Role)
	}
	if custom.Deleted && slices.ContainsFunc(binding.Members, func(member string) bool { return !roles.previous.grants(binding.Role, member) }) {
		return status.Errorf(codes.InvalidArgument, "Cannot add new grants to deleted role (%s).", binding.Role)
	}
	return nil
}

// customRoleInHierarchy looks up a custom role and reports whether it exists
// and its parent node is an ancestor of the policy's resource.
func (e *Engine) customRoleInHierarchy(role, parent string, roles roleContext) (CustomRole, bool) {
	if roles.lookup == nil {
		return CustomRole{}, false
	}
	custom, found := roles.lookup(role)
	if !found {
		return CustomRole{}, false
	}
	ancestors, err := e.ancestry(Target{Name: roles.resource})
	return custom, err == nil && slices.ContainsFunc(ancestors, func(ancestor state.Resource) bool { return ancestor.Name == parent })
}

// grants reports whether a policy already binds a member to a role.
func (p Policy) grants(role, member string) bool {
	for _, binding := range p.Bindings {
		if binding.Role == role && slices.Contains(binding.Members, member) {
			return true
		}
	}
	return false
}
