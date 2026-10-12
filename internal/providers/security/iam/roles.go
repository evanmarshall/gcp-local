package iam

import (
	"cmp"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Predefined-role methods: GCP requires no permission to read them.
const (
	methodRolesGet  = "iam.roles.get"
	methodRolesList = "iam.roles.list"
)

// Method verbs and Role fields the handlers branch on.
const (
	verbGet          = "get"
	verbList         = "list"
	verbPatch        = "patch"
	verbDelete       = "delete"
	fieldTitle       = "title"
	fieldDescription = "description"
	fieldETag        = "etag"
)

// roleVerbs maps each custom-role method verb to the permission it needs.
var roleVerbs = map[string]string{
	"create": "iam.roles.create", verbList: "iam.roles.list", verbGet: "iam.roles.get",
	verbPatch: "iam.roles.update", verbDelete: "iam.roles.delete", "undelete": "iam.roles.undelete",
}

// roleMaskFields are the Role fields patch can update; no mask means all.
var roleMaskFields = []string{fieldTitle, fieldDescription, "includedPermissions", "stage"}

// Parent kinds of custom roles, as they appear in role names and method IDs.
const (
	parentProjects      = "projects"
	parentOrganizations = "organizations"
)

// Deletion timeline (https://cloud.google.com/iam/docs/creating-custom-roles#deleting-custom-role):
// a deleted role can be undeleted for 7 days; then it is permanently deleted
// and its ID stays unavailable until up to 44 days after the delete request.
const (
	undeleteWindow = 7 * 24 * time.Hour
	idReservation  = 44 * 24 * time.Hour
)

// roleID is GCP's role_id pattern.
var roleID = regexp.MustCompile(`^[a-zA-Z0-9_.]{3,64}$`)

// roleStages are the RoleLaunchStage enum names; ALPHA is the proto default.
var roleStages = map[string]bool{"ALPHA": true, "BETA": true, "GA": true, "DEPRECATED": true, "DISABLED": true, "EAP": true}

// Request limits from the custom-role documentation.
const (
	maxRolePermissions = 3000
	defaultRolePage    = 300
	maxRolePage        = 1000
)

// orgLocation keys an organization's roles in the role store; project roles
// use the project's global scope.
const orgLocation = "organization"

// roleRecord is one stored custom role. permissions is replaced, never
// mutated, so the engine's lookup can share it.
type roleRecord struct {
	title, description, stage, etag string
	permissions                     map[string]bool
	deletedAt                       time.Time
}

// roleScope holds one project's or organization's custom roles by ID.
type roleScope struct {
	roles map[string]*roleRecord
}

func newRoleScope() roleScope { return roleScope{roles: map[string]*roleRecord{}} }

// roles owns custom roles and serves the predefined-role snapshots. Its store
// callbacks never call the engine: the engine reads roles through lookup
// while holding its own lock.
type roles struct {
	engine   *iam.Engine
	store    *state.Store[roleScope]
	clock    func() time.Time
	revision atomic.Uint64
}

// registerRoles registers the role methods and installs the engine lookup.
func registerRoles(registry *gateway.Registry, engine *iam.Engine, clock func() time.Time) (*roles, error) {
	r := &roles{engine: engine, clock: clock, store: state.NewStore(newRoleScope, func(scope state.Scope) error {
		if scope.Location != state.Global && scope.Location != orgLocation {
			return errRoleScope
		}
		return nil
	})}
	for _, parent := range []string{parentProjects, parentOrganizations} {
		for verb, permission := range roleVerbs {
			method := "iam." + parent + ".roles." + verb
			if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
				return nil, fmt.Errorf("register %s: %w", method, err)
			}
			if err := registry.Register(method, r.handle); err != nil {
				return nil, fmt.Errorf("register %s: %w", method, err)
			}
		}
	}
	for _, method := range []string{methodRolesGet, methodRolesList} {
		if err := registry.Register(method, r.handle); err != nil {
			return nil, fmt.Errorf("register %s: %w", method, err)
		}
	}
	engine.UseCustomRoles(r.lookup)
	return r, nil
}

// errRoleScope rejects a role store scope that names neither a project nor an
// organization.
var errRoleScope = errors.New("role scope must be a project or an organization")

// roleCall is one parsed role request.
type roleCall struct {
	verb, parentKind, parentID, id string
}

// parent is the role parent's relative name (projects/P).
func (c roleCall) parent() string { return c.parentKind + "/" + c.parentID }

// name is the role's full name.
func (c roleCall) name() string { return c.parent() + "/roles/" + c.id }

// scope keys the parent in the role store.
func (c roleCall) scope() state.Scope {
	if c.parentKind == parentOrganizations {
		return state.Scope{Project: c.parentID, Location: orgLocation}
	}
	return provider.ProjectScope(c.parentID)
}

// parseRoleCall reads the method's parent and role ID. iam.roles.list with
// a parent query parameter lists that parent's custom roles.
func parseRoleCall(method string, request *gateway.RESTRequest) (roleCall, bool, error) {
	if method == methodRolesList {
		parent := request.Query.Get("parent")
		if parent == "" {
			return roleCall{verb: verbList}, true, nil
		}
		kind, id, ok := strings.Cut(parent, "/")
		if (kind != parentProjects && kind != parentOrganizations) || !ok || id == "" || strings.Contains(id, "/") {
			return roleCall{}, false, status.Errorf(codes.InvalidArgument, "The parent %q is invalid.", parent)
		}
		return roleCall{verb: verbList, parentKind: kind, parentID: id}, false, nil
	}
	if method == methodRolesGet {
		return roleCall{verb: verbGet, id: request.Path["role"]}, true, nil
	}
	parts := strings.Split(method, ".")
	call := roleCall{verb: parts[3], parentKind: parts[1], id: request.Path["role"]}
	call.parentID = request.Path["project"]
	if call.parentKind == parentOrganizations {
		call.parentID = request.Path["organization"]
	}
	return call, false, nil
}

// handle is the gateway entry point for every role method. Custom-role
// methods authorize against the parent before reading or changing state.
func (r *roles) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid IAM request")
	}
	parsed, predefined, err := parseRoleCall(call.Method, request)
	if err != nil {
		return nil, err
	}
	if err := checkRoleQuery(parsed.verb, request); err != nil {
		return nil, err
	}
	if predefined {
		// Predefined role definitions need no permission, only a caller.
		if call.Principal.State != gateway.Authenticated {
			return provider.WireError(http.StatusUnauthorized, "UNAUTHENTICATED", "Request is missing required authentication credential."), nil
		}
		return r.predefined(parsed, request)
	}
	target := iam.Target{Name: "//cloudresourcemanager.googleapis.com/" + parsed.parent()}
	call.Method = "iam." + parsed.parentKind + ".roles." + parsed.verb
	decision, authorizeErr := r.engine.Authorize(call, target, nil)
	if authorizeErr != nil {
		if provider.Unverified(authorizeErr) {
			return nil, fmt.Errorf("authorize %s: %w", call.Method, authorizeErr)
		}
		return provider.Denied(decision, target.Name), nil
	}
	response, err := r.execute(parsed, request)
	if err != nil {
		return nil, err
	}
	response.Headers = http.Header{"X-Gcp-Local-Decision-Id": {decision.ID}}
	return response, nil
}

// roleQuery lists the query parameters each verb accepts; alt and
// prettyPrint are system parameters every method takes.
var roleQuery = map[string][]string{
	verbList: {"view", "showDeleted", "pageSize", "pageToken", "parent"}, verbPatch: {"updateMask"}, verbDelete: {fieldETag},
}

// checkRoleQuery refuses a query parameter the method doesn't define, as GCP
// does (roles.get has no view parameter).
func checkRoleQuery(verb string, request *gateway.RESTRequest) error {
	for name := range request.Query {
		if name == "alt" || name == "prettyPrint" || slices.Contains(roleQuery[verb], name) {
			continue
		}
		return status.Errorf(codes.InvalidArgument, "Invalid JSON payload received. Unknown name %q: Cannot bind query parameter. Field '%s' could not be found in request message.", name, name)
	}
	return nil
}

// lookup is the engine's view of one custom role. A role past its undelete
// window no longer exists for evaluation or new bindings.
func (r *roles) lookup(name string) (iam.CustomRole, bool) {
	parts := strings.Split(name, "/")
	if len(parts) != 4 || parts[2] != "roles" {
		return iam.CustomRole{}, false
	}
	parsed := roleCall{parentKind: parts[0], parentID: parts[1], id: parts[3]}
	now := r.clock()
	var role iam.CustomRole
	found := false
	_ = r.store.With(parsed.scope(), func(s *roleScope) error {
		if record := s.roles[parsed.id]; record != nil && record.visible(now) {
			role, found = iam.CustomRole{Permissions: record.permissions, Deleted: !record.deletedAt.IsZero()}, true
		}
		return nil
	})
	return role, found
}

// visible reports whether a role still exists: live, or deleted within its
// undelete window.
func (record *roleRecord) visible(now time.Time) bool {
	return record.deletedAt.IsZero() || now.Sub(record.deletedAt) < undeleteWindow
}

// reserved reports whether a role's ID is still unavailable for a new role.
func (record *roleRecord) reserved(now time.Time) bool {
	return record.deletedAt.IsZero() || now.Sub(record.deletedAt) < idReservation
}

// nextETag returns a fresh etag in GCP's eight-byte form.
func (r *roles) nextETag() string {
	raw := make([]byte, 8)
	binary.BigEndian.PutUint64(raw, r.revision.Add(1))
	return base64.StdEncoding.EncodeToString(raw)
}

// reset clears a project's custom roles after a backend reset and removes
// their bindings, so a role recreated with the same ID starts with none.
func (r *roles) reset(scope state.Scope) error {
	var names []string
	if err := r.store.With(scope, func(s *roleScope) error {
		for id := range s.roles {
			names = append(names, roleCall{parentKind: parentProjects, parentID: scope.Project, id: id}.name())
		}
		s.roles = map[string]*roleRecord{}
		return nil
	}); err != nil {
		return fmt.Errorf("clear the role scope: %w", err)
	}
	for _, name := range names {
		r.engine.RetireRole(name)
	}
	return nil
}

// roleBody is the Role message fields a request sets.
type roleBody struct {
	Title               string   `json:"title"`
	Description         string   `json:"description"`
	IncludedPermissions []string `json:"includedPermissions"`
	Stage               string   `json:"stage"`
	ETag                string   `json:"etag"`
}

// execute runs one authorized custom-role request.
func (r *roles) execute(call roleCall, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	switch call.verb {
	case "create":
		return r.create(call, request)
	case verbList:
		return r.list(call, request)
	case verbGet:
		return r.get(call)
	case verbPatch:
		return r.patch(call, request)
	case verbDelete:
		return r.remove(call, request.Query.Get(fieldETag))
	default:
		var body struct {
			ETag string `json:"etag"`
		}
		if json.Unmarshal(request.JSON, &body) != nil {
			return nil, provider.Invalid("Invalid JSON payload received.")
		}
		return r.undelete(call, body.ETag)
	}
}

// validate checks a role body's stage and permissions and returns its
// permission set.
func (r *roles) validate(call roleCall, body roleBody) (map[string]bool, error) {
	if body.Stage != "" && !roleStages[body.Stage] {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid value at 'role.stage' (type.googleapis.com/google.iam.admin.v1.Role.RoleLaunchStage), %q", body.Stage)
	}
	permissions := map[string]bool{}
	for _, permission := range body.IncludedPermissions {
		if err := r.engine.CheckCustomRolePermission(permission, call.parentKind == parentOrganizations); err != nil {
			return nil, err //nolint:wrapcheck // the engine's wire status
		}
		permissions[permission] = true
	}
	if len(permissions) > maxRolePermissions {
		return nil, provider.Invalid("A custom role can contain at most 3000 permissions.")
	}
	return permissions, nil
}

// create validates and stores one custom role. A deleted role's ID stays
// reserved; once the reservation ends, the old role's bindings are removed
// before the ID is reused.
func (r *roles) create(call roleCall, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body struct {
		RoleID string   `json:"roleId"`
		Role   roleBody `json:"role"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid JSON payload received.")
	}
	if !roleID.MatchString(body.RoleID) {
		return nil, status.Errorf(codes.InvalidArgument, `The role_id %q is invalid. It doesn't match pattern "[a-zA-Z0-9_\.]{3,64}". The role_id must be 3 to 64 characters long and can be a mix of uppercase and lowercase English letters, digits, underscores and periods.`, body.RoleID)
	}
	call.id = body.RoleID
	permissions, err := r.validate(call, body.Role)
	if err != nil {
		return nil, err
	}
	// The engine can't be called under the store lock, so an expired role's
	// bindings are retired between two store callbacks.
	expired := false
	_ = r.store.With(call.scope(), func(s *roleScope) error {
		existing := s.roles[call.id]
		expired = existing != nil && !existing.reserved(r.clock())
		return nil
	})
	if expired {
		r.engine.RetireRole(call.name())
	}
	var response map[string]any
	err = r.store.With(call.scope(), func(s *roleScope) error {
		if existing := s.roles[call.id]; existing != nil && existing.reserved(r.clock()) {
			if existing.deletedAt.IsZero() {
				return status.Errorf(codes.AlreadyExists, "A role named %s in %s already exists.", call.id, call.parent())
			}
			return status.Errorf(codes.FailedPrecondition, "You can't create a role with role_id (%s) where there is an existing role with that role_id in a deleted state.", call.id)
		}
		record := &roleRecord{title: body.Role.Title, description: body.Role.Description, stage: body.Role.Stage,
			permissions: permissions, etag: r.nextETag()}
		s.roles[call.id] = record
		response = roleWire(call.name(), record, true)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// withRole runs use on an existing (visible) role under the store lock.
func (r *roles) withRole(call roleCall, use func(*roleScope, *roleRecord) error) error {
	return r.store.With(call.scope(), func(s *roleScope) error { //nolint:wrapcheck // wire statuses
		record := s.roles[call.id]
		if record == nil || !record.visible(r.clock()) {
			return status.Errorf(codes.NotFound, "The role named %s was not found.", call.name())
		}
		return use(s, record)
	})
}

// get reads one custom role; a deleted role reads back with deleted: true.
func (r *roles) get(call roleCall) (*gateway.RESTResponse, error) {
	var response map[string]any
	err := r.withRole(call, func(_ *roleScope, record *roleRecord) error {
		response = roleWire(call.name(), record, true)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// patch applies the update mask (every field without one, as GCP does) after
// the etag check. A deleted role can't be updated.
func (r *roles) patch(call roleCall, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body roleBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid JSON payload received.")
	}
	mask := roleMaskFields
	if raw := request.Query.Get("updateMask"); raw != "" {
		mask = strings.Split(raw, ",")
	}
	for _, field := range mask {
		if !slices.Contains(roleMaskFields, field) {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid update_mask path: %s", field)
		}
	}
	permissions, err := r.validate(call, body)
	if err != nil {
		return nil, err
	}
	var response map[string]any
	err = r.withRole(call, func(_ *roleScope, record *roleRecord) error {
		if !record.deletedAt.IsZero() {
			return status.Errorf(codes.FailedPrecondition, "You can't update a role (%s) which is in a deleted state.", call.name())
		}
		if body.ETag != "" && body.ETag != record.etag {
			return errRoleETag
		}
		*record = masked(*record, mask, body, permissions)
		record.etag = r.nextETag()
		response = roleWire(call.name(), record, true)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// masked returns the role with the masked fields taken from the request.
func masked(record roleRecord, mask []string, body roleBody, permissions map[string]bool) roleRecord {
	for _, field := range mask {
		switch field {
		case fieldTitle:
			record.title = body.Title
		case fieldDescription:
			record.description = body.Description
		case "includedPermissions":
			record.permissions = permissions
		default:
			record.stage = body.Stage
		}
	}
	return record
}

// errRoleETag is GCP's stale-etag error for role writes.
var errRoleETag = status.Error(codes.Aborted, "There were concurrent policy changes. Please retry the whole read-modify-write with exponential backoff.")

// remove soft-deletes a role: it stops granting at once and stays readable
// and undeletable for seven days.
func (r *roles) remove(call roleCall, etag string) (*gateway.RESTResponse, error) {
	var response map[string]any
	err := r.withRole(call, func(_ *roleScope, record *roleRecord) error {
		if !record.deletedAt.IsZero() {
			return status.Errorf(codes.FailedPrecondition, "You can't delete role_id (%s) as it is already deleted.", call.name())
		}
		if etag != "" && etag != record.etag {
			return errRoleETag
		}
		record.deletedAt, record.etag = r.clock(), r.nextETag()
		response = roleWire(call.name(), record, true)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// undelete restores a role deleted within the last seven days.
func (r *roles) undelete(call roleCall, etag string) (*gateway.RESTResponse, error) {
	var response map[string]any
	err := r.withRole(call, func(_ *roleScope, record *roleRecord) error {
		if record.deletedAt.IsZero() {
			return status.Error(codes.FailedPrecondition, "A role that is not deleted cannot be undeleted.")
		}
		if etag != "" && etag != record.etag {
			return errRoleETag
		}
		record.deletedAt, record.etag = time.Time{}, r.nextETag()
		response = roleWire(call.name(), record, true)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// listQuery is a list request's view, showDeleted, and paging.
type listQuery struct {
	full, showDeleted bool
	size              int
	token             string
}

// listOptions reads view, showDeleted, and paging.
func listOptions(request *gateway.RESTRequest) (listQuery, error) {
	query := listQuery{showDeleted: request.Query.Get("showDeleted") == "true", size: defaultRolePage}
	switch view := request.Query.Get("view"); view {
	case "", "BASIC":
	case "FULL":
		query.full = true
	default:
		return listQuery{}, status.Errorf(codes.InvalidArgument, "Invalid value at 'view' (type.googleapis.com/google.iam.admin.v1.RoleView), %q", view)
	}
	if raw := request.Query.Get("pageSize"); raw != "" {
		size, err := strconv.Atoi(raw)
		if err != nil || size < 0 {
			return listQuery{}, provider.Invalid("Invalid pageSize.")
		}
		query.size = min(cmp.Or(size, defaultRolePage), maxRolePage)
	}
	if raw := request.Query.Get("pageToken"); raw != "" {
		decoded, err := base64.RawURLEncoding.DecodeString(raw)
		if err != nil {
			return listQuery{}, provider.Invalid("Invalid pageToken.")
		}
		query.token = string(decoded)
	}
	return query, nil
}

// page renders one page of name-ordered roles after the token's name.
func page(names []string, query listQuery, render func(string) map[string]any) map[string]any {
	slices.Sort(names)
	start, _ := slices.BinarySearch(names, query.token)
	if query.token != "" && start < len(names) && names[start] == query.token {
		start++
	}
	end := min(start+query.size, len(names))
	items := make([]map[string]any, 0, end-start)
	for _, name := range names[start:end] {
		items = append(items, render(name))
	}
	response := map[string]any{}
	if len(items) > 0 {
		response["roles"] = items
	}
	if end < len(names) {
		response["nextPageToken"] = base64.RawURLEncoding.EncodeToString([]byte(names[end-1]))
	}
	return response
}

// list returns a parent's roles in name order. Deleted roles appear only with
// showDeleted; the BASIC view omits permissions.
func (r *roles) list(call roleCall, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	query, err := listOptions(request)
	if err != nil {
		return nil, err
	}
	var response map[string]any
	_ = r.store.With(call.scope(), func(s *roleScope) error {
		now := r.clock()
		byName := map[string]*roleRecord{}
		for id, record := range s.roles {
			if record.visible(now) && (query.showDeleted || record.deletedAt.IsZero()) {
				byName[roleCall{parentKind: call.parentKind, parentID: call.parentID, id: id}.name()] = record
			}
		}
		response = page(slices.Collect(maps.Keys(byName)), query, func(name string) map[string]any {
			return roleWire(name, byName[name], query.full)
		})
		return nil
	})
	return &gateway.RESTResponse{JSON: response}, nil
}

// roleWire renders a custom role as GCP does: an ALPHA stage (the proto
// default) and an empty description or permission list are omitted.
func roleWire(name string, record *roleRecord, full bool) map[string]any {
	wire := map[string]any{"name": name, fieldTitle: record.title, fieldETag: record.etag}
	if record.description != "" {
		wire[fieldDescription] = record.description
	}
	if full && len(record.permissions) > 0 {
		wire["includedPermissions"] = slices.Sorted(maps.Keys(record.permissions))
	}
	if record.stage != "" && record.stage != "ALPHA" {
		wire["stage"] = record.stage
	}
	if !record.deletedAt.IsZero() {
		wire["deleted"] = true
	}
	return wire
}

// predefined serves the pinned predefined-role snapshots: get returns the
// recorded definition, and list pages through them in name order.
func (r *roles) predefined(call roleCall, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if call.verb == verbGet {
		raw, ok := r.engine.PredefinedRole("roles/" + call.id)
		if !ok {
			return nil, status.Errorf(codes.Unimplemented, "Predefined role roles/%s has no pinned snapshot", call.id)
		}
		var definition map[string]any
		if err := json.Unmarshal(raw, &definition); err != nil {
			return nil, status.Error(codes.Internal, "Invalid predefined role snapshot")
		}
		return &gateway.RESTResponse{JSON: definition}, nil
	}
	query, err := listOptions(request)
	if err != nil {
		return nil, err
	}
	response := page(r.engine.PredefinedRoleNames(), query, func(name string) map[string]any {
		raw, _ := r.engine.PredefinedRole(name)
		var definition map[string]any
		_ = json.Unmarshal(raw, &definition)
		if !query.full {
			delete(definition, "includedPermissions")
		}
		return definition
	})
	return &gateway.RESTResponse{JSON: response}, nil
}
