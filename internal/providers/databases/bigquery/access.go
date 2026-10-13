package bigquery

import (
	"cmp"
	"encoding/json"
	"regexp"
	"slices"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/iam"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// A dataset's access list is its IAM policy: GCP stores member entries as
// bindings ("IAM setPolicy failed for Dataset ..." refusals, recorded) and
// returns them sorted by IAM role, then member, with duplicates merged and
// the basic dataset roles under their legacy names (recorded:
// bigquery-dataset-access). Authorized views and datasets are not IAM; they
// follow the member entries in the order they were added.

// legacyRoles are the access roles READER, WRITER, and OWNER as IAM roles.
var legacyRoles = map[string]string{
	"READER": "roles/bigquery.dataViewer",
	"WRITER": "roles/bigquery.dataEditor",
	"OWNER":  "roles/bigquery.dataOwner",
}

// Special groups. The project groups name the holders of the project's basic
// roles (viewer, editor, owner), which the local role catalog does not
// carry, so they grant nothing locally; allAuthenticatedUsers is the IAM
// member of that name.
const (
	groupProjectReaders = "projectReaders"
	groupProjectWriters = "projectWriters"
	groupProjectOwners  = "projectOwners"
	groupAllUsers       = "allAuthenticatedUsers"
)

// projectGroups are the special groups that are not IAM members.
var projectGroups = []string{groupProjectReaders, groupProjectWriters, groupProjectOwners}

// targetViews is the authorized-dataset target type that authorizes views.
const targetViews = "VIEWS"

// accessMember is one member entry: an IAM role and an IAM member, or a
// project special group's name.
type accessMember struct{ role, member string }

// authorization is one authorized view or, with isDataset, one authorized
// dataset and its target types.
type authorization struct {
	view        tableName
	dataset     string
	isDataset   bool
	targetTypes []string
}

// accessList is a dataset's access list as stored.
type accessList struct {
	members    []accessMember
	authorized []authorization
}

// clone copies an access list.
func (a accessList) clone() accessList {
	copied := accessList{members: slices.Clone(a.members), authorized: slices.Clone(a.authorized)}
	for i := range copied.authorized {
		copied.authorized[i].targetTypes = slices.Clone(a.authorized[i].targetTypes)
	}
	return copied
}

// defaultAccessList is the access GCP gives a dataset created without one:
// the project groups and the creator as OWNER (recorded).
func defaultAccessList(creator string) accessList {
	list := accessList{members: []accessMember{
		{legacyRoles["WRITER"], groupProjectWriters}, {legacyRoles["OWNER"], groupProjectOwners},
		{legacyRoles["READER"], groupProjectReaders},
	}}
	if creator != "" {
		list.members = append(list.members, accessMember{legacyRoles["OWNER"], creator})
	}
	list.members = sortMembers(list.members)
	return list
}

// sortMembers orders member entries by role, then member, and drops
// duplicates.
func sortMembers(members []accessMember) []accessMember {
	slices.SortFunc(members, func(x, y accessMember) int {
		return cmp.Or(strings.Compare(x.role, y.role), strings.Compare(x.member, y.member))
	})
	return slices.Compact(members)
}

// accessEntry is one Dataset.access entry as sent.
type accessEntry struct {
	Role         string          `json:"role"`
	UserByEmail  string          `json:"userByEmail"`
	GroupByEmail string          `json:"groupByEmail"`
	Domain       string          `json:"domain"`
	SpecialGroup string          `json:"specialGroup"`
	IamMember    string          `json:"iamMember"`
	View         *tableReference `json:"view"`
	Routine      json.RawMessage `json:"routine"`
	Dataset      *struct {
		Dataset     datasetReference `json:"dataset"`
		TargetTypes []string         `json:"targetTypes"`
	} `json:"dataset"`
	Condition json.RawMessage `json:"condition"`
}

// tableReference names a table or view.
type tableReference struct {
	ProjectID string `json:"projectId"`
	DatasetID string `json:"datasetId"`
	TableID   string `json:"tableId"`
}

// member is the entry's IAM member, or its project group's name, and how
// many member fields it sets.
func (e accessEntry) member() (string, int) {
	var member string
	count := 0
	for _, field := range []struct{ value, form string }{
		{e.UserByEmail, userMember(e.UserByEmail)}, {e.GroupByEmail, "group:" + e.GroupByEmail},
		{e.Domain, "domain:" + e.Domain}, {e.SpecialGroup, e.SpecialGroup}, {e.IamMember, e.IamMember},
	} {
		if field.value != "" {
			member, count = field.form, count+1
		}
	}
	return member, count
}

// unmodeled refuses an entry's condition and routine, which are gaps.
func (e accessEntry) unmodeled() error {
	if len(e.Condition) > 0 && string(e.Condition) != jsonNull {
		return gap("dataset access entry conditions")
	}
	if len(e.Routine) > 0 && string(e.Routine) != jsonNull {
		return gap("routine access entries (routines are not modeled)")
	}
	return nil
}

// userMember is a userByEmail entry's IAM member: GCP takes a service
// account's email there too and returns it there (recorded).
func userMember(email string) string {
	if strings.HasSuffix(strings.ToLower(email), ".gserviceaccount.com") {
		return "serviceAccount:" + email
	}
	return "user:" + email
}

// accessTarget is what normalizing an access list reads: the dataset's
// project, id, and location, and whether a view or dataset exists.
type accessTarget struct {
	project, dataset, location string
	view                       func(dataset, table string) bool
	datasetExists              func(dataset string) bool
}

// normalizeAccess validates a sent access list and stores it in GCP's form.
// Member entries go to IAM, which validates their roles and members.
func normalizeAccess(entries []accessEntry, target accessTarget) (accessList, error) {
	var list accessList
	for _, entry := range entries {
		if err := addEntry(&list, entry, target); err != nil {
			return list, err
		}
	}
	list.members = sortMembers(list.members)
	return list, nil
}

// addEntry validates one sent entry and appends it to a.
func addEntry(a *accessList, entry accessEntry, target accessTarget) error {
	if err := entry.unmodeled(); err != nil {
		return err
	}
	member, members := entry.member()
	var err error
	var authorized authorization
	switch {
	case entry.View != nil && members == 0 && entry.Dataset == nil:
		authorized, err = authorizeView(*entry.View, target)
	case entry.Dataset != nil && members == 0:
		authorized, err = authorizeDataset(entry.Dataset.Dataset, entry.Dataset.TargetTypes, target)
	case members == 1 && entry.View == nil:
		var added accessMember
		added, err = memberEntry(entry.Role, member, entry.SpecialGroup)
		a.members = append(a.members, added)
		return err
	default:
		// Not recorded.
		return failure(codes.InvalidArgument, reasonInvalid, "An access entry names exactly one member, view, or dataset")
	}
	a.authorized = append(a.authorized, authorized)
	return err
}

// memberEntry maps one member entry's role to its IAM role. A role-less
// entry is refused as GCP refuses it (recorded); IAM refuses an unknown role.
func memberEntry(role, member, specialGroup string) (accessMember, error) {
	if role == "" {
		return accessMember{}, failure(codes.InvalidArgument, reasonInvalid, "Invalid or missing role")
	}
	if specialGroup != "" && specialGroup != groupAllUsers && !slices.Contains(projectGroups, specialGroup) {
		// Not recorded.
		return accessMember{}, failure(codes.InvalidArgument, reasonInvalid, "Invalid special group: "+specialGroup)
	}
	if mapped, ok := legacyRoles[role]; ok {
		role = mapped
	}
	return accessMember{role: role, member: member}, nil
}

// authorizeView checks one authorized view: a view of the project that
// exists (recorded refusal).
func authorizeView(view tableReference, target accessTarget) (authorization, error) {
	if view.ProjectID != "" && view.ProjectID != target.project {
		return authorization{}, gap("authorized views in other projects")
	}
	if !target.view(view.DatasetID, view.TableID) {
		return authorization{}, failure(codes.InvalidArgument, reasonInvalid,
			"View "+target.project+":"+view.DatasetID+"."+view.TableID+" to authorize not found in location "+target.location+".")
	}
	return authorization{view: tableName{dataset: view.DatasetID, table: view.TableID}}, nil
}

// authorizeDataset checks one authorized dataset of the project. Only the
// VIEWS target authorizes anything locally; routines are not modeled.
func authorizeDataset(dataset datasetReference, targetTypes []string, target accessTarget) (authorization, error) {
	if dataset.ProjectID != "" && dataset.ProjectID != target.project {
		return authorization{}, gap("authorized datasets in other projects")
	}
	if !target.datasetExists(dataset.DatasetID) {
		// Not recorded; worded as the recorded view refusal.
		return authorization{}, failure(codes.InvalidArgument, reasonInvalid,
			"Dataset "+target.project+":"+dataset.DatasetID+" to authorize not found in location "+target.location+".")
	}
	return authorization{dataset: dataset.DatasetID, isDataset: true, targetTypes: slices.Clone(targetTypes)}, nil
}

// policy is the dataset IAM policy the member entries make; the project
// groups bind no IAM member.
func (a accessList) policy() iam.Policy {
	policy := iam.Policy{Bindings: []iam.Binding{}}
	for _, entry := range a.members {
		if slices.Contains(projectGroups, entry.member) {
			continue
		}
		if last := len(policy.Bindings) - 1; last >= 0 && policy.Bindings[last].Role == entry.role {
			policy.Bindings[last].Members = append(policy.Bindings[last].Members, entry.member)
			continue
		}
		policy.Bindings = append(policy.Bindings, iam.Binding{Role: entry.role, Members: []string{entry.member}})
	}
	return policy
}

// customRole is the form of a custom role name, as IAM checks it.
var customRole = regexp.MustCompile(`^(?:projects|organizations)/[^/]+/roles/[a-zA-Z0-9_.]{3,64}$`)

// setDatasetPolicy writes the dataset's IAM policy from its access list. A
// refusal reads as GCP's (recorded); a role outside the pinned catalog stays
// UNIMPLEMENTED. The project groups bind no member, so their roles are
// checked here, as GCP checks them (recorded).
func (p *Provider) setDatasetPolicy(project, dataset string, list accessList) error {
	refused := func(message string) error {
		return failure(codes.InvalidArgument, reasonInvalid, "IAM setPolicy failed for Dataset "+project+":"+dataset+": "+message)
	}
	for _, entry := range list.members {
		switch {
		case !slices.Contains(projectGroups, entry.member) || customRole.MatchString(entry.role):
		case !strings.HasPrefix(entry.role, "roles/"):
			return refused(`The role name must be in the form "roles/{role}", "organizations/{organization_id}/roles/{role}", or "projects/{project_id}/roles/{role}".`)
		default:
			if _, pinned := p.engine.PredefinedRole(entry.role); !pinned {
				return status.Error(codes.Unimplemented, "Role is outside the pinned supported role set")
			}
		}
	}
	if _, err := p.engine.SetPolicy(datasetResource(project, dataset), list.policy()); err != nil {
		current := status.Convert(err)
		if current.Code() != codes.InvalidArgument {
			return err //nolint:wrapcheck // already a wire status
		}
		return refused(current.Message())
	}
	return nil
}

// wire renders the access list as Dataset.access.
func (a accessList) wire(project string) []any {
	entries := make([]any, 0, len(a.members)+len(a.authorized))
	for _, entry := range a.members {
		entries = append(entries, memberWire(entry))
	}
	for _, authorized := range a.authorized {
		if authorized.isDataset {
			entries = append(entries, map[string]any{"dataset": map[string]any{
				"dataset":     map[string]any{keyProjectID: project, keyDatasetID: authorized.dataset},
				"targetTypes": slices.Clone(authorized.targetTypes),
			}})
			continue
		}
		entries = append(entries, map[string]any{"view": map[string]any{
			keyProjectID: project, keyDatasetID: authorized.view.dataset, keyTableID: authorized.view.table,
		}})
	}
	return entries
}

// memberWire renders one member entry under GCP's field for its member kind.
func memberWire(entry accessMember) map[string]any {
	role := entry.role
	for legacy, mapped := range legacyRoles {
		if mapped == role {
			role = legacy
		}
	}
	kind, value, _ := strings.Cut(entry.member, ":")
	field := "iamMember"
	switch {
	case slices.Contains(projectGroups, entry.member) || entry.member == groupAllUsers:
		field, value = "specialGroup", entry.member
	case kind == "user" || kind == "serviceAccount":
		field = "userByEmail"
	case kind == "group":
		field = "groupByEmail"
	case kind == "domain":
		field = "domain"
	default:
		value = entry.member
	}
	return map[string]any{"role": role, field: value}
}

// authorizes reports whether the access list lets view read the dataset's
// tables: the view is listed, or its dataset is, for views.
func (a accessList) authorizes(view tableName) bool {
	return slices.ContainsFunc(a.authorized, func(entry authorization) bool {
		if entry.isDataset {
			return strings.EqualFold(entry.dataset, view.dataset) && slices.Contains(entry.targetTypes, targetViews)
		}
		return strings.EqualFold(entry.view.dataset, view.dataset) && strings.EqualFold(entry.view.table, view.table)
	})
}
