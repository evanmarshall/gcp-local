package resourcemanager

import (
	"slices"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// getProject reads one project by ID or number, on v3 or v1. An unknown
// project is the recorded denial, not 404.
func (p *Provider) getProject(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	id := request.Path["project"]
	p.mu.Lock()
	defer p.mu.Unlock()
	project, found := p.lookupProject(id)
	if !found {
		return nil, p.absent(call, "projects/"+id)
	}
	decision, err := p.gate.Check(call, iam.Target{Name: project.Name}, "projects/"+id)
	if err != nil {
		return nil, err
	}
	if call.Method == methodProjectsGetV1 {
		return respond(p.projectWireV1(project), decision), nil
	}
	return respond(p.projectWire(project, false), decision), nil
}

// listProjects lists an organization's or folder's direct projects. A
// missing or malformed parent is the recorded 400; a denial is the recorded
// 403 without details.
func (p *Provider) listProjects(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := authenticated(call); err != nil {
		return nil, err
	}
	if _, err := boolQuery(request, "showDeleted"); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	parent, ok, found := p.parentRef(request.Query.Get("parent"))
	switch {
	case !ok:
		return nil, errInvalidParent
	case !found:
		return nil, errNoPermission
	}
	decision, err := p.check(call, parent.Name)
	if err != nil {
		return nil, plain(err)
	}
	var projects []map[string]any
	for _, project := range p.children(parent.Name, kindProject) {
		projects = append(projects, p.projectWire(project, true))
	}
	page, next, err := provider.Page(projects, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	return respond(provider.ListBody("projects", page, next), decision), nil
}

// searchProjects returns the projects matching query that the caller may
// get, ordered by project ID. The query supports exact parent, id
// (projectId), and state terms joined by AND.
func (p *Provider) searchProjects(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := authenticated(call); err != nil {
		return nil, err
	}
	terms, err := parseQuery(request.Query.Get("query"), "parent", "id", "projectId", fieldState)
	if err != nil {
		return nil, err
	}
	if id, ok := terms["id"]; ok {
		delete(terms, "id")
		terms["projectId"] = id
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var projects []map[string]any
	for _, project := range p.ofKind(kindProject) {
		wire := p.projectWire(project, true)
		if !terms.match(wire) {
			continue
		}
		visible, err := p.visible(call, project.Name)
		if err != nil {
			return nil, err
		}
		if visible {
			projects = append(projects, wire)
		}
	}
	page, next, err := provider.Page(projects, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: provider.ListBody("projects", page, next)}, nil
}

// moveProject reparents a project under an ACTIVE organization or folder.
// The caller needs resourcemanager.projects.move on the project (its current
// parent's grants included) and on the destination; a denial is the
// recorded 403 without details. The new ancestry governs the project's IAM
// from the move on.
func (p *Provider) moveProject(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body struct {
		DestinationParent string `json:"destinationParent"`
	}
	if err := provider.DecodeJSON(request.JSON, "", &body, gap, provider.Fields{}); err != nil {
		return nil, err
	}
	if err := authenticated(call); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	project, found := p.lookupProject(request.Path["project"])
	if !found {
		return nil, errNoPermission
	}
	decision, err := p.check(call, project.Name)
	if err != nil {
		return nil, plain(err)
	}
	destination, err := p.destination(call, body.DestinationParent)
	if err != nil {
		return nil, plain(err)
	}
	if !p.active(destination.Name) {
		return nil, errFolderDeleted
	}
	return p.run(call, projectName(project), metadataMoveProject, "Project", decision, func() (map[string]any, error) {
		if err := p.index.Reparent(service, project.Name, destination.Name); err != nil {
			return nil, failed("move the project: " + err.Error())
		}
		project.Parent = destination.Name
		p.meta(project.Name).updated = p.clock().UTC()
		return p.projectWire(project, false), nil
	})
}

// getOrganization reads one organization.
func (p *Provider) getOrganization(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	name := "organizations/" + request.Path["organization"]
	p.mu.Lock()
	defer p.mu.Unlock()
	organization, found := p.lookup(name, kindOrganization)
	if !found {
		return nil, p.absent(call, name)
	}
	decision, err := p.check(call, organization.Name)
	if err != nil {
		return nil, err
	}
	return respond(p.organizationWire(organization), decision), nil
}

// searchOrganizations returns the organizations the caller may get. A query
// (domain:) is a gap: organization domains aren't modeled.
func (p *Provider) searchOrganizations(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := authenticated(call); err != nil {
		return nil, err
	}
	if _, err := parseQuery(request.Query.Get("query")); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var organizations []map[string]any
	for _, organization := range p.ofKind(kindOrganization) {
		visible, err := p.visible(call, organization.Name)
		if err != nil {
			return nil, err
		}
		if visible {
			organizations = append(organizations, p.organizationWire(organization))
		}
	}
	page, next, err := provider.Page(organizations, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: provider.ListBody(collectionOrganizations, page, next)}, nil
}

// terms are a search query's exact field matches.
type terms map[string]string

// parseQuery reads a search query of KEY=VALUE or KEY:VALUE terms joined by
// " AND ", for the keys a method supports. Other keys, wildcards, and other
// operators are a gap.
func parseQuery(query string, keys ...string) (terms, error) {
	result := terms{}
	if strings.TrimSpace(query) == "" {
		return result, nil
	}
	for term := range strings.SplitSeq(query, " AND ") {
		term = strings.TrimSpace(term)
		split := strings.IndexAny(term, "=:")
		if split <= 0 {
			return nil, gap("search query term " + strconv.Quote(term))
		}
		key, value := term[:split], strings.Trim(strings.TrimSpace(term[split+1:]), `"`)
		if !slices.Contains(keys, key) || value == "" || strings.Contains(value, "*") {
			return nil, gap("search query term " + strconv.Quote(term))
		}
		result[key] = value
	}
	return result, nil
}

// match reports whether a rendered node has every term's value.
func (t terms) match(wire map[string]any) bool {
	for key, value := range t {
		if provider.Text(wire[key]) != value {
			return false
		}
	}
	return true
}

// lookupRef resolves the node a policy or permission request names. The
// caller holds mu.
func (p *Provider) lookupRef(reference string) (state.Resource, bool) {
	kind, id, _ := strings.Cut(reference, "/")
	switch kind {
	case "projects":
		return p.lookupProject(id)
	case collectionFolders:
		return p.lookup(reference, kindFolder)
	default:
		return p.lookup(reference, kindOrganization)
	}
}
