package resourcemanager

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"math/big"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// node is the metadata of one hierarchy node that the index doesn't hold.
// A node without a record (seeded by the bootstrap config) reports the
// provider's start as its creation time and, for a folder, its ID as its
// display name.
type node struct {
	displayName      string
	created, updated time.Time
	deleted          time.Time // zero while ACTIVE
}

// meta returns name's record, creating it with the defaults. The caller holds mu.
func (p *Provider) meta(name string) *node {
	record := p.nodes[name]
	if record == nil {
		record = &node{displayName: lastSegment(name), created: p.started, updated: p.started}
		p.nodes[name] = record
	}
	return record
}

// relative strips the service prefix: "folders/123".
func relative(name string) string { return strings.TrimPrefix(name, namePrefix) }

// lastSegment is a name's final path segment.
func lastSegment(name string) string { return name[strings.LastIndex(name, "/")+1:] }

// lookup resolves an indexed node of kind by its relative name
// ("folders/123"). The caller holds mu.
func (p *Provider) lookup(relativeName, kind string) (state.Resource, bool) {
	resource, found := p.index.Get(namePrefix + relativeName)
	return resource, found && resource.Service == service && resource.Kind == kind
}

// lookupProject resolves "projects/ID" or "projects/NUMBER". The caller holds mu.
func (p *Provider) lookupProject(id string) (state.Resource, bool) {
	if resource, found := p.lookup("projects/"+id, kindProject); found {
		return resource, true
	}
	resource, found := p.index.ProjectByNumber(id)
	return resource, found && id != ""
}

// parentRef resolves a parent reference ("organizations/N" or "folders/N").
// ok is false when the reference has another shape; found is false when it
// names no indexed node.
func (p *Provider) parentRef(reference string) (state.Resource, bool, bool) {
	kind, id, cut := strings.Cut(reference, "/")
	if !cut || !nodeIDPattern.MatchString(id) || (kind != collectionOrganizations && kind != collectionFolders) {
		return state.Resource{}, false, false
	}
	if kind == collectionOrganizations {
		resource, found := p.lookup(reference, kindOrganization)
		return resource, true, found
	}
	resource, found := p.lookup(reference, kindFolder)
	return resource, true, found
}

// active reports whether a node is ACTIVE. Organizations and projects
// always are.
func (p *Provider) active(name string) bool {
	record := p.nodes[name]
	return record == nil || record.deleted.IsZero()
}

// children lists the indexed nodes of kind directly under parent, by name.
func (p *Provider) children(parent, kind string) []state.Resource {
	var result []state.Resource
	for _, resource := range p.index.Owned(service) {
		if resource.Kind == kind && resource.Parent == parent {
			result = append(result, resource)
		}
	}
	return result
}

// ofKind lists every indexed node of kind, by name.
func (p *Provider) ofKind(kind string) []state.Resource {
	var result []state.Resource
	for _, resource := range p.index.Owned(service) {
		if resource.Kind == kind {
			result = append(result, resource)
		}
	}
	return result
}

// folderDepth counts the folders from name up to its organization, name
// included when it is a folder.
func (p *Provider) folderDepth(name string) int {
	depth := 0
	for name != "" {
		resource, found := p.index.Get(name)
		if !found {
			break
		}
		if resource.Kind == kindFolder {
			depth++
		}
		name = resource.Parent
	}
	return depth
}

// subtreeDepth is the folder levels at and below folder.
func (p *Provider) subtreeDepth(folder string) int {
	deepest := 0
	for _, child := range p.children(folder, kindFolder) {
		deepest = max(deepest, p.subtreeDepth(child.Name))
	}
	return deepest + 1
}

// within reports whether name is folder or one of its descendants.
func (p *Provider) within(name, folder string) bool {
	for name != "" {
		if name == folder {
			return true
		}
		resource, found := p.index.Get(name)
		if !found {
			return false
		}
		name = resource.Parent
	}
	return false
}

// nameTaken reports an ACTIVE sibling folder with the display name. GCP
// requires display names to be unique among a parent's folders.
func (p *Provider) nameTaken(parent, displayName, except string) bool {
	for _, sibling := range p.children(parent, kindFolder) {
		if sibling.Name != except && p.active(sibling.Name) && p.meta(sibling.Name).displayName == displayName {
			return true
		}
	}
	return false
}

// newFolderID picks an unused random 12-digit folder ID.
func (p *Provider) newFolderID() string {
	limit := big.NewInt(900_000_000_000)
	for {
		offset, err := rand.Int(rand.Reader, limit)
		if err != nil {
			panic(err) // crypto/rand never fails on supported platforms
		}
		id := strconv.FormatInt(offset.Int64()+100_000_000_000, 10)
		if _, taken := p.index.Get(namePrefix + collectionFolders + "/" + id); !taken {
			return id
		}
	}
}

// stamp renders a time as GCP's millisecond timestamps.
func stamp(moment time.Time) string { return moment.UTC().Format("2006-01-02T15:04:05.000Z") }

// withETag adds the weak etag of the rendered fields.
func withETag(body map[string]any) map[string]any {
	encoded, _ := json.Marshal(body) //nolint:errchkjson // strings and maps only
	sum := sha256.Sum256(encoded)
	body["etag"] = `W/"` + hex.EncodeToString(sum[:8]) + `"`
	return body
}

// folderWire renders a v3 Folder.
func (p *Provider) folderWire(resource state.Resource) map[string]any {
	record := p.meta(resource.Name)
	body := map[string]any{fieldName: relative(resource.Name), "parent": relative(resource.Parent),
		fieldDisplayName: record.displayName, fieldState: stateActive,
		fieldCreateTime: stamp(record.created), fieldUpdateTime: stamp(record.updated)}
	if !record.deleted.IsZero() {
		body[fieldState], body["deleteTime"] = stateDeleteRequested, stamp(record.deleted)
	}
	return withETag(body)
}

// organizationWire renders a v3 Organization. The display name (the
// Workspace domain) and directory customer ID aren't modeled.
func (p *Provider) organizationWire(resource state.Resource) map[string]any {
	record := p.meta(resource.Name)
	return withETag(map[string]any{fieldName: relative(resource.Name), fieldState: stateActive,
		fieldCreateTime: stamp(record.created), fieldUpdateTime: stamp(record.updated)})
}

// projectName is a project's v3 name: projects/NUMBER, or projects/ID for a
// bootstrap project without a number.
func projectName(resource state.Resource) string {
	if resource.Number != "" {
		return "projects/" + resource.Number
	}
	return "projects/" + resource.Scope.Project
}

// projectWire renders a v3 Project. Labels aren't modeled; the display name
// is the project ID. Search and list items omit updateTime, as recorded
// (resourcemanager-hierarchy-visibility).
func (p *Provider) projectWire(resource state.Resource, item bool) map[string]any {
	record := p.meta(resource.Name)
	body := map[string]any{fieldName: projectName(resource), "projectId": resource.Scope.Project, fieldState: stateActive,
		fieldDisplayName: resource.Scope.Project, fieldCreateTime: stamp(record.created)}
	if !item {
		body[fieldUpdateTime] = stamp(record.updated)
	}
	if resource.Parent != "" {
		body["parent"] = relative(resource.Parent)
	}
	return withETag(body)
}

// projectWireV1 renders a v1 Project, whose parent is a {type, id} pair.
func (p *Provider) projectWireV1(resource state.Resource) map[string]any {
	body := map[string]any{"projectId": resource.Scope.Project, "lifecycleState": stateActive,
		fieldName: resource.Scope.Project, fieldCreateTime: stamp(p.meta(resource.Name).created)}
	if resource.Number != "" {
		body["projectNumber"] = resource.Number
	}
	if parent, found := p.index.Get(resource.Parent); found {
		body["parent"] = map[string]any{"type": parent.Kind, "id": lastSegment(parent.Name)}
	}
	return body
}
