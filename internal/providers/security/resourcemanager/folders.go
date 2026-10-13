package resourcemanager

import (
	"fmt"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// folderBody is the modeled part of a v3 Folder request body.
type folderBody struct {
	DisplayName string `json:"displayName"`
	Parent      string `json:"parent"`
}

// folderFields are the v3 Folder fields folderBody lacks: tags are set only
// at create and not modeled; the rest are output only.
var folderFields = provider.Fields{Unmodeled: []string{"tags"},
	OutputOnly: []string{fieldName, fieldState, fieldCreateTime, fieldUpdateTime, "deleteTime", "etag", "configuredCapabilities", "managementProject"}}

// errFolderExists refuses a display name an ACTIVE sibling already has.
var errFolderExists = failed("Requested entity already exists")

// errFolderDepth refuses nesting beyond maxFolderDepth.
var errFolderDepth = failed("Folder reached maximum depth.")

// errFolderDeleted refuses a change to a folder in DELETE_REQUESTED.
var errFolderDeleted = failed("The folder is not active.")

// validDisplayName refuses a display name outside the v3 rule.
func validDisplayName(name string) error {
	if !displayNamePattern.MatchString(name) {
		return provider.FieldViolation("display_name", "field [display_name] has issue [invalid display name]")
	}
	return nil
}

// createFolder creates a folder under an organization or folder. A create
// without a parent is the recorded 403 without details; an unknown parent
// is the recorded denial on it.
func (p *Provider) createFolder(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body folderBody
	if err := provider.DecodeJSON(request.JSON, "folder", &body, gap, folderFields); err != nil {
		return nil, err
	}
	if err := authenticated(call); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	parent, decision, err := p.parent(call, body.Parent, errInvalidArgument)
	if err != nil {
		return nil, err
	}
	if err := validDisplayName(body.DisplayName); err != nil {
		return nil, err
	}
	if !p.active(parent.Name) {
		return nil, errFolderDeleted
	}
	return p.run(call, body.Parent, metadataCreateFolder, "Folder", decision, func() (map[string]any, error) {
		if p.folderDepth(parent.Name)+1 > maxFolderDepth {
			return nil, errFolderDepth
		}
		if p.nameTaken(parent.Name, body.DisplayName, "") {
			return nil, errFolderExists
		}
		folder := state.Resource{Name: namePrefix + collectionFolders + "/" + p.newFolderID(), Service: service, Kind: kindFolder,
			Parent: parent.Name, Scope: state.Scope{Location: state.Global}}
		if err := p.index.Put(service, folder); err != nil {
			return nil, status.Errorf(codes.Internal, "publish the folder: %v", err)
		}
		now := p.clock().UTC()
		p.nodes[folder.Name] = &node{displayName: body.DisplayName, created: now, updated: now}
		return p.folderWire(folder), nil
	})
}

// parent resolves and authorizes the organization or folder a create or list
// names. No parent is the recorded 403 without details, another shape is
// malformed, and an unknown parent is the denial on it. The caller holds mu.
func (p *Provider) parent(call gateway.Call, reference string, malformed error) (state.Resource, iam.Decision, error) {
	if reference == "" {
		return state.Resource{}, iam.Decision{}, errNoPermission
	}
	parent, ok, found := p.parentRef(reference)
	switch {
	case !ok:
		return state.Resource{}, iam.Decision{}, malformed
	case !found:
		return state.Resource{}, iam.Decision{}, p.absent(call, reference)
	}
	decision, err := p.check(call, parent.Name)
	return parent, decision, err
}

// addressed resolves the folder a request's path names and authorizes call
// on it. The caller holds mu.
func (p *Provider) addressed(call gateway.Call, request *gateway.RESTRequest) (state.Resource, iam.Decision, error) {
	name := collectionFolders + "/" + request.Path["folder"]
	folder, found := p.lookup(name, kindFolder)
	if !found {
		return state.Resource{}, iam.Decision{}, p.absent(call, name)
	}
	decision, err := p.check(call, folder.Name)
	return folder, decision, err
}

// getFolder reads one folder, in either state.
func (p *Provider) getFolder(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	folder, decision, err := p.addressed(call, request)
	if err != nil {
		return nil, err
	}
	return respond(p.folderWire(folder), decision), nil
}

// listFolders lists a parent's direct folders, ACTIVE ones unless
// showDeleted is true. A list without a parent is the recorded 403 without
// details.
func (p *Provider) listFolders(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := authenticated(call); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	parent, decision, err := p.parent(call, request.Query.Get("parent"), errInvalidParent)
	if err != nil {
		return nil, err
	}
	deleted, err := boolQuery(request, "showDeleted")
	if err != nil {
		return nil, err
	}
	var folders []map[string]any
	for _, child := range p.children(parent.Name, kindFolder) {
		if deleted || p.active(child.Name) {
			folders = append(folders, p.folderWire(child))
		}
	}
	page, next, err := provider.Page(folders, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	return respond(provider.ListBody(collectionFolders, page, next), decision), nil
}

// searchFolders returns the folders matching query that the caller may get.
// The query supports exact displayName, parent, and state terms joined by AND.
func (p *Provider) searchFolders(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := authenticated(call); err != nil {
		return nil, err
	}
	terms, err := parseQuery(request.Query.Get("query"), fieldDisplayName, "parent", fieldState)
	if err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	var folders []map[string]any
	for _, folder := range p.ofKind(kindFolder) {
		wire := p.folderWire(folder)
		if !terms.match(wire) {
			continue
		}
		visible, err := p.visible(call, folder.Name)
		if err != nil {
			return nil, err
		}
		if visible {
			folders = append(folders, wire)
		}
	}
	page, next, err := provider.Page(folders, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: provider.ListBody(collectionFolders, page, next)}, nil
}

// visible reports whether a search shows a node: the caller holds the
// search's get permission on it. Refusals other than a denial fail closed.
func (p *Provider) visible(call gateway.Call, name string) (bool, error) {
	_, err := p.check(call, name)
	switch status.Code(err) {
	case codes.OK:
		return true, nil
	case codes.PermissionDenied:
		return false, nil
	default:
		return false, err
	}
}

// patchFolder renames a folder. display_name is the only updatable field;
// an absent update mask means it, as Terraform sends none.
func (p *Provider) patchFolder(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	for path := range strings.SplitSeq(request.Query.Get("updateMask"), ",") {
		if path != "" && path != "display_name" && path != fieldDisplayName {
			return nil, provider.FieldViolation("update_mask", "field [update_mask] has issue [only display_name can be updated]")
		}
	}
	var body folderBody
	if err := provider.DecodeJSON(request.JSON, "folder", &body, gap, folderFields); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	folder, decision, err := p.addressed(call, request)
	if err != nil {
		return nil, err
	}
	if err := validDisplayName(body.DisplayName); err != nil {
		return nil, err
	}
	if !p.active(folder.Name) {
		return nil, errFolderDeleted
	}
	return p.run(call, relative(folder.Name), metadataUpdateFolder, "Folder", decision, func() (map[string]any, error) {
		if p.nameTaken(folder.Parent, body.DisplayName, folder.Name) {
			return nil, errFolderExists
		}
		record := p.meta(folder.Name)
		record.displayName, record.updated = body.DisplayName, p.clock().UTC()
		return p.folderWire(folder), nil
	})
}

// deleteFolder moves an empty ACTIVE folder to DELETE_REQUESTED. A folder is
// empty when no ACTIVE folder or project is under it. Purging after 30 days
// is not modeled: the folder stays until undeleted.
func (p *Provider) deleteFolder(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	folder, decision, err := p.addressed(call, request)
	if err != nil {
		return nil, err
	}
	if !p.active(folder.Name) {
		return nil, errFolderDeleted
	}
	return p.run(call, relative(folder.Name), metadataDeleteFolder, "Folder", decision, func() (map[string]any, error) {
		for _, child := range p.children(folder.Name, kindFolder) {
			if p.active(child.Name) {
				return nil, failed("Folder must be empty to be deleted.")
			}
		}
		if len(p.children(folder.Name, kindProject)) > 0 {
			return nil, failed("Folder must be empty to be deleted.")
		}
		record, now := p.meta(folder.Name), p.clock().UTC()
		record.deleted, record.updated = now, now
		return p.folderWire(folder), nil
	})
}

// undeleteFolder returns a DELETE_REQUESTED folder to ACTIVE under its
// ACTIVE parent.
func (p *Provider) undeleteFolder(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	if err := provider.DecodeJSON(request.JSON, "", &struct{}{}, gap, provider.Fields{}); err != nil {
		return nil, err
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	folder, decision, err := p.addressed(call, request)
	if err != nil {
		return nil, err
	}
	if p.active(folder.Name) {
		return nil, failed("The folder is not in DELETE_REQUESTED state.")
	}
	if !p.active(folder.Parent) {
		return nil, failed("The parent folder is not active.")
	}
	return p.run(call, relative(folder.Name), metadataUndeleteFolder, "Folder", decision, func() (map[string]any, error) {
		record := p.meta(folder.Name)
		if p.nameTaken(folder.Parent, record.displayName, folder.Name) {
			return nil, errFolderExists
		}
		record.deleted, record.updated = time.Time{}, p.clock().UTC()
		return p.folderWire(folder), nil
	})
}

// moveFolder reparents an ACTIVE folder. The caller needs
// resourcemanager.folders.move on the folder (its current parent's grants
// included) and on the destination. A malformed destination is GCP's bare
// 400; an unknown folder or a denial is the 403 without details (recorded
// for folders/1).
func (p *Provider) moveFolder(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
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
	if _, ok, _ := p.parentRef(body.DestinationParent); !ok {
		return nil, errInvalidArgument
	}
	folder, found := p.lookup(collectionFolders+"/"+request.Path["folder"], kindFolder)
	if !found {
		return nil, errNoPermission
	}
	decision, err := p.check(call, folder.Name)
	if err != nil {
		return nil, plain(err)
	}
	destination, err := p.destination(call, body.DestinationParent)
	if err != nil {
		return nil, plain(err)
	}
	if err := p.movable(folder, destination); err != nil {
		return nil, err
	}
	return p.run(call, relative(folder.Name), metadataMoveFolder, "Folder", decision, func() (map[string]any, error) {
		return p.reparentFolder(folder, destination)
	})
}

// movable refuses a move under the folder itself or a descendant, and a
// move of or into a folder in DELETE_REQUESTED.
func (p *Provider) movable(folder, destination state.Resource) error {
	if p.within(destination.Name, folder.Name) {
		return failed("A folder cannot move under itself or one of its descendants.")
	}
	if !p.active(folder.Name) || !p.active(destination.Name) {
		return errFolderDeleted
	}
	return nil
}

// reparentFolder is a move's work: the depth limit and sibling names are
// checked against the destination, then the index entry moves.
func (p *Provider) reparentFolder(folder, destination state.Resource) (map[string]any, error) {
	if p.folderDepth(destination.Name)+p.subtreeDepth(folder.Name) > maxFolderDepth {
		return nil, errFolderDepth
	}
	if p.nameTaken(destination.Name, p.meta(folder.Name).displayName, folder.Name) {
		return nil, errFolderExists
	}
	if err := p.index.Reparent(service, folder.Name, destination.Name); err != nil {
		return nil, status.Errorf(codes.Internal, "move the folder: %v", err)
	}
	folder.Parent = destination.Name
	p.meta(folder.Name).updated = p.clock().UTC()
	return p.folderWire(folder), nil
}

// destination resolves and authorizes a move's destination parent. A
// malformed one is GCP's bare 400; an unknown one is the denial on it.
func (p *Provider) destination(call gateway.Call, reference string) (state.Resource, error) {
	destination, ok, found := p.parentRef(reference)
	switch {
	case !ok:
		return state.Resource{}, errInvalidArgument
	case !found:
		return state.Resource{}, p.absent(call, reference)
	}
	if _, err := p.check(call, destination.Name); err != nil {
		return state.Resource{}, err
	}
	return destination, nil
}

// boolQuery reads an optional boolean query parameter.
func boolQuery(request *gateway.RESTRequest, name string) (bool, error) {
	switch value := request.Query.Get(name); value {
	case "", "false":
		return false, nil
	case "true":
		return true, nil
	default:
		return false, provider.FieldViolation(name, fmt.Sprintf("Invalid value at '%s' (TYPE_BOOL), %q", name, value))
	}
}
