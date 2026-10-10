package firestore

import (
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// mu guards the clock-free field decode across requests.
var mu sync.Mutex

// execute dispatches one authorized Firestore request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodDocumentsCreate:
		return p.createFromCollection(call, request, scope)
	case methodDocumentsUpdate:
		return p.upsert(call, request, scope)
	case methodDocumentsGet:
		return p.get(request, scope)
	case methodDocumentsList:
		return p.list(request, scope)
	default:
		return p.remove(request, scope)
	}
}

// documentKey is the state key of one document.
func documentKey(collection, id string) string { return collection + "/" + id }

// upsert creates or replaces one document with the posted fields.
func (p *Provider) upsert(_ gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, id := request.Path["collectionId"], request.Path["documentId"]
	var body struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(request.JSON, &body); err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid document body")
	}
	if body.Fields == nil {
		return nil, status.Error(codes.InvalidArgument, "A document requires a fields object")
	}
	now := p.clock()
	var created bool
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := documentKey(collection, id)
		existing := s.documents[key]
		created = existing == nil
		stored := &documentRecord{Collection: collection, ID: id, Fields: body.Fields, CreateTime: stamp(now), UpdateTime: stamp(now)}
		if existing != nil {
			stored.CreateTime = existing.CreateTime
		}
		s.documents[key] = stored
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store the document: %w", err)
	}
	if created {
		if indexErr := p.index.Put(serviceName, state.Resource{
			Name: documentResource(scope.Project, collection, id), Service: serviceName,
			Kind: kindDocument, Parent: projectResource(scope.Project), Scope: scope,
		}); indexErr != nil {
			return nil, fmt.Errorf("index the document: %w", indexErr)
		}
	}
	mu.Lock()
	defer mu.Unlock()
	return &gateway.RESTResponse{JSON: documentWire(scope.Project, collection, id, body.Fields, now, now)}, nil
}

// get returns one document.
func (p *Provider) get(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, id := request.Path["collectionId"], request.Path["documentId"]
	var record *documentRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record = s.documents[documentKey(collection, id)]
		return nil
	})
	if record == nil {
		return nil, notFound(kindDocument, id)
	}
	return &gateway.RESTResponse{JSON: documentWire(scope.Project, collection, id, record.Fields, parseStamp(record.CreateTime), parseStamp(record.UpdateTime))}, nil
}

// list returns the collection's documents.
func (p *Provider) list(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection := request.Path["collectionId"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for key, record := range s.documents {
			coll, _ := splitKey(key)
			if coll != collection {
				continue
			}
			items = append(items, documentWire(scope.Project, record.Collection, record.ID, record.Fields, parseStamp(record.CreateTime), parseStamp(record.UpdateTime)))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"documents": items}}, nil
}

// remove deletes one document.
func (p *Provider) remove(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, id := request.Path["collectionId"], request.Path["documentId"]
	var existed bool
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := documentKey(collection, id)
		if s.documents[key] != nil {
			existed = true
			delete(s.documents, key)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("remove the document: %w", err)
	}
	if !existed {
		return nil, notFound(kindDocument, id)
	}
	if err := p.index.Remove(serviceName, scope, documentResource(scope.Project, collection, id)); err != nil {
		return nil, fmt.Errorf("unregister the document: %w", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// documentWire renders one document wire shape.
func documentWire(project, collection, id string, fields map[string]any, created, updated time.Time) map[string]any {
	return map[string]any{
		"name":       documentName(project, collection, id),
		"fields":     fields,
		"createTime": stamp(created),
		"updateTime": stamp(updated),
	}
}

// splitKey splits a state key into collection and id.
func splitKey(key string) (string, string) {
	for i := range len(key) {
		if key[i] == '/' {
			return key[:i], key[i+1:]
		}
	}
	return key, ""
}

// parseStamp reads one stored timestamp, defaulting to the zero time.
func parseStamp(value string) time.Time {
	parsed, err := time.Parse("2006-01-02T15:04:05.000000Z", value)
	if err != nil {
		return time.Time{}
	}
	return parsed
}

// createFromCollection creates one document from a collection POST with the
// documentId query, matching the real surface's collection-level create.
func (p *Provider) createFromCollection(_ gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection := request.Path["collectionId"]
	id := request.Query.Get("documentId")
	if !uid.MatchString(id) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid document id %q", id)
	}
	var body struct {
		Fields map[string]any `json:"fields"`
	}
	if err := json.Unmarshal(request.JSON, &body); err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid document body")
	}
	now := p.clock()
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := documentKey(collection, id)
		if s.documents[key] != nil {
			return status.Errorf(codes.AlreadyExists, "Document %s already exists", id)
		}
		s.documents[key] = &documentRecord{Collection: collection, ID: id, Fields: body.Fields, CreateTime: stamp(now), UpdateTime: stamp(now)}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store the document: %w", err)
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: documentResource(scope.Project, collection, id), Service: serviceName,
		Kind: kindDocument, Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the document: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: documentWire(scope.Project, collection, id, body.Fields, now, now)}, nil
}
