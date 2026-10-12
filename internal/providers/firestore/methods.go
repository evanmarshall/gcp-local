package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/url"
	"strings"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
)

// execute dispatches one authorized Firestore request inside the scope's
// backend operation. Documents belong to the emulator; collection-group field
// configuration is the Admin API, which the emulator does not serve, so the
// provider keeps it.
func (p *Provider) execute(ctx context.Context, b driver, call gateway.Call, request *gateway.RESTRequest, scope state.Scope, operation restOperation) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodFieldsGet:
		return p.fieldGet(request, scope)
	case methodFieldsUpdate:
		return p.fieldUpdate(request, scope)
	default:
		return forward(ctx, b, operation, request)
	}
}

// restOperation is one validated REST request: its permission plans and the
// emulator request the provider rebuilds from validated route parameters.
type restOperation struct {
	plans       []string
	verb, path  string
	body, query bool
}

// customMessages builds the request message a custom REST method decodes
// into for name validation and permission planning.
var customMessages = map[string]func() proto.Message{
	methodBatchGet:          func() proto.Message { return &pb.BatchGetDocumentsRequest{} },
	methodBatchWrite:        func() proto.Message { return &pb.BatchWriteRequest{} },
	methodBeginTransaction:  func() proto.Message { return &pb.BeginTransactionRequest{} },
	methodCommit:            func() proto.Message { return &pb.CommitRequest{} },
	methodRollback:          func() proto.Message { return &pb.RollbackRequest{} },
	methodRunQuery:          func() proto.Message { return &pb.RunQueryRequest{} },
	methodRunAggregation:    func() proto.Message { return &pb.RunAggregationQueryRequest{} },
	methodListCollectionIDs: func() proto.Message { return &pb.ListCollectionIdsRequest{} },
}

// restOperationFor validates one REST request and plans its authorization.
// The documents routes carry a multi-segment path whose parity selects a
// document (even) or a collection (odd); GET serves both get and list.
func restOperationFor(method string, request *gateway.RESTRequest) (restOperation, error) {
	if method == methodFieldsGet || method == methodFieldsUpdate {
		return restOperation{plans: []string{method}}, nil
	}
	root := "/v1/projects/" + url.PathEscape(request.Path["project"]) + "/databases/(default)/documents"
	path, nested := request.Path["path"]
	var segments []string
	if nested {
		escaped, err := escapeSegments(path)
		if err != nil {
			return restOperation{}, err
		}
		root += "/" + escaped
		segments = strings.Split(path, "/")
	}
	if operation, err := documentOperation(method, request, root, len(segments)%2 == 0); !errors.Is(err, errUnmatched) {
		return operation, err
	}
	return customOperation(method, request, root)
}

// escapeSegments validates and escapes each collection/document segment.
func escapeSegments(path string) (string, error) {
	segments := strings.Split(path, "/")
	for i, segment := range segments {
		if !validSegment(segment) {
			return "", status.Errorf(codes.InvalidArgument, "Invalid document path %q", path)
		}
		segments[i] = url.PathEscape(segment)
	}
	return strings.Join(segments, "/"), nil
}

// documentOperation plans the document routes; document reports an even
// segment count.
func documentOperation(method string, request *gateway.RESTRequest, root string, document bool) (restOperation, error) {
	path := request.Path["path"]
	switch method {
	case methodDocumentsGet:
		plan := methodDocumentsGet
		if !document {
			plan = planDocumentsList
		}
		return restOperation{plans: []string{plan}, verb: http.MethodGet, path: root, query: true}, nil
	case methodDocumentsCreate:
		if document {
			return restOperation{}, status.Errorf(codes.InvalidArgument, "Create needs a collection path, not %q", path)
		}
		if id := request.Query.Get("documentId"); id != "" && !validSegment(id) {
			return restOperation{}, status.Errorf(codes.InvalidArgument, "Invalid document id %q", id)
		}
		return restOperation{plans: []string{method}, verb: http.MethodPost, path: root, body: true, query: true}, nil
	case methodDocumentsUpdate, methodDocumentsDelete:
		if !document {
			return restOperation{}, status.Errorf(codes.InvalidArgument, "Invalid document name %q", path)
		}
		return restOperation{plans: []string{method}, verb: httpVerb(method), path: root, body: method == methodDocumentsUpdate, query: true}, nil
	default:
		return restOperation{}, errUnmatched
	}
}

// customOperation decodes a custom method's body strictly into its Firestore
// request message, fills the database or parent from the route, and plans it.
func customOperation(method string, request *gateway.RESTRequest, root string) (restOperation, error) {
	newMessage, ok := customMessages[method]
	if !ok {
		return restOperation{}, status.Error(codes.Unimplemented, "Firestore coverage gap: this method is not served")
	}
	message := newMessage()
	if err := protojson.Unmarshal(request.JSON, message); err != nil {
		return restOperation{}, status.Errorf(codes.InvalidArgument, "Invalid %s body: %v", method, err)
	}
	// The route names the database or parent; it wins over the body.
	project := request.Path["project"]
	parent := databaseName(project) + "/documents"
	if path, nested := request.Path["path"]; nested {
		parent += "/" + path
	}
	reflected := message.ProtoReflect()
	if field := reflected.Descriptor().Fields().ByName("database"); field != nil {
		reflected.Set(field, protoreflect.ValueOfString(databaseName(project)))
	} else if field := reflected.Descriptor().Fields().ByName("parent"); field != nil {
		reflected.Set(field, protoreflect.ValueOfString(parent))
	}
	planned, err := requestPlans(project, message)
	if err != nil {
		return restOperation{}, err
	}
	custom := strings.TrimPrefix(method, "firestore.documents.")
	return restOperation{plans: planned, verb: http.MethodPost, path: root + ":" + custom, body: true}, nil
}

// forwardedQuery lists the documented documents-API parameters the emulator
// receives; system parameters and credentials such as access_token never do.
var forwardedQuery = map[string]bool{
	"documentId": true, "mask.fieldPaths": true, "updateMask.fieldPaths": true,
	"currentDocument.exists": true, "currentDocument.updateTime": true,
	"pageSize": true, "pageToken": true, "orderBy": true, "showMissing": true, "readTime": true,
}

// maxBackendResponse bounds one emulator response body.
const maxBackendResponse = 32 << 20

// forward replays one authorized documents request against the private
// emulator and returns its status and JSON body unchanged. The fixed "owner"
// bearer is the emulator's documented administrative bypass of security rules:
// the provider has already made the IAM decision, and no caller credential is
// sent.
func forward(ctx context.Context, b driver, operation restOperation, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body io.Reader
	if operation.body {
		body = bytes.NewReader(request.JSON)
	}
	target := operation.path
	if operation.query {
		target += forwardedQueryString(request)
	}
	outbound, err := http.NewRequestWithContext(ctx, operation.verb, b.baseURL()+target, body)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to build the emulator request")
	}
	outbound.Header.Set("Authorization", ownerBearer)
	if body != nil {
		outbound.Header.Set("Content-Type", "application/json")
	}
	response, err := backendClient.Do(outbound)
	if err != nil {
		if ctx.Err() != nil {
			return nil, status.FromContextError(ctx.Err()).Err()
		}
		return nil, status.Error(codes.Unavailable, "Firestore emulator request failed")
	}
	defer response.Body.Close()
	return relay(response)
}

// forwardedQueryString keeps only the allowed documents query parameters.
func forwardedQueryString(request *gateway.RESTRequest) string {
	query := url.Values{}
	for key, values := range request.Query {
		if forwardedQuery[key] {
			query[key] = values
		}
	}
	if len(query) == 0 {
		return ""
	}
	return "?" + query.Encode()
}

// relay returns the emulator's status and JSON body unchanged; a non-JSON
// reply means the emulator does not serve that surface.
func relay(response *http.Response) (*gateway.RESTResponse, error) {
	raw, err := io.ReadAll(io.LimitReader(response.Body, maxBackendResponse+1))
	if err != nil || len(raw) > maxBackendResponse {
		return nil, status.Error(codes.Unavailable, "Firestore emulator response was unreadable or too large")
	}
	if !json.Valid(raw) {
		return nil, status.Errorf(codes.Unimplemented, "Firestore coverage gap: the emulator returned HTTP %d without a JSON body", response.StatusCode)
	}
	return &gateway.RESTResponse{Status: response.StatusCode, JSON: json.RawMessage(raw)}, nil
}

// httpVerb maps a documents method to its REST verb.
func httpVerb(method string) string {
	switch method {
	case methodDocumentsCreate:
		return http.MethodPost
	case methodDocumentsUpdate:
		return http.MethodPatch
	case methodDocumentsDelete:
		return http.MethodDelete
	default:
		return http.MethodGet
	}
}

// fieldGet returns one collection-group field record.
func (p *Provider) fieldGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, field := request.Path["collectionId"], request.Path["fieldId"]
	if !uid.MatchString(collection) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid collection id %q", collection)
	}
	if !uid.MatchString(field) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid field id %q", field)
	}
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		document = s.fields[collection+"/"+field].wire(projectField(scope.Project, collection, field))
		return nil
	})
	return &gateway.RESTResponse{JSON: document}, nil
}

// wire renders a stored field, or an unconfigured one when record is nil:
// real Firestore reports an unconfigured field with fully automatic index
// settings rather than a 404. Call it under the scope lock.
func (record *fieldRecord) wire(name string) map[string]any {
	if record == nil {
		return fieldWire(name, nil, nil, false)
	}
	return fieldWire(record.Name, maps.Clone(record.TTLConfig), maps.Clone(record.IndexConfig), record.Configured)
}

// fieldUpdate patches one field's TTL or index configuration.
func (p *Provider) fieldUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, field := request.Path["collectionId"], request.Path["fieldId"]
	if !uid.MatchString(collection) || !uid.MatchString(field) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid field path")
	}
	var body struct {
		TTLConfig *struct {
			ExpirationPolicy *struct {
				Expiration string `json:"expiration"`
			} `json:"expirationPolicy"`
		} `json:"ttlConfig"`
		IndexConfig *struct {
			AncestorField string `json:"ancestorField"`
		} `json:"indexConfig"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid field body")
	}
	now := p.clock()
	var ttl map[string]any
	if body.TTLConfig != nil && body.TTLConfig.ExpirationPolicy != nil {
		ttl = map[string]any{"expirationPolicy": map[string]any{"expiration": body.TTLConfig.ExpirationPolicy.Expiration}}
	}
	var index map[string]any
	if body.IndexConfig != nil {
		index = map[string]any{"ancestorField": body.IndexConfig.AncestorField}
	}
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.fields[collection+"/"+field]
		if record == nil {
			record = &fieldRecord{Name: projectField(scope.Project, collection, field), CreatedAt: now}
			s.fields[collection+"/"+field] = record
		}
		record.TTLConfig, record.IndexConfig = ttl, index
		record.Configured = true
		document = record.wire(record.Name)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store the field configuration: %w", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{"done": true, "response": document}}, nil
}

func projectField(project, collection, field string) string {
	return "projects/" + project + "/databases/(default)/collectionGroups/" + collection + "/fields/" + field
}

func fieldWire(name string, ttl, index map[string]any, configured bool) map[string]any {
	document := map[string]any{
		"name":        name,
		"indexConfig": map[string]any{"ancestorField": "REQUEST_TIME", "reverting": false, "usesAncestorConfig": true},
	}
	document["ttlConfig"] = map[string]any{}
	if configured {
		if ttl != nil {
			document["ttlConfig"] = ttl
		} else {
			// An empty HCL ttl_config{} sets the default expiration policy;
			// the wire always carries one once configured.
			document["ttlConfig"] = map[string]any{"expirationPolicy": map[string]any{"expiration": "864000s"}}
		}
	}
	if index != nil {
		document["indexConfig"] = index
	}
	return document
}
