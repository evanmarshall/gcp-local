package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/firestore/apiv1/admin/adminpb"
	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// gap reports an unmodeled Firestore request shape explicitly.
var gap = provider.CoverageGap("Firestore")

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
	if adminMethods[method] {
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
	if err := decodeDocument(method, request); err != nil {
		return restOperation{}, err
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

// decodeDocument strictly decodes a create or patch body, which is forwarded
// as is: the emulator would accept fields a Document doesn't have.
func decodeDocument(method string, request *gateway.RESTRequest) error {
	if method != methodDocumentsCreate && method != methodDocumentsUpdate {
		return nil
	}
	return provider.DecodeProto(request.JSON, "document", &pb.Document{}, gap)
}

// customOperation decodes a custom method's body strictly into its Firestore
// request message, fills the database or parent from the route, and plans it.
func customOperation(method string, request *gateway.RESTRequest, root string) (restOperation, error) {
	newMessage, ok := customMessages[method]
	if !ok {
		return restOperation{}, status.Error(codes.Unimplemented, "Firestore coverage gap: this method is not served")
	}
	message := newMessage()
	if err := provider.DecodeProto(request.JSON, "", message, gap); err != nil {
		return restOperation{}, err
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

// serveAdmin serves the collection-group field and operation methods.
func (p *Provider) serveAdmin(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodFieldsGet:
		return p.fieldGet(request, scope)
	case methodFieldsUpdate:
		return p.fieldUpdate(request, scope)
	case methodOperationsList:
		body, err := p.operations.List(scope, request.Query, provider.Paging{Default: 100, Max: 1000}, gap)
		if err != nil {
			return nil, err
		}
		for _, item := range provider.MapList(anySlice(body["operations"])) {
			renameOperation(scope, item)
		}
		return &gateway.RESTResponse{JSON: body}, nil
	}
	body, err := p.operations.Get(scope, request.Path["operation"])
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: renameOperation(scope, body)}, nil
}

// anySlice widens a list of operations to the []any MapList reads.
func anySlice(value any) []any {
	items, _ := value.([]map[string]any)
	out := make([]any, 0, len(items))
	for _, item := range items {
		out = append(out, item)
	}
	return out
}

// renameOperation gives an operation Firestore's name,
// projects/P/databases/(default)/operations/ID, and omits done while false,
// as GCP's proto3 JSON does (firestore-document-lifecycle).
func renameOperation(scope state.Scope, body map[string]any) map[string]any {
	body["name"] = databaseName(scope.Project) + "/operations/" + provider.LastSegment(provider.Text(body["name"]))
	if body["done"] == false {
		delete(body, "done")
	}
	return body
}

// keyState is the JSON key of a resource or operation state.
const keyState = "state"

// fieldOperationMetadata renders a field operation's FieldOperationMetadata
// with the fields GCP returns (firestore-document-lifecycle).
func fieldOperationMetadata(operation provider.Operation) map[string]any {
	metadata := map[string]any{
		"@type": "type.googleapis.com/google.firestore.admin.v1.FieldOperationMetadata",
		"field": operation.Target, "startTime": provider.Stamp(operation.Created), keyState: "PROCESSING",
		"ttlConfigDelta": map[string]any{"changeType": operation.Verb},
	}
	if operation.Done {
		metadata[keyState], metadata["endTime"] = "SUCCESSFUL", provider.Stamp(operation.Ended)
		metadata["progressDocuments"] = map[string]any{}
		if operation.CancelRequested {
			metadata[keyState] = "CANCELLED"
		}
	}
	return metadata
}

// fieldGet returns one collection-group field.
func (p *Provider) fieldGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, field, err := fieldPath(request)
	if err != nil {
		return nil, err
	}
	now := p.clock()
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		document = fieldWire(scope.Project, collection, field, s.fields[collection+"/"+field], now)
		return nil
	})
	return &gateway.RESTResponse{JSON: document}, nil
}

// fieldPath validates a field route's collection group and field.
func fieldPath(request *gateway.RESTRequest) (string, string, error) {
	collection, field := request.Path["collectionId"], request.Path["fieldId"]
	if !uid.MatchString(collection) {
		return "", "", status.Errorf(codes.InvalidArgument, "Invalid collection id %q", collection)
	}
	if !uid.MatchString(field) {
		return "", "", status.Errorf(codes.InvalidArgument, "Invalid field id %q", field)
	}
	return collection, field, nil
}

// fieldUpdate turns a field's TTL policy on or off as a long-running
// operation, as Terraform's google_firestore_field does with
// updateMask=ttlConfig. Index exemptions (indexConfig) are a gap.
func (p *Provider) fieldUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	collection, field, err := fieldPath(request)
	if err != nil {
		return nil, err
	}
	body := &adminpb.Field{}
	if err := provider.DecodeProto(request.JSON, "field", body, gap, "indexConfig"); err != nil {
		return nil, err
	}
	if err := ttlMask(request); err != nil {
		return nil, err
	}
	name := projectField(scope.Project, collection, field)
	change := "ADD"
	if body.GetTtlConfig() == nil {
		change = "REMOVE"
	}
	id, err := p.operations.Start(scope, name, change)
	if err != nil {
		return nil, err
	}
	now := p.clock()
	active := now.Add(operationLatency)
	var done map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		key := collection + "/" + field
		if body.GetTtlConfig() == nil {
			delete(s.fields, key)
		} else if s.fields[key] == nil {
			s.fields[key] = &fieldRecord{ActiveAt: active}
		}
		// The operation's response is the field once it finishes.
		done = fieldWire(scope.Project, collection, field, s.fields[key], active)
		return nil
	})
	done["@type"] = "type.googleapis.com/google.firestore.admin.v1.Field"
	p.operations.Finish(scope, id, done, nil)
	operation, err := p.operations.Get(scope, id)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: renameOperation(scope, operation)}, nil
}

// ttlMask refuses an update mask naming anything but the TTL policy.
func ttlMask(request *gateway.RESTRequest) error {
	for path := range strings.SplitSeq(request.Query.Get("updateMask"), ",") {
		if path != "" && path != "ttlConfig" && path != "ttl_config" {
			return gap("field updateMask " + strconv.Quote(path))
		}
	}
	return nil
}

func projectField(project, collection, field string) string {
	return databaseName(project) + "/collectionGroups/" + collection + "/fields/" + field
}

// fieldWire renders a field as GCP does (firestore-document-lifecycle): the
// automatic single-field indexes inherited from the database default, and a
// TTL policy that reads CREATING until its operation finishes.
func fieldWire(project, collection, field string, record *fieldRecord, now time.Time) map[string]any {
	index := func(key, value string) map[string]any {
		return map[string]any{"fields": []any{map[string]any{"fieldPath": field, key: value}}, "queryScope": "COLLECTION", keyState: "READY"}
	}
	document := map[string]any{
		"name": projectField(project, collection, field),
		"indexConfig": map[string]any{
			"ancestorField":      databaseName(project) + "/collectionGroups/__default__/fields/*",
			"indexes":            []any{index("order", "ASCENDING"), index("order", "DESCENDING"), index("arrayConfig", "CONTAINS")},
			"usesAncestorConfig": true,
		},
	}
	if record != nil {
		state := "ACTIVE"
		if now.Before(record.ActiveAt) {
			state = "CREATING"
		}
		document["ttlConfig"] = map[string]any{keyState: state}
	}
	return document
}

// expire deletes the scope's documents whose TTL field holds a timestamp at
// or before the provider clock, as GCP's TTL policies do (GCP deletes within
// 24 hours of expiry; this deletes before serving the next request). Only an
// ACTIVE policy deletes; a field that holds an array or another type is
// left alone.
func (p *Provider) expire(ctx context.Context, b driver, scope state.Scope) error {
	now := p.clock()
	client := pb.NewFirestoreClient(b.client())
	ctx = ownerContext(ctx, scope.Project)
	for _, group := range p.activeTTL(scope, now) {
		names, err := expired(ctx, client, scope.Project, group[0], group[1], now)
		if err != nil {
			return err
		}
		for start := 0; start < len(names); start += 500 {
			writes := []*pb.Write{}
			for _, name := range names[start:min(start+500, len(names))] {
				writes = append(writes, &pb.Write{Operation: &pb.Write_Delete{Delete: name}})
			}
			if _, err := client.Commit(ctx, &pb.CommitRequest{Database: databaseName(scope.Project), Writes: writes}); err != nil {
				return status.Errorf(codes.Unavailable, "Firestore TTL deletion failed: %v", err)
			}
		}
	}
	return nil
}

// activeTTL lists the collection group and field of every ACTIVE TTL policy.
func (p *Provider) activeTTL(scope state.Scope, now time.Time) [][2]string {
	var groups [][2]string
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for key, record := range s.fields {
			if !now.Before(record.ActiveAt) {
				collection, field, _ := strings.Cut(key, "/")
				groups = append(groups, [2]string{collection, field})
			}
		}
		return nil
	})
	return groups
}

// expired names the documents of one collection group whose field is a
// timestamp at or before now.
func expired(ctx context.Context, client pb.FirestoreClient, project, collection, field string, now time.Time) ([]string, error) {
	stream, err := client.RunQuery(ctx, &pb.RunQueryRequest{
		Parent: databaseName(project) + "/documents",
		QueryType: &pb.RunQueryRequest_StructuredQuery{StructuredQuery: &pb.StructuredQuery{
			From: []*pb.StructuredQuery_CollectionSelector{{CollectionId: collection, AllDescendants: true}},
			Where: &pb.StructuredQuery_Filter{FilterType: &pb.StructuredQuery_Filter_FieldFilter{FieldFilter: &pb.StructuredQuery_FieldFilter{
				Field: &pb.StructuredQuery_FieldReference{FieldPath: field}, Op: pb.StructuredQuery_FieldFilter_LESS_THAN_OR_EQUAL,
				Value: &pb.Value{ValueType: &pb.Value_TimestampValue{TimestampValue: timestamppb.New(now)}},
			}}},
		}},
	})
	if err != nil {
		return nil, status.Errorf(codes.Unavailable, "Firestore TTL query failed: %v", err)
	}
	var names []string
	for {
		response, err := stream.Recv()
		if errors.Is(err, io.EOF) {
			return names, nil
		}
		if err != nil {
			return nil, status.Errorf(codes.Unavailable, "Firestore TTL query failed: %v", err)
		}
		if name := response.GetDocument().GetName(); name != "" {
			names = append(names, name)
		}
	}
}
