package firestore

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/url"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized Firestore request inside the scope's
// backend operation. Documents belong to the emulator; collection-group field
// configuration is the Admin API, which the emulator does not serve, so the
// provider keeps it.
func (p *Provider) execute(ctx context.Context, b driver, call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodFieldsGet:
		return p.fieldGet(request, scope)
	case methodFieldsUpdate:
		return p.fieldUpdate(request, scope)
	default:
		return forward(ctx, b, call.Method, request)
	}
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
func forward(ctx context.Context, b driver, method string, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var body io.Reader
	if method == methodDocumentsCreate || method == methodDocumentsUpdate {
		body = bytes.NewReader(request.JSON)
	}
	outbound, err := http.NewRequestWithContext(ctx, httpVerb(method), b.baseURL()+backendPath(request), body)
	if err != nil {
		return nil, status.Error(codes.Internal, "Unable to build the emulator request")
	}
	outbound.Header.Set("Authorization", "Bearer owner")
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

// backendPath rebuilds the documents path and its allowed query parameters.
func backendPath(request *gateway.RESTRequest) string {
	path := "/v1/projects/" + url.PathEscape(request.Path["project"]) + "/databases/(default)/documents/" + url.PathEscape(request.Path["collectionId"])
	if id, ok := request.Path["documentId"]; ok {
		path += "/" + url.PathEscape(id)
	}
	query := url.Values{}
	for key, values := range request.Query {
		if forwardedQuery[key] {
			query[key] = values
		}
	}
	if len(query) != 0 {
		path += "?" + query.Encode()
	}
	return path
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
	p.ensureDatabase(scope)
	collection, field := request.Path["collectionId"], request.Path["fieldId"]
	if !uid.MatchString(collection) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid collection id %q", collection)
	}
	if !uid.MatchString(field) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid field id %q", field)
	}
	record := p.fieldLocked(scope, collection, field)
	if record == nil {
		// Real Firestore reports an unconfigured field with fully automatic
		// index settings rather than a 404.
		return &gateway.RESTResponse{JSON: fieldWire(projectField(scope.Project, collection, field), nil, nil, false)}, nil
	}
	return &gateway.RESTResponse{JSON: fieldWire(record.Name, record.TTLConfig, record.IndexConfig, record.Configured)}, nil
}

// fieldUpdate patches one field's TTL or index configuration.
func (p *Provider) fieldUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	p.ensureDatabase(scope)
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
	var record *fieldRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		record = s.fields[collection+"/"+field]
		if record == nil {
			record = &fieldRecord{Name: projectField(scope.Project, collection, field), CreatedAt: now}
			s.fields[collection+"/"+field] = record
		}
		record.TTLConfig, record.IndexConfig = ttl, index
		record.Configured = true
		return nil
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"done":     true,
		"response": fieldWire(record.Name, record.TTLConfig, record.IndexConfig, record.Configured),
	}}, nil
}

// fieldLocked reads one field under the scope lock.
func (p *Provider) fieldLocked(scope state.Scope, collection, field string) *fieldRecord {
	var record *fieldRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.fields[collection+"/"+field]; return nil })
	return record
}

// projectField renders the canonical field name.
func projectField(project, collection, field string) string {
	return "projects/" + project + "/databases/(default)/collectionGroups/" + collection + "/fields/" + field
}

// fieldWire renders one field document.
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
