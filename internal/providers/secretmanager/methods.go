package secretmanager

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"hash/crc32"
	"maps"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxPayload is the recorded cloud payload limit (64 KiB).
const maxPayload = 64 << 10

// castagnoli is the CRC32C table for payload checksums.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// labelsOf copies a wire label map.
func labelsOf(value any) map[string]string {
	labels := map[string]string{}
	if values, ok := value.(map[string]any); ok {
		for key, label := range values {
			labels[key] = textOf(label)
		}
	}
	return labels
}

// version states.
const (
	stateEnabled   = "ENABLED"
	stateDisabled  = "DISABLED"
	stateDestroyed = "DESTROYED"
)

// execute dispatches one authorized method.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodSecretsCreate, methodSecretsGet, methodSecretsList, methodSecretsUpdate,
		methodSecretsDelete, methodSecretsGetIam, methodSecretsSetIam:
		return p.executeSecret(call, request, scope, record)
	case methodVersionsAdd, methodVersionsList, methodVersionsGet, methodVersionsAccess, methodVersionsDestroy,
		methodVersionsDisable, methodVersionsEnable:
		return p.executeVersion(call, request, scope, record)
	case methodLocationsList:
		return &gateway.RESTResponse{JSON: map[string]any{"locations": []any{
			map[string]any{"locationId": "us-central1"},
		}}}, nil
	default:
		return nil, gap("this method is not implemented")
	}
}

// executeSecret dispatches the secret-level methods.
func (p *Provider) executeSecret(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodSecretsCreate:
		return p.create(request, scope)
	case methodSecretsGet:
		return p.get(scope, record)
	case methodSecretsList:
		return p.list(scope)
	case methodSecretsUpdate:
		return p.update(request, scope, record)
	case methodSecretsDelete:
		return p.delete(request, scope, record)
	case methodSecretsGetIam:
		return p.getIam(scope, record)
	case methodSecretsSetIam:
		return p.setIam(request, scope, record)
	default:
		return nil, gap("this method is not implemented")
	}
}

// executeVersion dispatches the version-level methods.
func (p *Provider) executeVersion(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodVersionsAdd:
		return p.addVersion(request, scope, record)
	case methodVersionsList:
		return p.versionsList(scope, record)
	case methodVersionsGet:
		return p.versionGet(request, scope, record)
	case methodVersionsAccess:
		return p.versionAccess(request, scope, record)
	case methodVersionsDestroy:
		return p.versionDestroy(request, scope, record)
	case methodVersionsDisable:
		return p.versionSetState(request, scope, record, stateDisabled)
	case methodVersionsEnable:
		return p.versionSetState(request, scope, record, stateEnabled)
	default:
		return nil, gap("this method is not implemented")
	}
}

// decodeBody reads one request body as a plain map.
func decodeBody(body []byte) (map[string]any, error) {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid request body")
	}
	return value, nil
}

// textOf reads a string field as empty-safe text.
func textOf(value any) string {
	if value, ok := value.(string); ok {
		return value
	}
	return ""
}

// idOfName extracts the trailing identifier of a full resource name.
func idOfName(name string) string {
	if index := strings.LastIndex(name, "/"); index >= 0 {
		return name[index+1:]
	}
	return name
}

// secretDocument renders the secret wire shape.
func secretDocument(project string, record *secretRecord) map[string]any {
	labels := map[string]string{}
	maps.Copy(labels, record.Labels)
	return map[string]any{
		keyName:       secretWireName(project, record.Name),
		"replication": record.Replication,
		"labels":      labels,
		"createTime":  stamp(record.CreatedAt),
		"etag":        etag(record.CreatedAt, record.Revision),
	}
}

// versionDocument renders one version wire shape without its payload.
func versionDocument(project string, record *secretRecord, version *versionRecord) map[string]any {
	document := map[string]any{
		"name":              versionWireName(project, record.Name, version.Number),
		"state":             version.State,
		"replicationStatus": map[string]any{"automatic": map[string]any{}},
		"createTime":        stamp(version.CreatedAt),
		"etag":              etag(version.CreatedAt, version.Revision),
	}
	if version.State == stateDestroyed {
		document["destroyTime"] = stamp(version.DestroyedAt)
	}
	return document
}

// etag renders a quoted revision tag that changes with each mutation.
func etag(created time.Time, revision int) string {
	return strconv.Quote(strconv.FormatInt(created.UnixNano()+int64(revision), 16))
}

// versionOf resolves a version number or the "latest" alias (newest enabled).
func versionOf(record *secretRecord, id string) *versionRecord {
	if id != "latest" {
		return record.Versions[id]
	}
	for _, version := range sortedVersions(record) {
		if version.State == stateEnabled {
			return version
		}
	}
	return nil
}

// sortedVersions returns the secret's versions newest-first.
func sortedVersions(record *secretRecord) []*versionRecord {
	numbers := make([]string, 0, len(record.Versions))
	for number := range record.Versions {
		numbers = append(numbers, number)
	}
	sort.Sort(sort.Reverse(sort.StringSlice(numbers)))
	ordered := make([]*versionRecord, 0, len(numbers))
	for _, number := range numbers {
		ordered = append(ordered, record.Versions[number])
	}
	return ordered
}

// create stores one secret.
func (p *Provider) create(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeBody(request.JSON)
	if err != nil {
		return nil, err
	}
	id, idErr := secretID(request, body)
	if idErr != nil {
		return nil, idErr
	}
	replication, ok := body["replication"].(map[string]any)
	if !ok || len(replication) == 0 {
		return nil, status.Error(codes.InvalidArgument, "A secret requires a replication policy")
	}
	now := p.clock()
	var conflict *gateway.RESTResponse
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.secrets[id] != nil {
			conflict = wireError(409, "ALREADY_EXISTS", "secret "+id+" already exists")
			return nil
		}
		s.secrets[id] = &secretRecord{
			Name: id, Replication: replication, Labels: labelsOf(body["labels"]),
			Versions: map[string]*versionRecord{}, CreatedAt: now,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("store the secret: %w", err)
	}
	if conflict != nil {
		return conflict, nil
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: secretResource(scope.Project, id), Service: serviceName,
		Kind: "secret", Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the secret: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: secretDocument(p.wireProject(scope.Project), p.mustSecret(scope, id))}, nil
}

// mustSecret reads one secret record under the scope lock.
func (p *Provider) mustSecret(scope state.Scope, id string) *secretRecord {
	var record *secretRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.secrets[id]; return nil })
	return record
}

// get returns one secret document.
func (p *Provider) get(scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: secretDocument(p.wireProject(scope.Project), record)}, nil
}

// list returns the project's secrets.
func (p *Provider) list(scope state.Scope) (*gateway.RESTResponse, error) {
	var names []string
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.secrets {
			names = append(names, name)
		}
		return nil
	})
	sort.Strings(names)
	items := make([]any, 0, len(names))
	for _, name := range names {
		items = append(items, secretDocument(p.wireProject(scope.Project), p.mustSecret(scope, name)))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"secrets": items}}, nil
}

// update replaces the labels of one secret.
func (p *Provider) update(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	body, err := decodeBody(request.JSON)
	if err != nil {
		return nil, err
	}
	if labels, ok := body["labels"].(map[string]any); ok {
		record.Labels = labelsOf(labels)
		record.Revision++
	}
	return &gateway.RESTResponse{JSON: secretDocument(p.wireProject(scope.Project), record)}, nil
}

// delete removes one secret and all its versions.
func (p *Provider) delete(_ *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	name := record.Name
	if err := p.scopes.With(scope, func(s *scopeState) error {
		delete(s.secrets, name)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("clear the secret: %w", err)
	}
	if err := p.index.Remove(serviceName, scope, secretResource(scope.Project, name)); err != nil {
		return nil, fmt.Errorf("unindex the secret: %w", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// getIam returns the stored secret policy.
func (p *Provider) getIam(scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(secretResource(scope.Project, record.Name))
	if err != nil {
		return nil, fmt.Errorf("read secret policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setIam stores one unconditional version-1 secret policy.
func (p *Provider) setIam(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(secretResource(scope.Project, record.Name), envelope.Policy)
	if err != nil {
		return nil, fmt.Errorf("write secret policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// addVersion stores one payload as the next version.
func (p *Provider) addVersion(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	var envelope struct {
		Payload struct {
			Data string `json:"data"`
		} `json:"payload"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid version payload")
	}
	payload, err := base64Decode(envelope.Payload.Data)
	if err != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid base64 payload")
	}
	if len(payload) > maxPayload {
		return nil, status.Errorf(codes.InvalidArgument, "The payload exceeds %d bytes", maxPayload)
	}
	version := &versionRecord{Number: nextVersion(record), Payload: payload, State: stateEnabled, CreatedAt: p.clock()}
	record.Versions[version.Number] = version
	return &gateway.RESTResponse{JSON: versionDocument(p.wireProject(scope.Project), record, version)}, nil
}

// versionsList returns the secret's versions newest-first.
func (p *Provider) versionsList(scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	versions := sortedVersions(record)
	items := make([]any, 0, len(versions))
	for _, version := range versions {
		items = append(items, versionDocument(p.wireProject(scope.Project), record, version))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"versions": items}}, nil
}

// versionGet returns one version document.
func (p *Provider) versionGet(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	version := versionOf(record, request.Path["version"])
	if version == nil {
		return nil, notFound("versions", request.Path["version"])
	}
	return &gateway.RESTResponse{JSON: versionDocument(p.wireProject(scope.Project), record, version)}, nil
}

// versionAccess returns one version's payload to a granted accessor.
func (p *Provider) versionAccess(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	version := versionOf(record, request.Path["version"])
	if version == nil {
		return nil, notFound("versions", request.Path["version"])
	}
	if version.State == stateDestroyed {
		return nil, status.Errorf(codes.FailedPrecondition, "Version %s of the secret is destroyed", version.Number)
	}
	if version.State == stateDisabled {
		return nil, status.Errorf(codes.FailedPrecondition, "Version %s of the secret is disabled", version.Number)
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		"name": versionWireName(p.wireProject(scope.Project), record.Name, version.Number),
		"payload": map[string]any{
			"data":       base64Encode(version.Payload),
			"dataCrc32c": strconv.FormatUint(uint64(crc32.Checksum(version.Payload, castagnoli)), 10),
		},
	}}, nil
}

// versionDestroy permanently destroys one version's payload.
func (p *Provider) versionDestroy(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	version := versionOf(record, request.Path["version"])
	if version == nil {
		return nil, notFound("versions", request.Path["version"])
	}
	if version.State == stateDestroyed {
		return nil, status.Errorf(codes.FailedPrecondition, "Version %s of the secret is already destroyed", version.Number)
	}
	version.State = stateDestroyed
	version.Payload = nil
	version.DestroyedAt = p.clock()
	version.Revision++
	return &gateway.RESTResponse{JSON: versionDocument(p.wireProject(scope.Project), record, version)}, nil
}

// nextVersion allocates the next version number of a secret.
func nextVersion(record *secretRecord) string {
	highest := 0
	for number := range record.Versions {
		if parsed, err := strconv.Atoi(number); err == nil && parsed > highest {
			highest = parsed
		}
	}
	return strconv.Itoa(highest + 1)
}

// base64Decode decodes the wire base64 payload.
func base64Decode(value string) ([]byte, error) {
	if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
		return decoded, nil
	}
	decoded, urlErr := base64.URLEncoding.DecodeString(value)
	if urlErr != nil {
		return nil, fmt.Errorf("decode base64 payload: %w", urlErr)
	}
	return decoded, nil
}

// base64Encode encodes one payload to the wire base64 shape.
func base64Encode(payload []byte) string { return base64.StdEncoding.EncodeToString(payload) }

// versionSetState disables or re-enables one version.
func (p *Provider) versionSetState(request *gateway.RESTRequest, scope state.Scope, record *secretRecord, state string) (*gateway.RESTResponse, error) {
	version := versionOf(record, request.Path["version"])
	if version == nil {
		return nil, notFound("versions", request.Path["version"])
	}
	if version.State == stateDestroyed {
		return nil, status.Errorf(codes.InvalidArgument, "Version %s of the secret is destroyed", version.Number)
	}
	version.State = state
	version.Revision++
	return &gateway.RESTResponse{JSON: versionDocument(p.wireProject(scope.Project), record, version)}, nil
}

// secretID resolves the secret id from the path, the body name, or the query.
func secretID(request *gateway.RESTRequest, body map[string]any) (string, error) {
	id := request.Path["secret"]
	if id == "" {
		id = idOfName(textOf(body["name"]))
	}
	if id == "" {
		id = request.Query.Get("secretId")
	}
	if !uidPattern.MatchString(id) {
		return "", status.Errorf(codes.InvalidArgument, "Invalid secret name %q", id)
	}
	return id, nil
}
