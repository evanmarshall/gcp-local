package secretmanager

import (
	"cmp"
	"encoding/base64"
	"errors"
	"fmt"
	"hash/crc32"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// maxPayload is the recorded cloud payload limit (64 KiB).
const maxPayload = 64 << 10

// castagnoli is the CRC32C table for payload checksums.
var castagnoli = crc32.MakeTable(crc32.Castagnoli)

// version states.
const (
	stateEnabled   = "ENABLED"
	stateDisabled  = "DISABLED"
	stateDestroyed = "DESTROYED"
)

// unmodeled are the Secret fields a create or update refuses when set.
// Customer-managed encryption would need payloads sealed with the named KMS
// key as the service agent; typed secrets enforce payload structure. Neither
// is stored and ignored.
var unmodeled = []string{
	"customerManagedEncryption", "replication.automatic.customerManagedEncryption",
	"replication.userManaged.replicas.customerManagedEncryption", "secretType",
}

// Recorded refusals (secretmanager-access-lifecycle).
var (
	// "update-stale-etag" and "disable-stale-etag".
	errStaleETag = status.Error(codes.FailedPrecondition, "The etag provided in the request does not match the resource's current etag. "+
		"Please retry the whole read-modify-write with exponential backoff.")
	// "update-no-mask".
	errNoMask = status.Error(codes.InvalidArgument, "Field [update_mask] is required.")
	// "update-replication", from automatic to user-managed.
	errReplicationChange = status.Error(codes.InvalidArgument, "Existing secret has automatic replication, but requested secret does not. "+
		"Updating secret replication is not supported.")
	// The converse and a replica change are modeled on it, not recorded.
	errUserManagedChange = status.Error(codes.InvalidArgument, "Existing secret has user managed replication, but requested secret does not. "+
		"Updating secret replication is not supported.")
	errReplicasChange = status.Error(codes.InvalidArgument, "Updating secret replication is not supported.")
	// "add-checksum-mismatch".
	errChecksum = status.Error(codes.InvalidArgument, "Provided SecretPayload.data crc32c does not match calculated crc32c.")
	// errBadToken marks Page's refusal, which invalidToken words with the token.
	errBadToken = errors.New("invalid page token")
)

// paging is every list's: a server-chosen default (the whole list here),
// at most 25000 per page, as documented. A negative size reads as unset
// (recorded "versions-negative-size").
var paging = provider.Paging{Default: 25000, Max: 25000, InvalidToken: errBadToken}

// page returns one page of ordered items and its list body, adding the
// totalSize GCP reports (recorded "versions-paged").
func page[T any](items []T, query url.Values, field string, render func(T) any) (map[string]any, error) {
	if size, err := strconv.Atoi(query.Get("pageSize")); err == nil && size < 0 {
		query = maps.Clone(query)
		query.Del("pageSize")
	}
	selected, next, err := provider.Page(items, query, paging, gap)
	if errors.Is(err, errBadToken) {
		// Recorded "versions-invalid-token" and "secrets-invalid-token".
		return nil, status.Errorf(codes.InvalidArgument, "page_token [%s] is invalid. Please retry with a valid token or retry the initial List request.", query.Get("pageToken"))
	}
	if err != nil {
		return nil, err
	}
	rendered := make([]any, 0, len(selected))
	for _, item := range selected {
		rendered = append(rendered, render(item))
	}
	body := provider.ListBody(field, rendered, next)
	if len(items) > 0 {
		body["totalSize"] = len(items)
	}
	return body, nil
}

// annotationKey is the recorded annotation key rule ("update-invalid-annotation").
var annotationKey = regexp.MustCompile(`^[a-z0-9A-Z]+([_.\-]*[a-z0-9A-Z]+)*$`)

// checkAnnotations refuses a key GCP refuses, in key order.
func checkAnnotations(annotations map[string]string) error {
	for _, key := range slices.Sorted(maps.Keys(annotations)) {
		if !annotationKey.MatchString(key) || utf8.RuneCountInString(key) >= 64 || len(key) >= 128 {
			return status.Errorf(codes.InvalidArgument, "[%s] must follow pattern [a-z0-9A-Z]+([_\\.\\-]*[a-z0-9A-Z]+)*), "+
				"be less than 64 characters, and must have a UTF encoding of less than 128 bytes", key)
		}
	}
	return nil
}

// execute dispatches one authorized method on the locked scope state; record
// is the addressed secret, nil for project-level methods.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodSecretsCreate:
		return p.create(call, request, scope, s)
	case methodSecretsList:
		return p.list(request, scope, s)
	case methodLocationsList:
		body, err := page([]string{"us-central1"}, request.Query, "locations", func(id string) any {
			return map[string]any{"locationId": id}
		})
		if err != nil {
			return nil, err
		}
		return &gateway.RESTResponse{JSON: body}, nil
	case methodSecretsGet, methodSecretsUpdate, methodSecretsDelete, methodSecretsGetIam, methodSecretsSetIam:
		return p.executeSecret(call, request, scope, s, record)
	default:
		return p.executeVersion(call, request, scope, s, record)
	}
}

// executeSecret dispatches the secret-level methods on a locked record.
func (p *Provider) executeSecret(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodSecretsGet:
		return &gateway.RESTResponse{JSON: secretDocument(p.wireProject(scope.Project), record)}, nil
	case methodSecretsUpdate:
		return p.update(call, request, scope, s, record)
	case methodSecretsDelete:
		return p.delete(request, scope, s, record)
	case methodSecretsGetIam:
		return p.getIam(scope, record)
	default:
		return p.setIam(request, scope, record)
	}
}

// executeVersion dispatches the version-level methods on a locked record.
func (p *Provider) executeVersion(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodVersionsAdd:
		return p.addVersion(request, scope, s, record)
	case methodVersionsList:
		project := p.wireProject(scope.Project)
		body, err := page(sortedVersions(record), request.Query, "versions", func(version *versionRecord) any {
			return versionDocument(project, record, version)
		})
		if err != nil {
			return nil, err
		}
		return &gateway.RESTResponse{JSON: body}, nil
	case methodVersionsGet:
		return p.versionGet(request, scope, record)
	case methodVersionsAccess:
		return p.versionAccess(request, scope, record)
	case methodVersionsDestroy:
		return p.versionDestroy(request, scope, s, record)
	case methodVersionsDisable:
		return p.versionSetState(request, scope, s, record, stateDisabled, &secretmanagerpb.DisableSecretVersionRequest{})
	case methodVersionsEnable:
		return p.versionSetState(request, scope, s, record, stateEnabled, &secretmanagerpb.EnableSecretVersionRequest{})
	default:
		return nil, gap("this method is not implemented")
	}
}

// secretDocument renders the secret wire shape with its set lifecycle
// policies.
func secretDocument(project string, record *secretRecord) map[string]any {
	document := map[string]any{
		keyName:          secretWireName(project, record.Name),
		fieldReplication: replicationDocument(record.Replicas),
		"createTime":     provider.Stamp(record.CreatedAt),
		"etag":           etag(record.CreatedAt, record.Revision),
	}
	if len(record.Labels) > 0 {
		document["labels"] = maps.Clone(record.Labels)
	}
	if len(record.Annotations) > 0 {
		document["annotations"] = maps.Clone(record.Annotations)
	}
	addPolicies(document, record.policies)
	return document
}

// versionDocument renders one version wire shape without its payload.
func versionDocument(project string, record *secretRecord, version *versionRecord) map[string]any {
	document := map[string]any{
		"name":              versionWireName(project, record.Name, version.Number),
		"state":             version.State,
		"replicationStatus": replicationDocument(record.Replicas),
		"createTime":        provider.Stamp(version.CreatedAt),
		"etag":              etag(version.CreatedAt, version.Revision),
	}
	if version.State == stateDestroyed {
		document["destroyTime"] = provider.Stamp(version.DestroyedAt)
	}
	if !version.ScheduledDestroy.IsZero() {
		document["scheduledDestroyTime"] = provider.Stamp(version.ScheduledDestroy)
	}
	if version.ClientChecksum {
		document["clientSpecifiedPayloadChecksum"] = true
	}
	return document
}

// etag renders a quoted revision tag that changes with each mutation.
func etag(created time.Time, revision int) string {
	return strconv.Quote(strconv.FormatInt(created.UnixNano()+int64(revision), 16))
}

// checkETag refuses a request etag that isn't the resource's current one;
// an empty etag skips the check.
func checkETag(given, current string) error {
	if given != "" && given != current {
		return errStaleETag
	}
	return nil
}

// versionOf resolves a version number or the "latest" alias (newest
// enabled), and with aliases a version alias, which GCP resolves only for get
// and access.
func versionOf(record *secretRecord, id string, aliases bool) *versionRecord {
	if number, ok := record.Aliases[id]; ok && aliases {
		return record.Versions[strconv.FormatInt(number, 10)]
	}
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

// sortedVersions returns the secret's versions newest-first (recorded
// "versions-order"). Version ids are decimal integers assigned in order, so
// they compare as numbers, not text.
func sortedVersions(record *secretRecord) []*versionRecord {
	ordered := slices.Collect(maps.Values(record.Versions))
	slices.SortFunc(ordered, func(a, b *versionRecord) int {
		return cmp.Compare(versionNumber(b.Number), versionNumber(a.Number))
	})
	return ordered
}

// versionNumber parses a stored version id; nextVersion assigns only integers.
func versionNumber(id string) int {
	number, _ := strconv.Atoi(id)
	return number
}

// decodeSecret strictly decodes a create or update body.
func decodeSecret(raw []byte) (*secretmanagerpb.Secret, error) {
	secret := &secretmanagerpb.Secret{}
	if err := provider.DecodeProto(raw, "secret", secret, gap, unmodeled...); err != nil {
		return nil, err
	}
	return secret, checkAnnotations(secret.GetAnnotations())
}

// create stores one secret in the locked scope state.
func (p *Provider) create(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, s *scopeState) (*gateway.RESTResponse, error) {
	body, err := decodeSecret(request.JSON)
	if err != nil {
		return nil, err
	}
	id, idErr := secretID(request, body.GetName())
	if idErr != nil {
		return nil, idErr
	}
	if body.GetReplication().GetAutomatic() == nil && body.GetReplication().GetUserManaged() == nil {
		return nil, status.Error(codes.InvalidArgument, "A secret requires a replication policy")
	}
	now := p.clock()
	settings, err := p.newPolicies(call, scope, body, now)
	if err != nil {
		return nil, err
	}
	if s.secrets[id] != nil {
		return provider.WireError(409, "ALREADY_EXISTS", "secret "+id+" already exists"), nil
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: secretResource(scope.Project, id), Service: serviceName,
		Kind: "secret", Parent: provider.ProjectResource(scope.Project), Scope: scope,
	}); err != nil {
		return nil, fmt.Errorf("index the secret: %w", err)
	}
	record := &secretRecord{
		policies: settings, Name: id, Labels: maps.Clone(body.GetLabels()), Annotations: maps.Clone(body.GetAnnotations()),
		Versions: map[string]*versionRecord{}, CreatedAt: now,
	}
	s.secrets[id] = record
	p.launch(scope, record)
	project := p.wireProject(scope.Project)
	document := secretDocument(project, record)
	s.notify(project, record, eventCreate, document, "")
	return &gateway.RESTResponse{JSON: document}, nil
}

// newPolicies reads and checks a created secret's lifecycle settings.
func (p *Provider) newPolicies(call gateway.Call, scope state.Scope, body *secretmanagerpb.Secret, now time.Time) (policies, error) {
	settings, err := readPolicies(body, now)
	if err == nil {
		err = checkNext(settings.Next, now)
	}
	if err == nil {
		err = checkPolicies(settings, nil)
	}
	if err == nil && len(settings.Topics) > 0 {
		err = p.checkTopics(call.Context, scope.Project, settings.Topics)
	}
	return settings, err
}

// list returns one page of the project's secrets in name order. Recorded
// "secrets-paged": a secret created later but named first lists first.
func (p *Provider) list(request *gateway.RESTRequest, scope state.Scope, s *scopeState) (*gateway.RESTResponse, error) {
	project := p.wireProject(scope.Project)
	body, err := page(slices.Sorted(maps.Keys(s.secrets)), request.Query, "secrets", func(name string) any {
		return secretDocument(project, s.secrets[name])
	})
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// Secret fields an update mask may name.
const (
	fieldLabels      = "labels"
	fieldAnnotations = "annotations"
	fieldReplication = "replication"
	fieldTopics      = "topics"
	fieldExpireTime  = "expireTime"
	fieldTTL         = "ttl"
	fieldRotation    = "rotation"
	fieldNext        = "rotation.nextRotationTime"
	fieldPeriod      = "rotation.rotationPeriod"
	fieldAliases     = "versionAliases"
	fieldDestroyTTL  = "versionDestroyTtl"
)

// maskFields maps each update mask path, camelCase or snake_case, to the
// field it names: the modeled ones and every top-level unmodeled one, whose
// clearing is a no-op since none is ever set.
var maskFields = func() map[string]string {
	fields := map[string]string{}
	modeled := []string{fieldLabels, fieldAnnotations, fieldReplication, fieldTopics, fieldExpireTime, fieldTTL,
		fieldRotation, fieldNext, fieldPeriod, fieldAliases, fieldDestroyTTL}
	for _, field := range append(modeled, unmodeled...) {
		if !strings.Contains(field, ".") || strings.HasPrefix(field, fieldRotation+".") {
			fields[field], fields[snake(field)] = field, field
		}
	}
	return fields
}()

// snake spells a camelCase field name in snake_case.
func snake(name string) string {
	var spelled strings.Builder
	for _, letter := range name {
		if 'A' <= letter && letter <= 'Z' {
			spelled.WriteByte('_')
			letter += 'a' - 'A'
		}
		spelled.WriteRune(letter)
	}
	return spelled.String()
}

// update applies the masked fields of one locked secret. The whole request
// is validated before anything changes.
func (p *Provider) update(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	body, err := decodeSecret(request.JSON)
	if err != nil {
		return nil, err
	}
	fields, err := maskOf(request.Query.Get("updateMask"))
	if err != nil {
		return nil, err
	}
	now := p.clock()
	parsed, err := readPolicies(body, now)
	if err != nil {
		return nil, err
	}
	if fields[fieldReplication] {
		if err := replicationChange(record.Replicas, body.GetReplication(), parsed.Replicas); err != nil {
			return nil, err
		}
	}
	if err := checkETag(body.GetEtag(), etag(record.CreatedAt, record.Revision)); err != nil {
		return nil, err
	}
	settings, err := p.patchPolicies(call, scope, record, fields, parsed, now)
	if err != nil {
		return nil, err
	}
	if fields[fieldLabels] {
		record.Labels = maps.Clone(body.GetLabels())
	}
	if fields[fieldAnnotations] {
		record.Annotations = maps.Clone(body.GetAnnotations())
	}
	record.policies = settings
	record.Revision++
	p.launch(scope, record)
	project := p.wireProject(scope.Project)
	document := secretDocument(project, record)
	s.notify(project, record, eventUpdate, document, "")
	return &gateway.RESTResponse{JSON: document}, nil
}

// maskOf reads an update mask into the fields it names.
func maskOf(mask string) (map[string]bool, error) {
	if mask == "" {
		return nil, errNoMask
	}
	fields := map[string]bool{}
	for path := range strings.SplitSeq(mask, ",") {
		field, ok := maskFields[strings.TrimSpace(path)]
		if !ok {
			return nil, status.Errorf(codes.InvalidArgument, "Invalid update_mask path %q", path)
		}
		fields[field] = true
	}
	return fields, nil
}

// replicationChange refuses a masked replication that differs from the
// stored one; replication is immutable.
func replicationChange(stored []string, requested *secretmanagerpb.Replication, replicas []string) error {
	switch {
	case stored == nil && requested.GetAutomatic() == nil:
		return errReplicationChange
	case stored != nil && requested.GetUserManaged() == nil:
		return errUserManagedChange
	case !slices.Equal(stored, replicas):
		return errReplicasChange
	}
	return nil
}

// policyFields copies each masked lifecycle field from a parsed body.
var policyFields = []struct {
	fields []string
	copy   func(settings *policies, parsed policies)
}{
	{[]string{fieldTopics}, func(s *policies, from policies) { s.Topics = from.Topics }},
	{[]string{fieldExpireTime, fieldTTL}, func(s *policies, from policies) { s.Expire = from.Expire }},
	{[]string{fieldRotation, fieldNext}, func(s *policies, from policies) { s.Next = from.Next }},
	{[]string{fieldRotation, fieldPeriod}, func(s *policies, from policies) { s.Period = from.Period }},
	{[]string{fieldAliases}, func(s *policies, from policies) { s.Aliases = from.Aliases }},
	{[]string{fieldDestroyTTL}, func(s *policies, from policies) { s.DestroyTTL = from.DestroyTTL }},
}

// patchPolicies returns the secret's settings with the masked lifecycle
// fields replaced, after checking the result. A newly set next rotation time
// must fall in the documented window; changed topics must be publishable.
func (p *Provider) patchPolicies(call gateway.Call, scope state.Scope, record *secretRecord, fields map[string]bool, parsed policies, now time.Time) (policies, error) {
	settings := record.clone()
	for _, field := range policyFields {
		if slices.ContainsFunc(field.fields, func(name string) bool { return fields[name] }) {
			field.copy(&settings, parsed)
		}
	}
	var err error
	if fields[fieldRotation] || fields[fieldNext] {
		err = checkNext(settings.Next, now)
	}
	if err == nil {
		err = checkPolicies(settings, record.Versions)
	}
	if err == nil && fields[fieldTopics] {
		err = p.checkTopics(call.Context, scope.Project, settings.Topics)
	}
	return settings, err
}

// delete removes one locked secret, its versions, and its index entry. A
// stale etag query refuses it.
func (p *Provider) delete(request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	if err := checkETag(request.Query.Get("etag"), etag(record.CreatedAt, record.Revision)); err != nil {
		return nil, err
	}
	if err := p.index.Remove(serviceName, scope, secretResource(scope.Project, record.Name)); err != nil {
		return nil, fmt.Errorf("unindex the secret: %w", err)
	}
	p.stop(scope, record.Name)
	s.deleted(p.wireProject(scope.Project), record, "REQUESTED")
	delete(s.secrets, record.Name)
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

// setIam stores one secret policy; conditional bindings need version 3.
func (p *Provider) setIam(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	decoded, err := provider.DecodePolicy(request.JSON, gap)
	if err != nil {
		return nil, err
	}
	policy, err := p.engine.SetPolicy(secretResource(scope.Project, record.Name), decoded)
	if err != nil {
		return nil, fmt.Errorf("write secret policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// addVersion stores one payload as the next version. A supplied CRC32C must
// match the payload, and the version then reports it.
func (p *Provider) addVersion(request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	var body secretmanagerpb.AddSecretVersionRequest
	if err := provider.DecodeProto(request.JSON, "", &body, gap); err != nil {
		return nil, err
	}
	payload := body.GetPayload().GetData()
	if len(payload) > maxPayload {
		return nil, status.Errorf(codes.InvalidArgument, "The payload exceeds %d bytes", maxPayload)
	}
	given := body.GetPayload().DataCrc32C
	if given != nil && uint32(*given) != crc32.Checksum(payload, castagnoli) { //nolint:gosec // a CRC32C carried in an int64
		return nil, errChecksum
	}
	version := &versionRecord{
		Number: nextVersion(record), Payload: payload, State: stateEnabled, CreatedAt: p.clock(), ClientChecksum: given != nil,
	}
	record.Versions[version.Number] = version
	return p.versionChanged(scope, s, record, version, eventVersionAdd)
}

// versionChanged notifies one version event and renders the version.
func (p *Provider) versionChanged(scope state.Scope, s *scopeState, record *secretRecord, version *versionRecord, kind string) (*gateway.RESTResponse, error) {
	project := p.wireProject(scope.Project)
	document := versionDocument(project, record, version)
	s.notify(project, record, kind, document, version.Number)
	return &gateway.RESTResponse{JSON: document}, nil
}

func (p *Provider) versionGet(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	version := versionOf(record, request.Path["version"], true)
	if version == nil {
		return nil, provider.NotFound("versions", request.Path["version"])
	}
	return &gateway.RESTResponse{JSON: versionDocument(p.wireProject(scope.Project), record, version)}, nil
}

// versionAccess returns one version's payload to a granted accessor.
func (p *Provider) versionAccess(request *gateway.RESTRequest, scope state.Scope, record *secretRecord) (*gateway.RESTResponse, error) {
	version := versionOf(record, request.Path["version"], true)
	if version == nil {
		return nil, provider.NotFound("versions", request.Path["version"])
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
			"data":       base64.StdEncoding.EncodeToString(version.Payload),
			"dataCrc32c": strconv.FormatUint(uint64(crc32.Checksum(version.Payload, castagnoli)), 10),
		},
	}}, nil
}

// etagged is a version state-change request message.
type etagged interface {
	proto.Message
	GetEtag() string
}

// versionDestroy permanently destroys one version's payload, or with a
// versionDestroyTtl disables it and schedules the destruction.
func (p *Provider) versionDestroy(request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord) (*gateway.RESTResponse, error) {
	var body secretmanagerpb.DestroySecretVersionRequest
	if err := provider.DecodeProto(request.JSON, "", &body, gap); err != nil {
		return nil, err
	}
	version := versionOf(record, request.Path["version"], false)
	if version == nil {
		return nil, provider.NotFound("versions", request.Path["version"])
	}
	if err := checkETag(body.GetEtag(), etag(version.CreatedAt, version.Revision)); err != nil {
		return nil, err
	}
	if version.State == stateDestroyed {
		return nil, status.Errorf(codes.FailedPrecondition, "Version %s of the secret is already destroyed", version.Number)
	}
	version.Revision++
	if record.DestroyTTL > 0 {
		if !version.ScheduledDestroy.IsZero() {
			return nil, status.Errorf(codes.FailedPrecondition, "Version %s of the secret is already scheduled for destruction", version.Number)
		}
		version.State, version.ScheduledDestroy = stateDisabled, p.clock().Add(record.DestroyTTL)
		p.launch(scope, record)
		return p.versionChanged(scope, s, record, version, eventVersionScheduled)
	}
	version.State = stateDestroyed
	version.Payload = nil
	version.DestroyedAt = p.clock()
	return p.versionChanged(scope, s, record, version, eventVersionDestroy)
}

// nextVersion allocates the next version number of a secret.
func nextVersion(record *secretRecord) string {
	highest := 0
	for number := range record.Versions {
		highest = max(highest, versionNumber(number))
	}
	return strconv.Itoa(highest + 1)
}

// versionSetState disables or re-enables one version; body is the method's
// request message, whose etag must be current when set. Enabling a version
// scheduled for destruction cancels the destruction.
func (p *Provider) versionSetState(request *gateway.RESTRequest, scope state.Scope, s *scopeState, record *secretRecord, state string, body etagged) (*gateway.RESTResponse, error) {
	if err := provider.DecodeProto(request.JSON, "", body, gap); err != nil {
		return nil, err
	}
	version := versionOf(record, request.Path["version"], false)
	if version == nil {
		return nil, provider.NotFound("versions", request.Path["version"])
	}
	if err := checkETag(body.GetEtag(), etag(version.CreatedAt, version.Revision)); err != nil {
		return nil, err
	}
	if version.State == stateDestroyed {
		return nil, status.Errorf(codes.InvalidArgument, "Version %s of the secret is destroyed", version.Number)
	}
	version.State = state
	version.Revision++
	if state == stateEnabled {
		version.ScheduledDestroy = time.Time{}
		p.launch(scope, record)
		return p.versionChanged(scope, s, record, version, eventVersionEnable)
	}
	return p.versionChanged(scope, s, record, version, eventVersionDisable)
}

// secretID resolves the secret id from the path, the body name, or the query.
func secretID(request *gateway.RESTRequest, name string) (string, error) {
	id := request.Path["secret"]
	if id == "" {
		id = provider.LastSegment(name)
	}
	if id == "" {
		id = request.Query.Get("secretId")
	}
	if !uidPattern.MatchString(id) {
		return "", status.Errorf(codes.InvalidArgument, "Invalid secret name %q", id)
	}
	return id, nil
}
