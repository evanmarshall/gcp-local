package run

import (
	"cmp"
	"context"
	"fmt"
	"github.com/evanmarshall/gcp-local/internal/providers/run/container"
	"github.com/evanmarshall/gcp-local/internal/providers/run/metadata"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kindService is the resource index kind of a service.
const kindService = "service"

// methodNames lists every registered method.
func methodNames() []string {
	return []string{
		methodServicesCreate, methodServicesGet, methodServicesList,
		methodServicesUpdate, methodServicesDelete,
		methodServicesGetIam, methodServicesSetIam,
		methodRevisionsList, methodRevisionsGet, methodRevisionsDelete,
		methodOperationsGet, methodOperationsList, methodOperationsDelete, methodOperationsWait,
	}
}

// revisionRecord is one stored revision of a service.
type revisionRecord struct {
	ID string
	// Generation is the service generation that created the revision; it
	// orders revisions, whose names a caller may choose.
	Generation      int64
	Template        templateShape
	CreatedAt       time.Time
	Container       string
	Address         string
	releaseMetadata func()
	lastActivity    time.Time
	// reaped marks a container the idle reaper stopped; the next invocation
	// cold-starts it. starting is closed when a cold start in flight ends.
	reaped   bool
	starting chan struct{}
	// inflight counts the requests the revision is serving now; its scaling
	// caps it.
	inflight int
}

// serviceRecord is one stored service of a scope.
type serviceRecord struct {
	Name       string
	UID        string
	Generation int64
	Spec       serviceSpec
	Creator    string
	Modifier   string
	Revisions  map[string]*revisionRecord
	// Created counts the revisions created; it numbers the next one, as GCP
	// does (recording run-traffic).
	Created int
	// Ready is the newest revision whose rollout succeeded; LATEST traffic
	// targets resolve to it.
	Ready string
	// Statuses is the routing in effect: the traffic block of the newest
	// rollout that succeeded, resolved. A failed rollout leaves it as it was
	// (recording run-traffic).
	Statuses []trafficStatus
	// Rollout is the operation ID of the newest rollout; only it may change
	// Ready and the condition. Reconciling holds while it runs.
	Rollout     string
	Reconciling bool
	// Condition and Detail are the newest finished rollout's outcome.
	Condition, Detail    string
	CreatedAt, UpdatedAt time.Time
}

// scopeState is one project/location slice of provider state; operations
// live in the provider's operation store.
type scopeState struct {
	services map[string]*serviceRecord
}

func newScopeState() scopeState {
	return scopeState{services: map[string]*serviceRecord{}}
}

func (s *scopeState) reset() {
	s.services = map[string]*serviceRecord{}
}

// Any type URLs of the resources Cloud Run v2 operations carry as metadata
// and response.
const (
	typeService  = "type.googleapis.com/google.cloud.run.v2.Service"
	typeRevision = "type.googleapis.com/google.cloud.run.v2.Revision"
)

// Condition states of google.cloud.run.v2.Condition.
const (
	conditionPending   = "CONDITION_PENDING"
	conditionSucceeded = "CONDITION_SUCCEEDED"
	conditionFailed    = "CONDITION_FAILED"
)

// wireName is the relative resource name the v2 API returns, such as
// projects/p/locations/l/services/s. IAM targets use resource instead.
func wireName(scope state.Scope, path ...string) string {
	return "projects/" + scope.Project + "/locations/" + scope.Location + "/" + strings.Join(path, "/")
}

// execute dispatches one authorized method to its resource family. Each
// method looks the service up again under the scope lock; one deleted since
// authorization reads NOT_FOUND.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodServicesCreate, methodServicesGet, methodServicesList, methodServicesUpdate,
		methodServicesDelete, methodServicesGetIam, methodServicesSetIam:
		return p.executeService(call, request, scope, id)
	case methodRevisionsList, methodRevisionsGet, methodRevisionsDelete:
		return p.executeRevision(call, request, scope, id)
	case methodOperationsGet, methodOperationsList, methodOperationsDelete, methodOperationsWait:
		return p.executeOperations(call, request, scope)
	default:
		return nil, errUnimplemented
	}
}

// executeService dispatches the service methods.
func (p *Provider) executeService(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodServicesCreate:
		return p.create(call, request, scope)
	case methodServicesGet:
		return p.get(scope, id)
	case methodServicesList:
		return p.list(request, scope)
	case methodServicesUpdate:
		return p.update(call, request, scope, id)
	case methodServicesDelete:
		return p.delete(call, scope, id)
	case methodServicesGetIam:
		return p.getIam(scope, id)
	case methodServicesSetIam:
		return p.setIam(request, scope, id)
	default:
		return nil, errUnimplemented
	}
}

// executeRevision dispatches the revision methods.
func (p *Provider) executeRevision(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodRevisionsList:
		return p.revisionsList(request, scope, id)
	case methodRevisionsGet:
		return p.revisionGet(request, scope, id)
	case methodRevisionsDelete:
		return p.revisionDelete(call, request, scope, id)
	default:
		return nil, errUnimplemented
	}
}

// executeOperations dispatches the google.longrunning methods Cloud Run
// serves (run_v2.yaml binds list, get, delete, and wait; not cancel).
func (p *Provider) executeOperations(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Path["operation"]
	switch call.Method {
	case methodOperationsGet:
		return p.operation(scope, id)
	case methodOperationsList:
		body, err := p.operations.List(scope, request.Query, paging, gap)
		if err != nil {
			return nil, err
		}
		items, _ := body["operations"].([]map[string]any)
		for _, item := range items {
			omitPending(item)
		}
		return &gateway.RESTResponse{JSON: body}, nil
	case methodOperationsDelete:
		if err := p.operations.Delete(scope, id); err != nil {
			return nil, err
		}
		return &gateway.RESTResponse{JSON: map[string]any{}}, nil
	case methodOperationsWait:
		return p.wait(call, request, scope, id)
	default:
		return nil, errUnimplemented
	}
}

// maxWait bounds operations.wait; an unset timeout waits this long.
const maxWait = 2 * time.Minute

// wait blocks until the operation is done or its timeout passes.
func (p *Provider) wait(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var body longrunningpb.WaitOperationRequest
	if err := provider.DecodeProto(request.JSON, "", &body, gap); err != nil {
		return nil, err
	}
	timeout := maxWait
	if body.GetTimeout() != nil {
		timeout = min(max(body.GetTimeout().AsDuration(), 0), maxWait)
	}
	ctx := call.Context
	if ctx == nil {
		ctx = context.Background()
	}
	document, err := p.operations.Wait(ctx, scope, id, timeout)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: omitPending(document)}, nil
}

// operation renders one operation.
func (p *Provider) operation(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	document, err := p.operations.Get(scope, id)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: omitPending(document)}, nil
}

// omitPending drops done=false: Cloud Run's JSON omits the proto default
// (recording run-service-lifecycle).
func omitPending(document map[string]any) map[string]any {
	if document["done"] == false {
		delete(document, "done")
	}
	return document
}

// operationMetadata renders an operation's metadata: the resource it acts on,
// as Cloud Run v2 does, at the start and as finished once done.
func operationMetadata(operation provider.Operation) map[string]any {
	if operation.Done && operation.Response != nil {
		return operation.Response
	}
	return operation.Metadata
}

// paging is the list methods' page sizes and recorded refusals: an unknown
// page token is GCP's 500 INTERNAL, and a negative size reads as the default
// (recording run-service-lifecycle). The sizes are not documented or recorded.
var paging = provider.Paging{Default: 100, Max: 1000,
	InvalidToken: status.Error(codes.Internal, "Internal error encountered."), NegativeDefault: true}

// sortedRevisions returns the service's revisions in creation order.
func sortedRevisions(record *serviceRecord) []*revisionRecord {
	ordered := slices.Collect(maps.Values(record.Revisions))
	slices.SortFunc(ordered, func(a, b *revisionRecord) int { return int(a.Generation - b.Generation) })
	return ordered
}

// latestRevision returns the newest revision of a service.
func latestRevision(record *serviceRecord) *revisionRecord {
	ordered := sortedRevisions(record)
	if len(ordered) == 0 {
		return nil
	}
	return ordered[len(ordered)-1]
}

// servingRevision is the revision that receives traffic: the newest ready
// one, or before any rollout succeeded, the newest.
func servingRevision(record *serviceRecord) *revisionRecord {
	if revision := record.Revisions[record.Ready]; revision != nil {
		return revision
	}
	return latestRevision(record)
}

// serviceDocument renders the Cloud Run v2 service wire shape. Enum values
// and names follow google.cloud.run.v2.Service, so gRPC clients can decode it.
func (p *Provider) serviceDocument(scope state.Scope, record *serviceRecord) map[string]any {
	generation := strconv.FormatInt(record.Generation, 10)
	uri := p.serviceURI(scope, record.Name)
	terminal := map[string]any{keyType: "Ready", keyState: record.Condition}
	if record.Detail != "" {
		terminal["message"] = record.Detail
	}
	document := map[string]any{
		keyName:              wireName(scope, "services", record.Name),
		"uid":                record.UID,
		"generation":         generation,
		"observedGeneration": generation,
		"createTime":         provider.Stamp(record.CreatedAt),
		"updateTime":         provider.Stamp(record.UpdatedAt),
		"creator":            record.Creator,
		"lastModifier":       record.Modifier,
		"launchStage":        record.Spec.LaunchStage,
		"ingress":            record.Spec.Ingress,
		"template":           templateDocument(record.Spec.Template),
		"traffic":            trafficDocuments(record.Spec.Traffic),
		"uri":                uri,
		"urls":               []any{uri},
		"terminalCondition":  terminal,
		"conditions": []any{
			map[string]any{keyType: "RoutesReady", keyState: record.Condition},
			map[string]any{keyType: "ConfigurationsReady", keyState: record.Condition},
		},
	}
	if latest := latestRevision(record); latest != nil {
		document["latestCreatedRevision"] = wireName(scope, "services", record.Name, "revisions", latest.ID)
	}
	if record.Ready != "" {
		document["latestReadyRevision"] = wireName(scope, "services", record.Name, "revisions", record.Ready)
	}
	if len(record.Statuses) > 0 {
		document["trafficStatuses"] = statusDocuments(record.Statuses, func(tag string) string {
			return p.serviceURI(scope, tag+tagSeparator+record.Name)
		})
	}
	if record.Reconciling {
		document["reconciling"] = true
	}
	optional(document, "description", record.Spec.Description)
	optional(document, "client", record.Spec.Client)
	optional(document, "clientVersion", record.Spec.ClientVersion)
	if len(record.Spec.Labels) > 0 {
		document["labels"] = maps.Clone(record.Spec.Labels)
	}
	if len(record.Spec.Annotations) > 0 {
		document["annotations"] = maps.Clone(record.Spec.Annotations)
	}
	return document
}

// optional sets a string field that proto3 JSON omits when empty.
func optional(document map[string]any, key, value string) {
	if value != "" {
		document[key] = value
	}
}

// templateDocument renders a revision template wire shape.
func templateDocument(template templateShape) map[string]any {
	// Stable order: a map-ordered echo makes declarative clients see drift.
	env := make([]any, 0, len(template.Container.Env))
	for _, key := range slices.Sorted(maps.Keys(template.Container.Env)) {
		env = append(env, map[string]any{"name": key, "value": template.Container.Env[key]})
	}
	document := map[string]any{
		"serviceAccount":                template.ServiceAccount,
		"maxInstanceRequestConcurrency": template.Scaling.concurrency(),
		"containers": []any{map[string]any{
			"image":     template.Container.Image,
			"ports":     []any{map[string]any{"containerPort": template.Container.Port, keyName: "http1"}},
			"env":       env,
			"resources": map[string]any{},
		}},
	}
	if template.RevisionNamed {
		document["revision"] = template.Revision
	}
	if scaling := template.Scaling.document(); len(scaling) > 0 {
		document["scaling"] = scaling
	}
	return document
}

// revisionDocument renders one google.cloud.run.v2.Revision wire shape.
func (p *Provider) revisionDocument(scope state.Scope, record *serviceRecord, revision *revisionRecord) map[string]any {
	ready, detail := conditionSucceeded, "The revision is ready"
	if revision.Container == "" && !revision.reaped {
		ready, detail = conditionFailed, "The revision container is not running"
	}
	template := templateDocument(revision.Template)
	document := map[string]any{
		keyName:          wireName(scope, "services", record.Name, "revisions", revision.ID),
		"service":        wireName(scope, "services", record.Name),
		"uid":            revision.ID + "-uid",
		"generation":     strconv.FormatInt(revision.Generation, 10),
		"createTime":     provider.Stamp(revision.CreatedAt),
		"containers":     template["containers"],
		"serviceAccount": revision.Template.ServiceAccount,
		"conditions":     []any{map[string]any{keyType: "ContainerReady", keyState: ready, "message": detail}},
		// The revision carries its template's scaling (recording run-traffic).
		"maxInstanceRequestConcurrency": template["maxInstanceRequestConcurrency"],
	}
	if scaling, ok := template["scaling"]; ok {
		document["scaling"] = scaling
	}
	return document
}

// launched is one started revision container and its metadata listener.
type launched struct {
	container, address string
	release            func()
}

// launchSpec is everything a revision launch needs, copied out of the store
// under its lock so the launch itself runs outside it.
type launchSpec struct {
	start   container.StartSpec
	account string
}

// launchSpecOf copies the launch inputs of one revision; call it under the
// scope lock.
func launchSpecOf(record *serviceRecord, revision *revisionRecord) launchSpec {
	return launchSpec{
		start: container.StartSpec{
			Image: revision.Template.Container.Image,
			Port:  revision.Template.Container.Port,
			Env:   workloadEnv(record, revision),
		},
		account: revision.Template.ServiceAccount,
	}
}

// launch starts one revision container with its metadata listener. It never
// touches the store: a launch can pull an image for minutes.
func (p *Provider) launch(spec launchSpec) (launched, error) {
	var gate *metadata.PeerGate
	var release func()
	if spec.account != "" && p.metadata != nil {
		subject := "serviceAccount:" + spec.account
		handler, metadataErr := p.metadata(subject)
		if metadataErr != nil {
			return launched{}, fmt.Errorf("enroll the workload identity %s: %w", subject, metadataErr)
		}
		gate = metadata.NewPeerGate(handler)
		hostPort, stop, bindErr := p.metabind(gate)
		if bindErr != nil {
			return launched{}, fmt.Errorf("bind the workload metadata listener: %w", bindErr)
		}
		release = stop
		spec.start.Env = append(spec.start.Env, "GCE_METADATA_HOST="+hostPort)
	}
	// The revision outlives the request that launched it.
	started, err := p.runtime.Start(context.Background(), spec.start)
	if err != nil {
		if release != nil {
			release()
		}
		return launched{}, fmt.Errorf("start the revision container: %w", err)
	}
	if gate != nil {
		gate.Allow(started.Peer)
	}
	return launched{container: started.Container, address: started.Address, release: release}, nil
}

// discard stops a launched container the store no longer wants.
func (p *Provider) discard(started launched) {
	_ = p.runtime.Stop(context.Background(), started.container)
	if started.release != nil {
		started.release()
	}
}

// attach records a launched container on its revision; call it under the
// scope lock.
func (p *Provider) attach(revision *revisionRecord, started launched) {
	revision.Container = started.container
	revision.Address = started.address
	revision.releaseMetadata = started.release
	revision.reaped = false
	revision.lastActivity = p.clock()
}

// idleTimeout is how long a revision container may sit without an invocation
// before the provider stops it; the next invocation cold-starts it again.
const idleTimeout = 5 * time.Minute

// startReaper schedules the idle reaper for a service, replacing an earlier
// one. Reset and delete cancel it; a reaped or missing service ends it.
func (p *Provider) startReaper(scope state.Scope, id string) {
	watch, cancel := context.WithCancel(context.Background())
	p.reaperMu.Lock()
	if previous := p.reapers[reaperKey(scope, id)]; previous != nil {
		previous()
	}
	p.reapers[reaperKey(scope, id)] = cancel
	p.reaperMu.Unlock()
	go func() {
		defer cancel()
		for range time.Tick(time.Minute) {
			select {
			case <-watch.Done():
				return
			default:
			}
			if reaped := p.reap(watch, scope, id); reaped {
				return
			}
		}
	}()
}

// reap stops every revision container of the service that has been idle
// past the timeout and serves no request, except a revision whose minimum
// instances keep it warm. It reports whether the reaper is done: no
// container of the service runs.
func (p *Provider) reap(ctx context.Context, scope state.Scope, id string) bool {
	idle, running, err := p.idleContainers(scope, id)
	if err != nil {
		return false
	}
	if !running {
		return true
	}
	for revisionID, container := range idle {
		if p.runtime.Stop(ctx, container) != nil {
			continue
		}
		_ = p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
			if revision := record.Revisions[revisionID]; revision != nil && revision.Container == container {
				revision.Container, revision.Address, revision.reaped = "", "", true
				releaseMetadata(revision)
			}
			return nil
		})
	}
	return false
}

// idleContainers names the service's containers to stop, by revision, and
// reports whether any container runs.
func (p *Provider) idleContainers(scope state.Scope, id string) (map[string]string, bool, error) {
	idle, running := map[string]string{}, false
	err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		for _, revision := range record.Revisions {
			if revision.Container == "" {
				continue
			}
			running = true
			if !keptWarm(record, revision) && revision.inflight == 0 && p.clock().Sub(revision.lastActivity) >= idleTimeout {
				idle[revision.ID] = revision.Container
			}
		}
		return nil
	})
	return idle, running, err
}

// keptWarm reports a revision whose minInstanceCount keeps it running while
// it receives traffic. Locally one process stands in for every instance.
func keptWarm(record *serviceRecord, revision *revisionRecord) bool {
	if revision.Template.Scaling.MinInstances == 0 {
		return false
	}
	if len(record.Statuses) == 0 {
		return servingRevision(record) == revision
	}
	return referenced(record.Statuses, revision.ID)
}

// workloadEnv builds the workload environment: service identity variables plus
// the metadata binding. ADC inside the container resolves the service's
// configured service account through the per-workload metadata endpoint.
func workloadEnv(record *serviceRecord, revision *revisionRecord) []string {
	environment := map[string]string{
		"K_SERVICE":       record.Name,
		"K_REVISION":      revision.ID,
		"K_CONFIGURATION": "cfg-" + record.Name,
		"PORT":            strconv.Itoa(revision.Template.Container.Port),
	}
	maps.Copy(environment, revision.Template.Container.Env)
	spec := make([]string, 0, len(environment))
	for _, key := range slices.Sorted(maps.Keys(environment)) {
		spec = append(spec, key+"="+environment[key])
	}
	return spec
}

// principalEmail is the caller's email, as creator and lastModifier report it.
func principalEmail(call gateway.Call) string {
	_, email, found := strings.Cut(call.Principal.Subject, ":")
	if !found {
		return call.Principal.Subject
	}
	return email
}

// typed returns a copy of a document with its Any type URL, as operation
// metadata and responses carry it.
func typed(document map[string]any, typeURL string) map[string]any {
	copied := maps.Clone(document)
	copied["@type"] = typeURL
	return copied
}

// create registers a service and its first rollout, launches the revision
// outside the store lock, and returns the rollout operation, which container
// readiness finishes.
func (p *Provider) create(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	service, err := decodeService(request.JSON)
	if err != nil {
		return nil, err
	}
	spec, err := validateService(service, requestCreateName)
	if err != nil {
		return nil, err
	}
	// The v2 reference composes the name from parent and serviceId and ignores
	// service.name; without serviceId, the body's full name supplies the id.
	id := request.Query.Get("serviceId")
	if id == "" {
		id = provider.LastSegment(service.GetName())
	}
	if !uidPattern.MatchString(id) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid service name %q", id)
	}
	spec.Template.RevisionNamed = spec.Template.Revision != ""
	if spec.Template.Revision == "" {
		spec.Template.Revision = id + "-00001"
	}
	now := p.clock()
	var launchInputs launchSpec
	var document map[string]any
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if s.services[id] != nil {
			return status.Errorf(codes.AlreadyExists, "Resource '%s' already exists.", id)
		}
		if err := p.indexService(scope, id); err != nil {
			return err
		}
		revision := &revisionRecord{ID: spec.Template.Revision, Generation: 1, Template: spec.Template, CreatedAt: now}
		record := &serviceRecord{
			Name: id, UID: id + "-uid", Generation: 1, Spec: spec,
			Creator: principalEmail(call), Modifier: principalEmail(call),
			Revisions: map[string]*revisionRecord{revision.ID: revision}, Created: 1,
			Reconciling: true, Condition: conditionPending, CreatedAt: now, UpdatedAt: now,
		}
		s.services[id] = record
		launchInputs = launchSpecOf(record, revision)
		document = p.serviceDocument(scope, record)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	operation, err := p.startRollout(scope, id, 1, document)
	if err != nil {
		return nil, err
	}
	started, launchErr := p.launch(launchInputs)
	return p.finishLaunch(scope, id, spec.Template.Revision, operation, started, launchErr)
}

// startRollout records a rollout's operation and makes it the service's
// newest rollout, if the service is still at generation.
func (p *Provider) startRollout(scope state.Scope, id string, generation int64, document map[string]any) (string, error) {
	operation, err := p.operations.Start(scope, provider.Launch{Target: wireName(scope, "services", id), Verb: "deploy", Metadata: typed(document, typeService)})
	if err != nil {
		return "", err
	}
	_ = p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		if record.Generation == generation {
			record.Rollout = operation
		}
		return nil
	})
	return operation, nil
}

// finishLaunch records a create's launch outcome. A failed launch finishes
// the rollout with the error. A service deleted (or replaced) during its
// launch discards the container and reports the rollout aborted.
func (p *Provider) finishLaunch(scope state.Scope, id, revisionID, operation string, started launched, launchErr error) (*gateway.RESTResponse, error) {
	attached, deleted := false, false
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.services[id]
		revision := ownRevision(record, revisionID)
		if revision == nil || record.Rollout != operation {
			deleted = true
			return nil
		}
		if attached = launchErr == nil; attached {
			p.attach(revision, started)
		} else {
			record.Reconciling, record.Condition, record.Detail = false, conditionFailed, launchErr.Error()
			document = p.serviceDocument(scope, record)
		}
		return nil
	})
	switch {
	case deleted:
		if launchErr == nil {
			p.discard(started)
		}
		p.operations.Finish(scope, operation, nil, status.Error(codes.Aborted, "The service was deleted during its rollout"))
	case launchErr != nil:
		p.operations.Finish(scope, operation, typed(document, typeService), status.Error(codes.Internal, launchErr.Error()))
	case attached:
		p.indexHost(scope, id)
		p.startReaper(scope, id)
		p.watch(scope, id, revisionID, operation)
	}
	return p.operation(scope, operation)
}

// ownRevision returns one revision of a possibly missing service.
func ownRevision(record *serviceRecord, id string) *revisionRecord {
	if record == nil {
		return nil
	}
	return record.Revisions[id]
}

// indexService publishes the service and its location ancestry to the index;
// call it under the scope lock.
func (p *Provider) indexService(scope state.Scope, id string) error {
	if err := p.index.Put(serviceName, state.Resource{
		Name: locationTarget(scope), Service: serviceName, Kind: "location",
		Parent: provider.ProjectResource(scope.Project), Scope: scope,
	}); err != nil {
		return fmt.Errorf("index the run location: %w", err)
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: serviceResource(scope.Project, scope.Location, id), Service: serviceName,
		Kind: kindService, Parent: locationTarget(scope), Scope: scope,
	}); err != nil {
		return fmt.Errorf("index the service: %w", err)
	}
	return nil
}

// finishRollout ends one rollout: the operation finishes with the service as
// it now is, or with the failure. Only the service's newest rollout changes
// which revision serves and the service's condition; an operation finishes
// once, so a later outcome changes nothing.
func (p *Provider) finishRollout(scope state.Scope, id, revisionID, operation string, failure error) {
	var document map[string]any
	_ = p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		if record.Rollout == operation && record.Reconciling {
			failure = settleRollout(record, revisionID, failure)
		}
		document = typed(p.serviceDocument(scope, record), typeService)
		return nil
	})
	if document == nil && failure == nil {
		failure = status.Error(codes.Aborted, "The service was deleted during its rollout")
	}
	p.operations.Finish(scope, operation, document, failure)
}

// settleRollout ends the newest rollout on its record and returns its
// failure. A ready revision becomes the latest ready even when its traffic
// block can't take effect; the routing in effect then stays as it was.
func settleRollout(record *serviceRecord, revisionID string, failure error) error {
	record.Reconciling = false
	if failure == nil {
		record.Ready = revisionID
		statuses, missing := resolveStatuses(record.Spec.Traffic, revisionID, func(name string) bool { return record.Revisions[name] != nil })
		if failure = missing; missing == nil {
			record.Statuses = statuses
		}
	}
	record.Condition, record.Detail = conditionSucceeded, ""
	if failure != nil {
		record.Condition, record.Detail = conditionFailed, status.Convert(failure).Message()
	}
	return failure
}

// releaseMetadata stops the revision's private metadata listener, if one runs.
func releaseMetadata(revision *revisionRecord) {
	if revision.releaseMetadata != nil {
		revision.releaseMetadata()
		revision.releaseMetadata = nil
	}
}

// readinessTimeout bounds how long a launched container has to answer its
// first HTTP request before the launch operation fails.
const readinessTimeout = 60 * time.Second

// watch resolves a rollout from its revision's actual readiness: the
// revision answers its first HTTP request, or the container exits first.
func (p *Provider) watch(scope state.Scope, id, revisionID, operation string) {
	go func() {
		base := context.WithoutCancel(context.Background())
		var address, container string
		_ = p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
			if revision := record.Revisions[revisionID]; revision != nil {
				address, container = revision.Address, revision.Container
			}
			return nil
		})
		if address == "" {
			p.finishRollout(scope, id, revisionID, operation, status.Error(codes.Aborted, "The revision was removed during its rollout"))
			return
		}
		// The runtime's Ready blocks until the container exits, so a single
		// watcher races the readiness probe instead of joining the loop.
		go func() {
			if exited := p.runtime.Ready(base, container); exited != nil {
				p.finishRollout(scope, id, revisionID, operation, status.Error(codes.Unknown, exited.Error()))
			}
		}()
		deadline := time.Now().Add(readinessTimeout)
		for time.Now().Before(deadline) {
			if serving(address) && p.listening(container) {
				p.finishRollout(scope, id, revisionID, operation, nil)
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		p.finishRollout(scope, id, revisionID, operation, status.Error(codes.DeadlineExceeded, "The revision did not become ready before the deadline"))
	}()
}

// listenerOwner is a runtime whose revision address another process can
// claim before the revision binds it: local processes, whose port is picked
// before launch. Docker publishes or networks each container itself.
type listenerOwner interface {
	Listening(container string) bool
}

// listening reports whether the revision itself holds its address, when the
// runtime can tell; otherwise the address is the revision's alone.
func (p *Provider) listening(container string) bool {
	if owner, ok := p.runtime.(listenerOwner); ok {
		return owner.Listening(container)
	}
	return true
}

// serving reports whether the revision answers one loopback HTTP request.
func serving(address string) bool {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, "http://"+address+"/", nil)
	if err != nil {
		return false
	}
	client := &http.Client{Timeout: time.Second}
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	defer func() { _ = response.Body.Close() }()
	return true
}

// get returns one service document.
func (p *Provider) get(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		document = p.serviceDocument(scope, record)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// list returns one page of the location's services in name order.
func (p *Provider) list(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body map[string]any
	if err := p.scopes.With(scope, func(s *scopeState) error {
		page, next, err := provider.Page(slices.Sorted(maps.Keys(s.services)), request.Query, paging, gap)
		if err != nil {
			return err
		}
		items := make([]map[string]any, 0, len(page))
		for _, id := range page {
			items = append(items, p.serviceDocument(scope, s.services[id]))
		}
		body = provider.ListBody("services", items, next)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// pendingUpdate is what an update decided under the lock before its launch:
// the generation it read, and the revision to launch, if the template changed.
type pendingUpdate struct {
	generation int64
	revision   *revisionRecord
	spec       launchSpec
}

// update replaces the service and returns its rollout operation. A changed
// template starts a new revision outside the store lock, which serves once it
// is ready; an unchanged one creates no revision, and the rollout applies the
// new routing at once (recording run-traffic). An update that raced another
// update or a delete during its launch discards its container.
func (p *Provider) update(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	service, err := decodeService(request.JSON)
	if err != nil {
		return nil, err
	}
	spec, err := validateService(service, requestUpdateName)
	if err != nil {
		return nil, err
	}
	pending, err := p.planUpdate(scope, id, &spec)
	if err != nil {
		return nil, err
	}
	var started launched
	if pending.revision != nil {
		var launchErr error
		if started, launchErr = p.launch(pending.spec); launchErr != nil {
			return nil, status.Errorf(codes.Internal, "The revision container failed to start: %v", launchErr)
		}
	}
	document, ready, err := p.commitUpdate(call, scope, id, spec, pending, started)
	if err != nil {
		if pending.revision != nil {
			p.discard(started)
		}
		return nil, err
	}
	operation, err := p.startRollout(scope, id, pending.generation+1, document)
	if err != nil {
		return nil, err
	}
	if pending.revision == nil {
		p.finishRollout(scope, id, ready, operation, nil)
		return p.operation(scope, operation)
	}
	p.startReaper(scope, id)
	p.watch(scope, id, pending.revision.ID, operation)
	return p.operation(scope, operation)
}

// commitUpdate stores an update, with its launched revision if any, unless
// another change won the race. It returns the service document and the
// revision LATEST traffic resolves to now.
func (p *Provider) commitUpdate(call gateway.Call, scope state.Scope, id string, spec serviceSpec, pending pendingUpdate, started launched) (map[string]any, string, error) {
	var document map[string]any
	var ready string
	err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		if record.Generation != pending.generation {
			return status.Errorf(codes.Aborted, "service %q changed during the rollout; retry the update", id)
		}
		record.Generation++
		record.Spec, record.Modifier, record.UpdatedAt = spec, principalEmail(call), p.clock()
		record.Reconciling, record.Condition, record.Detail = true, conditionPending, ""
		if pending.revision != nil {
			p.attach(pending.revision, started)
			record.Revisions[pending.revision.ID] = pending.revision
			record.Created++
			record.UpdatedAt = pending.revision.CreatedAt
		}
		ready = cmp.Or(record.Ready, latestRevision(record).ID)
		document = p.serviceDocument(scope, record)
		return nil
	})
	return document, ready, err
}

// planUpdate reads the service and decides an update's revision: none when
// the template is unchanged (spec then keeps the current one), else a new one
// named by the request or numbered after the revisions created so far.
func (p *Provider) planUpdate(scope state.Scope, id string, spec *serviceSpec) (pendingUpdate, error) {
	var pending pendingUpdate
	err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		pending.generation = record.Generation
		spec.Template.RevisionNamed = spec.Template.Revision != ""
		if sameTemplate(record.Spec.Template, spec.Template) {
			spec.Template = record.Spec.Template
			return nil
		}
		revisionID := spec.Template.Revision
		if revisionID == "" {
			revisionID = fmt.Sprintf("%s-%05d", record.Name, record.Created+1)
		} else if record.Revisions[revisionID] != nil {
			return status.Errorf(codes.AlreadyExists, "Revision named '%s' already exists.", revisionID)
		}
		spec.Template.Revision = revisionID
		pending.revision = &revisionRecord{ID: revisionID, Generation: record.Generation + 1, Template: spec.Template, CreatedAt: p.clock()}
		pending.spec = launchSpecOf(record, pending.revision)
		return nil
	})
	return pending, err
}

// deleted records the operation of a finished delete. Its metadata is the
// resource being deleted, still reconciling, and its response the resource
// as deleted; it reads done after the store's latency, as GCP's delete does
// (recording run-service-lifecycle).
func (p *Provider) deleted(scope state.Scope, target, typeURL string, snapshot map[string]any) (*gateway.RESTResponse, error) {
	response := typed(snapshot, typeURL)
	metadata := maps.Clone(response)
	metadata["reconciling"] = true
	operation, err := p.operations.Start(scope, provider.Launch{Target: target, Verb: "delete", Metadata: metadata})
	if err != nil {
		return nil, err
	}
	p.operations.Finish(scope, operation, response, nil)
	return p.operation(scope, operation)
}

// delete removes the service and its index entry under the scope lock, then
// stops every container, and returns the delete's operation.
func (p *Provider) delete(call gateway.Call, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var snapshot map[string]any
	var revisions []*revisionRecord
	if err := p.withService(scope, id, func(s *scopeState, record *serviceRecord) error {
		// A delete is a change of the service: GCP reports it one generation on.
		snapshot = p.serviceDocument(scope, record)
		snapshot["generation"] = strconv.FormatInt(record.Generation+1, 10)
		snapshot["deleteTime"] = provider.Stamp(p.clock())
		delete(snapshot, "reconciling")
		// Removing the index entry also drops the service's policy.
		if err := p.index.Remove(serviceName, scope, serviceResource(scope.Project, scope.Location, id)); err != nil {
			return fmt.Errorf("unindex the service: %w", err)
		}
		delete(s.services, id)
		revisions = slices.Collect(maps.Values(record.Revisions))
		return nil
	}); err != nil {
		return nil, err
	}
	p.stopReaper(scope, id)
	p.forgetHost(scope, id)
	for _, revision := range revisions {
		_ = p.runtime.Stop(call.Context, revision.Container)
		releaseMetadata(revision)
	}
	return p.deleted(scope, wireName(scope, "services", id), typeService, snapshot)
}

// getIam returns the stored service policy.
func (p *Provider) getIam(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var policy iam.Policy
	if err := p.withService(scope, id, func(*scopeState, *serviceRecord) error {
		var err error
		policy, err = p.engine.GetPolicy(serviceResource(scope.Project, scope.Location, id))
		if err != nil {
			return fmt.Errorf("read service policy: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// setIam stores a service policy, conditional bindings included (recording
// run-service-lifecycle), while the service is known to exist. The default
// mask is the whole policy; audit configs and other masks are gaps.
func (p *Provider) setIam(request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	requested, err := provider.DecodePolicy(request.JSON, gap)
	if err != nil {
		return nil, err
	}
	var policy iam.Policy
	if err := p.withService(scope, id, func(*scopeState, *serviceRecord) error {
		var err error
		policy, err = p.engine.SetPolicy(serviceResource(scope.Project, scope.Location, id), requested)
		if err != nil {
			return fmt.Errorf("write service policy: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// revisionsList returns one page of the service's revisions, newest first
// (recording run-service-lifecycle).
func (p *Provider) revisionsList(request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var body map[string]any
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		ordered := sortedRevisions(record)
		slices.Reverse(ordered)
		page, next, err := provider.Page(ordered, request.Query, paging, gap)
		if err != nil {
			return err
		}
		items := make([]map[string]any, 0, len(page))
		for _, revision := range page {
			items = append(items, p.revisionDocument(scope, record, revision))
		}
		body = provider.ListBody("revisions", items, next)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// errNoRevision reports a missing revision.
func errNoRevision(id string) error {
	return status.Errorf(codes.NotFound, "Revision '%s' does not exist.", id)
}

// revisionGet returns one revision of the service.
func (p *Provider) revisionGet(request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		revision := record.Revisions[request.Path["revision"]]
		if revision == nil {
			return errNoRevision(request.Path["revision"])
		}
		document = p.revisionDocument(scope, record, revision)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// revisionDelete removes one revision that is neither the latest created
// nor serving a percent of the traffic in effect, with GCP's refusals
// (recording run-traffic). A tag alone doesn't keep a revision: its traffic
// targets go with it, which changes the service's generation and routing, as
// in GCP. The stop runs under the scope lock so a failed stop leaves the
// revision in place.
func (p *Provider) revisionDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	revisionID := request.Path["revision"]
	var snapshot map[string]any
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		revision := record.Revisions[revisionID]
		switch {
		case revision == nil:
			return errNoRevision(revisionID)
		case latestRevision(record) == revision:
			return status.Errorf(codes.FailedPrecondition, "The latest created Revision %q cannot be directly deleted.", revisionID)
		case serves(record.Statuses, revisionID):
			return status.Errorf(codes.FailedPrecondition, "Revision %q cannot be directly deleted because it is actively serving.", revisionID)
		}
		if err := p.runtime.Stop(call.Context, revision.Container); err != nil {
			return fmt.Errorf("stop the revision container: %w", err)
		}
		snapshot = p.revisionDocument(scope, record, revision)
		releaseMetadata(revision)
		delete(record.Revisions, revisionID)
		untarget(record, revisionID, p.clock())
		return nil
	}); err != nil {
		return nil, err
	}
	return p.deleted(scope, wireName(scope, "services", id, "revisions", revisionID), typeRevision, snapshot)
}

// untarget drops a deleted revision's traffic targets. When the traffic
// block named it, the service moves one generation on and its routing drops
// the revision; otherwise nothing changes (recording run-traffic).
func untarget(record *serviceRecord, revisionID string, now time.Time) {
	kept := slices.DeleteFunc(slices.Clone(record.Spec.Traffic), func(target trafficTarget) bool { return target.Revision == revisionID })
	if len(kept) == len(record.Spec.Traffic) {
		return
	}
	record.Spec.Traffic = kept
	record.Statuses = slices.DeleteFunc(slices.Clone(record.Statuses), func(entry trafficStatus) bool { return entry.Revision == revisionID })
	record.Generation++
	record.UpdatedAt = now
}

// serviceURI returns the local invocation URL of one service. The loopback
// host mirrors the run.app hostname shape on the shared local listener.
func (p *Provider) serviceURI(scope state.Scope, id string) string {
	base := p.baseURL
	if base == "" {
		base = "http://127.0.0.1"
	}
	return strings.TrimSuffix(base, "/") + invocationPath + hostOf(scope, id)
}
