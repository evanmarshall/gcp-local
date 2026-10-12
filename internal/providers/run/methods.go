package run

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// kind identifiers used by the resource index.
const (
	kindService   = "service"
	kindOperation = "operation"
)

// methodNames lists every registered method.
func methodNames() []string {
	return []string{
		methodServicesCreate, methodServicesGet, methodServicesList,
		methodServicesUpdate, methodServicesDelete,
		methodServicesGetIam, methodServicesSetIam,
		methodRevisionsList, methodRevisionsGet, methodRevisionsDelete,
		methodOperationsGet, methodOperationsList, methodOperationsDelete,
	}
}

// revisionRecord is one stored revision of a service.
type revisionRecord struct {
	ID              string
	Template        templateShape
	CreatedAt       time.Time
	Container       string
	Address         string
	releaseMetadata func()
	lastActivity    time.Time
}

// serviceRecord is one stored service of a scope.
type serviceRecord struct {
	Name       string
	UID        string
	Generation int64
	Ingress    string
	Template   templateShape
	Revisions  map[string]*revisionRecord
	Traffic    map[string]int
	CreatedAt  time.Time
	UpdatedAt  time.Time
}

// scopeState is one project/location slice of provider state.
type scopeState struct {
	services   map[string]*serviceRecord
	operations map[string]*operationRecord
}

func newScopeState() scopeState {
	return scopeState{services: map[string]*serviceRecord{}, operations: map[string]*operationRecord{}}
}

func (s *scopeState) reset() {
	s.services = map[string]*serviceRecord{}
	s.operations = map[string]*operationRecord{}
}

// operationRecord is one long-running operation of a scope. A rollout reports
// its service's current document; a delete reports the resource as deleted.
type operationRecord struct {
	Name      string
	Done      bool
	ErrCode   int
	ErrText   string
	CreatedAt time.Time
	Service   string
	Snapshot  map[string]any
}

// Any type URLs of the resources Cloud Run v2 operations carry as metadata
// and response.
const (
	typeService  = "type.googleapis.com/google.cloud.run.v2.Service"
	typeRevision = "type.googleapis.com/google.cloud.run.v2.Revision"
)

// wireName is the relative resource name the v2 API returns, such as
// projects/p/locations/l/services/s. IAM targets use resource instead.
func wireName(scope state.Scope, path ...string) string {
	return "projects/" + scope.Project + "/locations/" + scope.Location + "/" + strings.Join(path, "/")
}

// execute dispatches one authorized method to its resource family.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodServicesCreate, methodServicesGet, methodServicesList, methodServicesUpdate,
		methodServicesDelete, methodServicesGetIam, methodServicesSetIam:
		return p.executeService(call, request, scope, record)
	case methodRevisionsList, methodRevisionsGet, methodRevisionsDelete:
		return p.executeRevision(call, request, scope, record)
	case methodOperationsGet, methodOperationsList, methodOperationsDelete:
		return p.executeOperations(call, request, scope)
	default:
		return nil, errUnimplemented
	}
}

// executeService dispatches the service methods.
func (p *Provider) executeService(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodServicesCreate:
		return p.create(request, scope)
	case methodServicesGet:
		return p.get(scope, record)
	case methodServicesList:
		return p.list(scope)
	case methodServicesUpdate:
		return p.update(request, scope, record)
	case methodServicesDelete:
		return p.delete(call, scope, record)
	case methodServicesGetIam:
		return p.getIam(scope, record)
	case methodServicesSetIam:
		return p.setIam(request, scope, record)
	default:
		return nil, errUnimplemented
	}
}

// executeRevision dispatches the revision methods.
func (p *Provider) executeRevision(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodRevisionsList:
		return p.revisionsList(scope, record)
	case methodRevisionsGet:
		return p.revisionGet(request, scope, record)
	case methodRevisionsDelete:
		return p.revisionDelete(call, request, scope, record)
	default:
		return nil, errUnimplemented
	}
}

// executeOperations dispatches the operation methods.
func (p *Provider) executeOperations(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodOperationsGet:
		return p.operationGet(request, scope)
	case methodOperationsList:
		return p.operationsList(scope)
	case methodOperationsDelete:
		return p.operationDelete()
	default:
		return nil, errUnimplemented
	}
}

// sortedRevisions returns the service's revisions in creation order.
func sortedRevisions(record *serviceRecord) []*revisionRecord {
	ordered := make([]*revisionRecord, 0, len(record.Revisions))
	for _, id := range slices.Sorted(maps.Keys(record.Revisions)) {
		ordered = append(ordered, record.Revisions[id])
	}
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

// serviceDocument renders the Cloud Run v2 service wire shape. Enum values
// and names follow google.cloud.run.v2.Service, so gRPC clients can decode it.
func (p *Provider) serviceDocument(scope state.Scope, record *serviceRecord) map[string]any {
	traffic := make([]any, 0, len(record.Traffic))
	for _, revision := range slices.Sorted(maps.Keys(record.Traffic)) {
		traffic = append(traffic, map[string]any{"revision": revision, "percent": record.Traffic[revision], keyType: "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION"})
	}
	ready, detail := conditionSucceeded, "The service is ready"
	for _, revision := range record.Revisions {
		if revision.Container == "" {
			ready, detail = conditionFailed, "The revision container is not running"
			break
		}
	}
	document := map[string]any{
		keyName:                 wireName(scope, "services", record.Name),
		"uid":                   record.UID,
		"generation":            strconv.FormatInt(record.Generation, 10),
		"createTime":            provider.Stamp(record.CreatedAt),
		"updateTime":            provider.Stamp(record.UpdatedAt),
		"template":              templateDocument(record),
		"traffic":               traffic,
		"uri":                   p.serviceURI(scope, record.Name),
		"latestCreatedRevision": latestRevision(record).ID,
		"latestReadyRevision":   latestRevision(record).ID,
		"conditions":            []any{map[string]any{keyType: "Ready", "state": ready, "message": detail}},
	}
	// An unset ingress has no wire value; the enum has no empty spelling.
	if record.Ingress != "" {
		document["ingress"] = record.Ingress
	}
	return document
}

// Condition states of google.cloud.run.v2.Condition.
const (
	conditionSucceeded = "CONDITION_SUCCEEDED"
	conditionFailed    = "CONDITION_FAILED"
)

// templateDocument renders a revision template wire shape.
func templateDocument(record *serviceRecord) map[string]any {
	// Stable order: a map-ordered echo makes declarative clients see drift.
	env := make([]any, 0, len(record.Template.Container.Env))
	for _, key := range slices.Sorted(maps.Keys(record.Template.Container.Env)) {
		env = append(env, map[string]any{"name": key, "value": record.Template.Container.Env[key]})
	}
	document := map[string]any{
		"serviceAccount": record.Template.ServiceAccount,
		"containers": []any{map[string]any{
			"image":     record.Template.Container.Image,
			"ports":     []any{map[string]any{"containerPort": record.Template.Container.Port, keyName: "http1"}},
			"env":       env,
			"resources": map[string]any{},
		}},
	}
	if record.Template.RevisionNamed {
		document["revision"] = record.Template.Revision
	}
	return document
}

// revisionDocument renders one google.cloud.run.v2.Revision wire shape.
func (p *Provider) revisionDocument(scope state.Scope, record *serviceRecord, revision *revisionRecord) map[string]any {
	ready, detail := conditionSucceeded, "The revision is ready"
	if revision.Container == "" {
		ready, detail = conditionFailed, "The revision container is not running"
	}
	return map[string]any{
		keyName:          wireName(scope, "services", record.Name, "revisions", revision.ID),
		"service":        wireName(scope, "services", record.Name),
		"uid":            revision.ID + "-uid",
		"generation":     strconv.FormatInt(record.Generation, 10),
		"createTime":     provider.Stamp(revision.CreatedAt),
		"containers":     templateDocument(&serviceRecord{Template: revision.Template})["containers"],
		"serviceAccount": revision.Template.ServiceAccount,
		"conditions":     []any{map[string]any{keyType: "ContainerReady", "state": ready, "message": detail}},
	}
}

// operationDocument renders the long-running operation wire shape. As on
// Cloud Run v2, metadata and response are the resource the operation acts on:
// the deleted resource's snapshot, or the service's current document.
func (p *Provider) operationDocument(scope state.Scope, entry operationRecord, record *serviceRecord) map[string]any {
	subject := entry.Snapshot
	if subject == nil && record != nil {
		subject = p.serviceDocument(scope, record)
		subject["@type"] = typeService
	}
	document := map[string]any{keyName: entry.Name, "done": entry.Done}
	if subject != nil {
		document["metadata"] = subject
	}
	if !entry.Done {
		return document
	}
	if entry.ErrCode != 0 {
		document[keyError] = map[string]any{keyCode: entry.ErrCode, keyMessage: entry.ErrText}
		return document
	}
	if subject != nil {
		document["response"] = subject
	}
	return document
}

// startRevision launches the container for one revision.
func (p *Provider) startRevision(record *serviceRecord, revision *revisionRecord) error {
	spec := StartSpec{
		Image: revision.Template.Container.Image,
		Port:  revision.Template.Container.Port,
		Env:   workloadEnv(record, revision),
	}
	var gate *peerGate
	if revision.Template.ServiceAccount != "" && p.metadata != nil {
		subject := "serviceAccount:" + revision.Template.ServiceAccount
		handler, metadataErr := p.metadata(subject)
		if metadataErr != nil {
			return fmt.Errorf("enroll the workload identity %s: %w", subject, metadataErr)
		}
		gate = newPeerGate(handler)
		hostPort, release, bindErr := p.metabind(gate)
		if bindErr != nil {
			return fmt.Errorf("bind the workload metadata listener: %w", bindErr)
		}
		revision.releaseMetadata = release
		spec.Env = append(spec.Env, "GCE_METADATA_HOST="+hostPort)
	}
	started, err := p.runtime.Start(context.WithoutCancel(context.Background()), spec)
	if err != nil {
		releaseMetadata(revision)
		return fmt.Errorf("start the revision container: %w", err)
	}
	if gate != nil {
		gate.allow(started.Peer)
	}
	revision.Container = started.Container
	revision.Address = started.Address
	revision.lastActivity = p.clock()
	return nil
}

// idleTimeout is how long a revision container may sit without an invocation
// before the provider stops it; the next invocation cold-starts it again.
const idleTimeout = 5 * time.Minute

// startReaper schedules the idle reaper for a service once. Reset and delete
// cancel the reaper with its scope; a reaped or missing service ends it.
func (p *Provider) startReaper(scope state.Scope, id string) {
	watch, cancel := context.WithCancel(context.Background())
	p.reaperMu.Lock()
	p.reapers[scope.Project+"/"+id] = cancel
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

// reap stops the service's container when it has been idle past the timeout.
func (p *Provider) reap(ctx context.Context, scope state.Scope, id string) bool {
	p.recordMu.RLock()
	record, refused, err := p.serviceRecordLookUp(scope, id)
	if err != nil || refused != nil {
		p.recordMu.RUnlock()
		return false
	}
	revision := latestRevision(record)
	if revision == nil || revision.Container == "" {
		p.recordMu.RUnlock()
		return true
	}
	idle := p.clock().Sub(revision.lastActivity)
	p.recordMu.RUnlock()
	if idle < idleTimeout {
		return false
	}
	if stopErr := p.runtime.Stop(ctx, revision.Container); stopErr == nil {
		p.recordMu.Lock()
		revision.Container = ""
		revision.Address = ""
		releaseMetadata(revision)
		p.recordMu.Unlock()
		return true
	}
	return false
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

// create registers a service, starts its first revision, and returns a pending
// operation that resolves from container readiness.
func (p *Provider) create(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, decodeErr := decodeRequest(request.JSON)
	if decodeErr != nil {
		return nil, decodeErr
	}
	id, template, err := parseCreate(request)
	if err != nil {
		return nil, err
	}
	now := p.clock()
	var conflict *gateway.RESTResponse
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.services[id] != nil {
			conflict = provider.WireError(409, "ALREADY_EXISTS", "service "+id+" already exists")
			return nil
		}
		revision := &revisionRecord{ID: template.Revision, Template: template, CreatedAt: now}
		s.services[id] = &serviceRecord{
			Name: id, UID: id + "-uid", Generation: 1,
			Ingress: body.Ingress, Template: template,
			Revisions: map[string]*revisionRecord{revision.ID: revision},
			Traffic:   map[string]int{revision.ID: 100},
			CreatedAt: now, UpdatedAt: now,
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("register the service: %w", err)
	}
	if conflict != nil {
		return conflict, nil
	}
	if err := p.indexService(scope, id); err != nil {
		return nil, err
	}
	return p.launchOperation(scope, id), nil
}

// indexService publishes the service and its location ancestry to the index.
func (p *Provider) indexService(scope state.Scope, id string) error {
	if err := p.index.Put(serviceName, state.Resource{
		Name: locationTarget(scope), Service: serviceName, Kind: "location",
		Parent: provider.ProjectResource(scope.Project), Scope: scope,
	}); err != nil {
		return fmt.Errorf("index the run location: %w", err)
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: resource(scope.Project, scope.Location, kindService, id), Service: serviceName,
		Kind: kindService, Parent: locationTarget(scope), Scope: scope,
	}); err != nil {
		return fmt.Errorf("index the service: %w", err)
	}
	return nil
}

// parseCreate validates one services.create request into its identifiers.
func parseCreate(request *gateway.RESTRequest) (string, templateShape, error) {
	body, requestErr := decodeRequest(request.JSON)
	if requestErr != nil {
		return "", templateShape{}, requestErr
	}
	// The v2 reference composes the name from parent and serviceId and ignores
	// service.name; without serviceId, the body's full name supplies the id.
	id := request.Query.Get("serviceId")
	if id == "" {
		id = provider.LastSegment(body.Name)
	}
	if !uidPattern.MatchString(id) {
		return "", templateShape{}, status.Errorf(codes.InvalidArgument, "Invalid service name %q", id)
	}
	template, templateErr := resolveTemplate(body.Template)
	if templateErr != nil {
		return "", templateShape{}, templateErr
	}
	if ingressErr := validateIngress(body.Ingress); ingressErr != nil {
		return "", templateShape{}, ingressErr
	}
	template.RevisionNamed = template.Revision != ""
	if template.Revision == "" {
		template.Revision = id + "-00001"
	}
	return id, template, nil
}

// launchOperation starts the revision container and registers the pending
// operation that the readiness worker resolves.
func (p *Provider) launchOperation(scope state.Scope, id string) *gateway.RESTResponse {
	operationName := rolloutOperation(scope, id)
	record, refused, err := p.serviceRecordLookUp(scope, id)
	if err != nil || refused != nil {
		return provider.WireError(500, "INTERNAL", "The created service vanished before its launch")
	}
	revision := latestRevision(record)
	if startErr := p.startRevision(record, revision); startErr != nil {
		revision.Container = ""
		entry := operationRecord{Name: operationName, Done: true, ErrCode: 13, ErrText: startErr.Error(), CreatedAt: p.clock(), Service: id}
		p.putOperation(scope, entry)
		return &gateway.RESTResponse{JSON: p.operationDocument(scope, entry, record)}
	}
	entry := operationRecord{Name: operationName, CreatedAt: p.clock(), Service: id}
	p.putOperation(scope, entry)
	p.indexHost(scope, id)
	p.startReaper(scope, id)
	p.watch(scope, id, operationName)
	return &gateway.RESTResponse{JSON: p.operationDocument(scope, entry, record)}
}

// putOperation stores one operation record.
func (p *Provider) putOperation(scope state.Scope, entry operationRecord) {
	_ = p.scopes.With(scope, func(s *scopeState) error {
		s.operations[entry.Name] = &entry
		return nil
	})
}

// rolloutOperation names the long-running operation of a service's latest
// rollout; a later rollout of the same service reuses the name.
func rolloutOperation(scope state.Scope, id string) string {
	return provider.OperationName(scope, "rel-"+id)
}

// finishOperation records the terminal state of one operation.
func (p *Provider) finishOperation(scope state.Scope, name string, code int, message string) {
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if entry := s.operations[name]; entry != nil {
			entry.Done = true
			entry.ErrCode = code
			entry.ErrText = message
		}
		return nil
	})
}

// operationDocuments renders the scope's operations in name order, or only
// the named one. Entries are copied under the scope lock because the readiness
// worker finishes them concurrently.
func (p *Provider) operationDocuments(scope state.Scope, name string) []any {
	type view struct {
		entry   operationRecord
		service *serviceRecord
	}
	var views []view
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, key := range slices.Sorted(maps.Keys(s.operations)) {
			if name == "" || key == name {
				entry := *s.operations[key]
				views = append(views, view{entry, s.services[entry.Service]})
			}
		}
		return nil
	})
	p.recordMu.RLock()
	defer p.recordMu.RUnlock()
	documents := make([]any, 0, len(views))
	for _, v := range views {
		documents = append(documents, p.operationDocument(scope, v.entry, v.service))
	}
	return documents
}

// releaseMetadata stops the revision's private metadata listener, if one runs.
func releaseMetadata(revision *revisionRecord) {
	if revision.releaseMetadata != nil {
		revision.releaseMetadata()
		revision.releaseMetadata = nil
	}
}

// latestRevisionOf reads the newest revision of a service. Callers hold the
// record lock.
func (p *Provider) latestRevisionOf(scope state.Scope, id string) *revisionRecord {
	var record *serviceRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record = s.services[id]
		return nil
	})
	if record == nil {
		return nil
	}
	return latestRevision(record)
}

// readinessTimeout bounds how long a launched container has to answer its
// first HTTP request before the launch operation fails.
const readinessTimeout = 60 * time.Second

// watch resolves the launch operation from actual container readiness: the
// revision answers its first HTTP request, or the container exits first.
func (p *Provider) watch(scope state.Scope, id, operationName string) {
	go func() {
		base := context.WithoutCancel(context.Background())
		p.recordMu.RLock()
		revision := p.latestRevisionOf(scope, id)
		p.recordMu.RUnlock()
		if revision == nil || revision.Address == "" {
			return
		}
		// The runtime's Ready blocks until the container exits, so a single
		// watcher races the readiness probe instead of joining the loop.
		go func() {
			if exited := p.runtime.Ready(base, revision.Container); exited != nil {
				p.finishOperation(scope, operationName, 2, exited.Error())
			}
		}()
		deadline := time.Now().Add(readinessTimeout)
		for time.Now().Before(deadline) {
			if serving(revision.Address) {
				p.finishOperation(scope, operationName, 0, "")
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		p.finishOperation(scope, operationName, 4, "The revision did not become ready before the deadline")
	}()
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
func (p *Provider) get(scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: p.serviceDocument(scope, record)}, nil
}

// list returns the project/location service collection.
func (p *Provider) list(scope state.Scope) (*gateway.RESTResponse, error) {
	ids := p.serviceIDs(scope)
	items := make([]any, 0, len(ids))
	for _, id := range ids {
		record, refused, err := p.serviceRecordLookUp(scope, id)
		if err != nil || refused != nil {
			continue
		}
		items = append(items, p.serviceDocument(scope, record))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"services": items}}, nil
}

// update replaces the template, starts a new revision, and shifts all traffic.
func (p *Provider) update(request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	body, requestErr := decodeRequest(request.JSON)
	if requestErr != nil {
		return nil, requestErr
	}
	template, templateErr := resolveTemplate(body.Template)
	if templateErr != nil {
		return nil, templateErr
	}
	p.recordMu.Lock()
	defer p.recordMu.Unlock()
	if body.Ingress != "" {
		if ingressErr := validateIngress(body.Ingress); ingressErr != nil {
			return nil, ingressErr
		}
		record.Ingress = body.Ingress
	}
	record.Generation++
	revisionID := template.Revision
	if revisionID == "" {
		revisionID = fmt.Sprintf("%s-%05d", record.Name, record.Generation)
	}
	now := p.clock()
	revision := &revisionRecord{ID: revisionID, Template: template, CreatedAt: now}
	if startErr := p.startRevision(record, revision); startErr != nil {
		return nil, status.Errorf(codes.Internal, "The revision container failed to start: %v", startErr)
	}
	record.Template = template
	record.UpdatedAt = now
	record.Revisions[revisionID] = revision
	record.Traffic = p.resolveTraffic(body.Traffic, revisionID)
	// The real surface returns a long-running operation for the rollout.
	entry := operationRecord{Name: rolloutOperation(scope, record.Name), CreatedAt: now, Service: record.Name}
	p.putOperation(scope, entry)
	p.watch(scope, record.Name, entry.Name)
	return &gateway.RESTResponse{JSON: p.operationDocument(scope, entry, record)}, nil
}

// deleted records the finished operation of a delete. Its metadata and
// response are the resource as it was deleted.
func (p *Provider) deleted(scope state.Scope, id, typeURL string, snapshot map[string]any) *gateway.RESTResponse {
	snapshot["@type"] = typeURL
	entry := operationRecord{Name: provider.OperationName(scope, id), Done: true, CreatedAt: p.clock(), Snapshot: snapshot}
	p.putOperation(scope, entry)
	return &gateway.RESTResponse{JSON: p.operationDocument(scope, entry, nil)}
}

// delete stops every container, removes the service, and returns a finished
// operation.
func (p *Provider) delete(call gateway.Call, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	p.stopReapers(scope)
	p.recordMu.Lock()
	defer p.recordMu.Unlock()
	name := record.Name
	snapshot := p.serviceDocument(scope, record)
	for _, revision := range record.Revisions {
		_ = p.runtime.Stop(call.Context, revision.Container)
		releaseMetadata(revision)
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		delete(s.services, name)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("clear the service: %w", err)
	}
	p.forgetHost(scope, name)
	if err := p.index.Remove(serviceName, scope, resource(scope.Project, scope.Location, kindService, name)); err != nil {
		return nil, fmt.Errorf("unindex the service: %w", err)
	}
	return p.deleted(scope, "delete-service-"+name, typeService, snapshot), nil
}

// getIam returns the stored service policy.
func (p *Provider) getIam(scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(resource(scope.Project, scope.Location, kindService, record.Name))
	if err != nil {
		return nil, fmt.Errorf("read service policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setIam stores one unconditional version-1 service policy.
func (p *Provider) setIam(request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy     iam.Policy `json:"policy"`
		UpdateMask string     `json:"updateMask"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	// The default mask is the whole policy (bindings and etag); a partial
	// update is not modeled.
	if envelope.UpdateMask != "" {
		return nil, coverageGap("setIamPolicy updateMask")
	}
	policy, err := p.engine.SetPolicy(resource(scope.Project, scope.Location, kindService, record.Name), envelope.Policy)
	if err != nil {
		return nil, fmt.Errorf("write service policy: %w", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// revisionsList returns the service's revision collection.
func (p *Provider) revisionsList(scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	p.recordMu.RLock()
	defer p.recordMu.RUnlock()
	revisions := sortedRevisions(record)
	items := make([]any, 0, len(revisions))
	for _, revision := range revisions {
		items = append(items, p.revisionDocument(scope, record, revision))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"revisions": items}}, nil
}

// revisionGet returns one revision of the service resolve found.
func (p *Provider) revisionGet(request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	p.recordMu.RLock()
	defer p.recordMu.RUnlock()
	revision := record.Revisions[request.Path["revision"]]
	if revision == nil {
		return provider.WireError(404, "NOT_FOUND", "revision "+request.Path["revision"]+" is not found"), nil
	}
	return &gateway.RESTResponse{JSON: p.revisionDocument(scope, record, revision)}, nil
}

// revisionDelete removes one revision that serves no traffic. A reaped
// revision (container stopped) cannot be deleted through the runtime stop
// path; deleting it now is still correct since nothing serves it.
func (p *Provider) revisionDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, record *serviceRecord) (*gateway.RESTResponse, error) {
	p.recordMu.Lock()
	defer p.recordMu.Unlock()
	id := request.Path["revision"]
	revision := record.Revisions[id]
	if revision == nil {
		return provider.WireError(404, "NOT_FOUND", "revision "+id+" is not found"), nil
	}
	if record.Traffic[id] > 0 {
		return nil, status.Errorf(codes.InvalidArgument, "Revision %q is still serving traffic", id)
	}
	if err := p.runtime.Stop(call.Context, revision.Container); err != nil {
		return nil, fmt.Errorf("stop the revision container: %w", err)
	}
	snapshot := p.revisionDocument(scope, record, revision)
	releaseMetadata(revision)
	delete(record.Revisions, id)
	return p.deleted(scope, "delete-revision-"+id, typeRevision, snapshot), nil
}

func (p *Provider) operationsList(scope state.Scope) (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: map[string]any{"operations": p.operationDocuments(scope, "")}}, nil
}

func (p *Provider) operationGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	documents := p.operationDocuments(scope, provider.OperationName(scope, request.Path["operation"]))
	if len(documents) == 0 {
		return provider.WireError(404, "NOT_FOUND", "operation is not found"), nil
	}
	return &gateway.RESTResponse{JSON: documents[0]}, nil
}

// operationDelete acknowledges deletion of one operation. Operations here
// complete synchronously from readiness, so a finished operation is already
// terminal and the delete is an acknowledged no-op; the real surface uses
// this route to cancel pending rollouts.
func (p *Provider) operationDelete() (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
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

// resolveTraffic honors a requested traffic split when its percents sum to
// 100 and name known revisions; otherwise all traffic shifts to the latest.
func (p *Provider) resolveTraffic(requested []trafficEntry, revisionID string) map[string]int {
	split := map[string]int{}
	total := 0
	for _, entry := range requested {
		name := entry.Revision
		if entry.LatestRevision {
			name = revisionID
		}
		if name == "" || entry.Percent <= 0 || entry.Percent > 100 {
			return map[string]int{revisionID: 100}
		}
		split[name] += entry.Percent
		total += entry.Percent
	}
	if total != 100 {
		return map[string]int{revisionID: 100}
	}
	return split
}
