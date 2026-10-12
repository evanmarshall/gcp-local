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
	// reaped marks a container the idle reaper stopped; the next invocation
	// cold-starts it. starting is closed when a cold start in flight ends.
	reaped   bool
	starting chan struct{}
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
	Name string
	// Rollout identifies which rollout owns the record; a later rollout
	// reuses the name, and a stale watcher must not finish the new one.
	Rollout   uint64
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
	case methodOperationsGet, methodOperationsList, methodOperationsDelete:
		return p.executeOperations(call, request, scope)
	default:
		return nil, errUnimplemented
	}
}

// executeService dispatches the service methods.
func (p *Provider) executeService(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodServicesCreate:
		return p.create(request, scope)
	case methodServicesGet:
		return p.get(scope, id)
	case methodServicesList:
		return p.list(scope)
	case methodServicesUpdate:
		return p.update(request, scope, id)
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
		return p.revisionsList(scope, id)
	case methodRevisionsGet:
		return p.revisionGet(request, scope, id)
	case methodRevisionsDelete:
		return p.revisionDelete(call, request, scope, id)
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

// launched is one started revision container and its metadata listener.
type launched struct {
	container, address string
	release            func()
}

// launchSpec is everything a revision launch needs, copied out of the store
// under its lock so the launch itself runs outside it.
type launchSpec struct {
	start   StartSpec
	account string
}

// launchSpecOf copies the launch inputs of one revision; call it under the
// scope lock.
func launchSpecOf(record *serviceRecord, revision *revisionRecord) launchSpec {
	return launchSpec{
		start: StartSpec{
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
	var gate *peerGate
	var release func()
	if spec.account != "" && p.metadata != nil {
		subject := "serviceAccount:" + spec.account
		handler, metadataErr := p.metadata(subject)
		if metadataErr != nil {
			return launched{}, fmt.Errorf("enroll the workload identity %s: %w", subject, metadataErr)
		}
		gate = newPeerGate(handler)
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
		gate.allow(started.Peer)
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

// reap stops the service's latest container when it has been idle past the
// timeout. It reports whether the reaper is done.
func (p *Provider) reap(ctx context.Context, scope state.Scope, id string) bool {
	var container string
	var idle time.Duration
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		if revision := latestRevision(record); revision != nil {
			container, idle = revision.Container, p.clock().Sub(revision.lastActivity)
		}
		return nil
	}); err != nil {
		return false
	}
	if container == "" {
		return true
	}
	if idle < idleTimeout || p.runtime.Stop(ctx, container) != nil {
		return false
	}
	_ = p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		if revision := latestRevision(record); revision != nil && revision.Container == container {
			revision.Container, revision.Address, revision.reaped = "", "", true
			releaseMetadata(revision)
		}
		return nil
	})
	return true
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

// create registers a service and its pending rollout, launches the first
// revision outside the store lock, and returns the operation that container
// readiness resolves.
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
	rollout := operationRecord{Name: rolloutOperation(scope, id), Rollout: p.rollouts.Add(1), CreatedAt: now, Service: id}
	var spec launchSpec
	var conflict *gateway.RESTResponse
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if s.services[id] != nil {
			conflict = provider.WireError(409, "ALREADY_EXISTS", "service "+id+" already exists")
			return nil
		}
		if err := p.indexService(scope, id); err != nil {
			return err
		}
		revision := &revisionRecord{ID: template.Revision, Template: template, CreatedAt: now}
		record := &serviceRecord{
			Name: id, UID: id + "-uid", Generation: 1,
			Ingress: body.Ingress, Template: template,
			Revisions: map[string]*revisionRecord{revision.ID: revision},
			Traffic:   map[string]int{revision.ID: 100},
			CreatedAt: now, UpdatedAt: now,
		}
		s.services[id] = record
		s.operations[rollout.Name] = &rollout
		spec = launchSpecOf(record, revision)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("register the service: %w", err)
	}
	if conflict != nil {
		return conflict, nil
	}
	started, launchErr := p.launch(spec)
	return p.finishLaunch(scope, id, template.Revision, rollout, started, launchErr)
}

// finishLaunch records a create's launch outcome. A failed launch finishes
// the rollout with the error. A service deleted (or replaced) during its
// launch discards the container and reports the rollout aborted.
func (p *Provider) finishLaunch(scope state.Scope, id, revisionID string, rollout operationRecord, started launched, launchErr error) (*gateway.RESTResponse, error) {
	var document map[string]any
	attached := false
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record, entry := s.services[id], s.operations[rollout.Name]
		revision := ownRevision(record, revisionID)
		if revision == nil || entry == nil || entry.Rollout != rollout.Rollout {
			aborted := rollout
			aborted.Done, aborted.ErrCode, aborted.ErrText = true, int(codes.Aborted), "The service was deleted during its rollout"
			if entry != nil && entry.Rollout == rollout.Rollout {
				*entry = aborted
			}
			document = p.operationDocument(scope, aborted, nil)
			return nil
		}
		if launchErr != nil {
			entry.Done, entry.ErrCode, entry.ErrText = true, int(codes.Internal), launchErr.Error()
		} else {
			p.attach(revision, started)
			attached = true
		}
		document = p.operationDocument(scope, *entry, record)
		return nil
	}); err != nil {
		return nil, fmt.Errorf("record the launch: %w", err)
	}
	switch {
	case attached:
		p.indexHost(scope, id)
		p.startReaper(scope, id)
		p.watch(scope, id, rollout)
	case launchErr == nil:
		p.discard(started)
	}
	return &gateway.RESTResponse{JSON: document}, nil
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

// rolloutOperation names the long-running operation of a service's latest
// rollout; a later rollout of the same service reuses the name.
func rolloutOperation(scope state.Scope, id string) string {
	return provider.OperationName(scope, "rel-"+id)
}

// finishOperation records the terminal state of one rollout operation. A
// finished operation never changes again, and only the rollout that owns the
// record may finish it.
func (p *Provider) finishOperation(scope state.Scope, rollout operationRecord, code int, message string) {
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if entry := s.operations[rollout.Name]; entry != nil && entry.Rollout == rollout.Rollout && !entry.Done {
			entry.Done = true
			entry.ErrCode = code
			entry.ErrText = message
		}
		return nil
	})
}

// operationDocuments renders the scope's operations in name order, or only
// the named one, under the scope lock: the readiness worker finishes them
// concurrently.
func (p *Provider) operationDocuments(scope state.Scope, name string) []any {
	documents := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, key := range slices.Sorted(maps.Keys(s.operations)) {
			if name == "" || key == name {
				entry := *s.operations[key]
				documents = append(documents, p.operationDocument(scope, entry, s.services[entry.Service]))
			}
		}
		return nil
	})
	return documents
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

// watch resolves the launch operation from actual container readiness: the
// revision answers its first HTTP request, or the container exits first.
func (p *Provider) watch(scope state.Scope, id string, rollout operationRecord) {
	go func() {
		base := context.WithoutCancel(context.Background())
		var address, container string
		_ = p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
			if revision := latestRevision(record); revision != nil {
				address, container = revision.Address, revision.Container
			}
			return nil
		})
		if address == "" {
			return
		}
		// The runtime's Ready blocks until the container exits, so a single
		// watcher races the readiness probe instead of joining the loop.
		go func() {
			if exited := p.runtime.Ready(base, container); exited != nil {
				p.finishOperation(scope, rollout, 2, exited.Error())
			}
		}()
		deadline := time.Now().Add(readinessTimeout)
		for time.Now().Before(deadline) {
			if serving(address) && p.listening(container) {
				p.finishOperation(scope, rollout, 0, "")
				return
			}
			time.Sleep(250 * time.Millisecond)
		}
		p.finishOperation(scope, rollout, 4, "The revision did not become ready before the deadline")
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

// list returns the project/location service collection.
func (p *Provider) list(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		for _, id := range slices.Sorted(maps.Keys(s.services)) {
			items = append(items, p.serviceDocument(scope, s.services[id]))
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list the services: %w", err)
	}
	return &gateway.RESTResponse{JSON: map[string]any{"services": items}}, nil
}

// pendingUpdate is what an update decided under the lock before its launch.
type pendingUpdate struct {
	generation int64
	revision   *revisionRecord
	spec       launchSpec
}

// update replaces the template, starts a new revision outside the store lock,
// and shifts the traffic. An update that raced another update or a delete
// during its launch discards its container.
func (p *Provider) update(request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	body, requestErr := decodeRequest(request.JSON)
	if requestErr != nil {
		return nil, requestErr
	}
	template, templateErr := resolveTemplate(body.Template)
	if templateErr != nil {
		return nil, templateErr
	}
	if ingressErr := validateIngress(body.Ingress); ingressErr != nil {
		return nil, ingressErr
	}
	var pending pendingUpdate
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		revisionID := template.Revision
		if revisionID == "" {
			revisionID = fmt.Sprintf("%s-%05d", record.Name, record.Generation+1)
		}
		pending.generation = record.Generation
		pending.revision = &revisionRecord{ID: revisionID, Template: template, CreatedAt: p.clock()}
		pending.spec = launchSpecOf(record, pending.revision)
		return nil
	}); err != nil {
		return nil, err
	}
	started, launchErr := p.launch(pending.spec)
	if launchErr != nil {
		return nil, status.Errorf(codes.Internal, "The revision container failed to start: %v", launchErr)
	}
	rollout := operationRecord{Name: rolloutOperation(scope, id), Rollout: p.rollouts.Add(1), CreatedAt: pending.revision.CreatedAt, Service: id}
	var document map[string]any
	if err := p.withService(scope, id, func(s *scopeState, record *serviceRecord) error {
		if record.Generation != pending.generation {
			return status.Errorf(codes.Aborted, "service %q changed during the rollout; retry the update", id)
		}
		if body.Ingress != "" {
			record.Ingress = body.Ingress
		}
		record.Generation++
		record.Template = template
		record.UpdatedAt = pending.revision.CreatedAt
		p.attach(pending.revision, started)
		record.Revisions[pending.revision.ID] = pending.revision
		record.Traffic = p.resolveTraffic(body.Traffic, pending.revision.ID)
		// The real surface returns a long-running operation for the rollout.
		s.operations[rollout.Name] = &rollout
		document = p.operationDocument(scope, rollout, record)
		return nil
	}); err != nil {
		p.discard(started)
		return nil, err
	}
	p.startReaper(scope, id)
	p.watch(scope, id, rollout)
	return &gateway.RESTResponse{JSON: document}, nil
}

// deletedOperation records the finished operation of a delete; call it under
// the scope lock. Its metadata and response are the resource as deleted.
func (p *Provider) deletedOperation(s *scopeState, scope state.Scope, id, typeURL string, snapshot map[string]any) *gateway.RESTResponse {
	snapshot["@type"] = typeURL
	entry := operationRecord{Name: provider.OperationName(scope, id), Done: true, CreatedAt: p.clock(), Snapshot: snapshot}
	s.operations[entry.Name] = &entry
	return &gateway.RESTResponse{JSON: p.operationDocument(scope, entry, nil)}
}

// delete removes the service and its index entry under the scope lock, then
// stops every container, and returns a finished operation.
func (p *Provider) delete(call gateway.Call, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var response *gateway.RESTResponse
	var revisions []*revisionRecord
	if err := p.withService(scope, id, func(s *scopeState, record *serviceRecord) error {
		snapshot := p.serviceDocument(scope, record)
		// Removing the index entry also drops the service's policy.
		if err := p.index.Remove(serviceName, scope, serviceResource(scope.Project, scope.Location, id)); err != nil {
			return fmt.Errorf("unindex the service: %w", err)
		}
		delete(s.services, id)
		revisions = slices.Collect(maps.Values(record.Revisions))
		response = p.deletedOperation(s, scope, "delete-service-"+id, typeService, snapshot)
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
	return response, nil
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
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setIam stores one unconditional version-1 service policy while the service
// is known to exist.
func (p *Provider) setIam(request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
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
	var policy iam.Policy
	if err := p.withService(scope, id, func(*scopeState, *serviceRecord) error {
		var err error
		policy, err = p.engine.SetPolicy(serviceResource(scope.Project, scope.Location, id), envelope.Policy)
		if err != nil {
			return fmt.Errorf("write service policy: %w", err)
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// revisionsList returns the service's revision collection.
func (p *Provider) revisionsList(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	items := []any{}
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		for _, revision := range sortedRevisions(record) {
			items = append(items, p.revisionDocument(scope, record, revision))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{"revisions": items}}, nil
}

// revisionGet returns one revision of the service.
func (p *Provider) revisionGet(request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var response *gateway.RESTResponse
	if err := p.withService(scope, id, func(_ *scopeState, record *serviceRecord) error {
		revision := record.Revisions[request.Path["revision"]]
		if revision == nil {
			response = provider.WireError(404, "NOT_FOUND", "revision "+request.Path["revision"]+" is not found")
			return nil
		}
		response = &gateway.RESTResponse{JSON: p.revisionDocument(scope, record, revision)}
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
}

// revisionDelete removes one revision that serves no traffic. A reaped
// revision (container stopped) cannot be deleted through the runtime stop
// path; deleting it now is still correct since nothing serves it. The stop
// runs under the scope lock so a failed stop leaves the revision in place.
func (p *Provider) revisionDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, id string) (*gateway.RESTResponse, error) {
	var response *gateway.RESTResponse
	if err := p.withService(scope, id, func(s *scopeState, record *serviceRecord) error {
		revisionID := request.Path["revision"]
		revision := record.Revisions[revisionID]
		if revision == nil {
			response = provider.WireError(404, "NOT_FOUND", "revision "+revisionID+" is not found")
			return nil
		}
		if record.Traffic[revisionID] > 0 {
			return status.Errorf(codes.InvalidArgument, "Revision %q is still serving traffic", revisionID)
		}
		if err := p.runtime.Stop(call.Context, revision.Container); err != nil {
			return fmt.Errorf("stop the revision container: %w", err)
		}
		snapshot := p.revisionDocument(scope, record, revision)
		releaseMetadata(revision)
		delete(record.Revisions, revisionID)
		response = p.deletedOperation(s, scope, "delete-revision-"+revisionID, typeRevision, snapshot)
		return nil
	}); err != nil {
		return nil, err
	}
	return response, nil
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
