package bigtable

import (
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"slices"
	"strings"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized Bigtable request. Provider-owned reads are
// served directly; every mutation and every table method runs inside the
// scope's backend lifecycle gate.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodInstancesGet:
		return p.instancesGet(request, scope)
	case methodInstancesList:
		return p.instancesList(scope)
	case methodClustersList:
		return p.clustersList(request, scope)
	case methodClustersGet:
		return p.clustersGet(request, scope)
	case methodOperationsList, methodOperationGet, methodOperationDelete, methodOperationCancel, methodTableOpGet,
		methodTableOpDelete, methodTableOpCancel:
		return p.operationMethod(call.Method, request, scope)
	case methodInstancesGetIam, methodTablesGetIam:
		return p.getPolicy(call, request, scope, target)
	case methodInstancesSetIam, methodTablesSetIam:
		var envelope struct {
			Policy iam.Policy `json:"policy"`
		}
		if json.Unmarshal(request.JSON, &envelope) != nil {
			return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
		}
		return p.policy(call, request, scope, target, func(name string) (iam.Policy, error) {
			return p.engine.SetPolicy(name, envelope.Policy)
		})
	default:
		return p.mutate(call, request, scope)
	}
}

// getPolicy reads the instance's or table's policy at the requested version.
func (p *Provider) getPolicy(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	version, err := provider.RequestedPolicyVersion(request, "options.requestedPolicyVersion", gap)
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	return p.policy(call, request, scope, target, func(name string) (iam.Policy, error) {
		policy, err := p.engine.GetPolicy(name)
		return policy.View(version), err
	})
}

// policy checks the instance (and, for a table method, the published table)
// exists, authorizes the caller on it, and reads or writes its policy under
// one instance lock. Instance and table deletes unpublish under that lock, so
// a policy call racing a delete reads NOT_FOUND and never touches a recreated
// resource's policy. A table that was never published has no policy.
func (p *Provider) policy(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target, change func(string) (iam.Policy, error)) (*gateway.RESTResponse, error) {
	var response *gateway.RESTResponse
	err := p.withInstance(scope, request.Path["instance"], func(*scopeState, *instanceRecord) error {
		if err := p.publishedTable(call, request, target); err != nil {
			return err
		}
		if _, err := p.gate.Check(call, target, target.Name); err != nil {
			return credentials(call, err, adminHost, rpcNames[call.Method])
		}
		policy, err := change(target.Name)
		if err != nil {
			return err
		}
		response = &gateway.RESTResponse{JSON: policy.Wire()}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return response, nil
}

// publishedTable requires a table policy method's table to be published and
// to be the authorization target; instance methods pass.
func (p *Provider) publishedTable(call gateway.Call, request *gateway.RESTRequest, target iam.Target) error {
	if call.Method != methodTablesGetIam && call.Method != methodTablesSetIam {
		return nil
	}
	table := tableResource(request.Path["project"], request.Path["instance"], request.Path["table"])
	if _, published := p.index.Get(table); !published || target.Name != table {
		return tableNotFound(tableName(request.Path["project"], request.Path["instance"], request.Path["table"]))
	}
	return nil
}

// mutate runs one instance mutation or table method under the backend gate,
// lazily starting the scope's emulator. Callers have already authorized.
func (p *Provider) mutate(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var response *gateway.RESTResponse
	err := p.manager.Use(call.Context, backend.Key{Service: serviceName, Scope: scope}, func(ctx context.Context, driver backend.Backend) error {
		emulator, err := emulatorOf(driver)
		if err != nil {
			return err
		}
		response, err = p.backendMethod(ctx, emulator.admin, call.Method, request, scope)
		return err
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status, or a lifecycle failure the gateway masks
	}
	return response, nil
}

// backendMethod dispatches one gated method.
func (p *Provider) backendMethod(ctx context.Context, admin adminpb.BigtableTableAdminClient, method string, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	tables := tableAdmin{provider: p, admin: admin, scope: scope, request: request}
	switch method {
	case methodInstancesCreate:
		return p.instancesCreate(request, scope)
	case methodInstancesUpdate:
		return p.instancesUpdate(request, scope)
	case methodInstancesDelete:
		return tables.deleteInstance(ctx)
	case methodTablesCreate:
		return tables.create(ctx)
	case methodTablesGet:
		return tables.get(ctx)
	case methodTablesList:
		return tables.list(ctx)
	case methodTablesUpdate:
		return tables.update(ctx)
	case methodTablesModifyFam:
		return tables.modifyFamilies(ctx)
	default:
		return tables.remove(ctx)
	}
}

// Instance and cluster fields the provider doesn't model, by JSON path. Each
// is refused with a gap when set (adminpb CreateInstanceRequest, Instance,
// Cluster): tags, node scaling, autoscaling, and CMEK.
var unmodeledInstance = []string{
	"instance.tags", "clusters.nodeScalingFactor", "clusters.clusterConfig", "clusters.encryptionConfig",
}

// instancesCreate validates one instance with its cluster, then stores it as a
// create operation that reports done after createLatency.
func (p *Provider) instancesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record, err := p.parseInstance(request, scope)
	if err != nil {
		return nil, err
	}
	name := provider.InstanceName(scope.Project, record.Name)
	id, err := p.operations.Start(scope, provider.Launch{Target: operationTarget(name, record.Cluster.Location), Verb: verbCreateInstance})
	if err != nil {
		return nil, err
	}
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.instances[record.Name] != nil {
			return status.Errorf(codes.AlreadyExists, "Instance %s already exists.", record.Name)
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: instanceResource(scope.Project, record.Name), Service: serviceName,
			Kind: kindInstance, Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return fmt.Errorf("index the instance: %w", err)
		}
		s.instances[record.Name] = record
		return nil
	})
	result := instanceSummary(scope.Project, record)
	result["@type"] = typeInstance
	return p.started(scope, id, result, err)
}

// parseInstance strictly decodes and validates one CreateInstance request.
func (p *Provider) parseInstance(request *gateway.RESTRequest, scope state.Scope) (*instanceRecord, error) {
	body := &adminpb.CreateInstanceRequest{}
	if err := provider.DecodeProto(request.JSON, "", body, gap, unmodeledInstance...); err != nil {
		return nil, err
	}
	if parent := body.GetParent(); parent != "" && parent != "projects/"+scope.Project {
		return nil, status.Error(codes.InvalidArgument, "The parent does not match the request path")
	}
	if !uid.MatchString(body.GetInstanceId()) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", body.GetInstanceId())
	}
	cluster, err := parseCluster(body.GetClusters())
	if err != nil {
		return nil, err
	}
	instance := body.GetInstance()
	kind, edition := "PRODUCTION", defaultEdition
	if instance.GetType() != adminpb.Instance_TYPE_UNSPECIFIED {
		kind = instance.GetType().String()
	}
	if instance.GetEdition() != adminpb.Instance_EDITION_UNSPECIFIED {
		edition = instance.GetEdition().String()
	}
	return &instanceRecord{
		Name: body.GetInstanceId(), DisplayName: instance.GetDisplayName(), Type: kind, Edition: edition,
		Labels: maps.Clone(instance.GetLabels()), Cluster: cluster, CreatedAt: p.clock(),
	}, nil
}

// parseCluster validates an instance's one cluster.
func parseCluster(clusters map[string]*adminpb.Cluster) (*clusterRecord, error) {
	if len(clusters) != 1 {
		return nil, gap("instances with other than one cluster; replication is not modeled")
	}
	var cluster *clusterRecord
	for id, spec := range clusters {
		storage := spec.GetDefaultStorageType().String()
		if spec.GetDefaultStorageType() == adminpb.StorageType_STORAGE_TYPE_UNSPECIFIED {
			storage = "SSD"
		}
		cluster = &clusterRecord{Name: id, Location: lastNamed(spec.GetLocation()), ServeNodes: int(spec.GetServeNodes()), StorageType: storage}
	}
	if !uid.MatchString(cluster.Name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid cluster id %q", cluster.Name)
	}
	if cluster.ServeNodes <= 0 || cluster.Location == "" {
		return nil, status.Error(codes.InvalidArgument, "A cluster requires location and serveNodes")
	}
	return cluster, nil
}

// lastNamed returns the id after the final '/', or the value itself.
func lastNamed(value string) string {
	return value[strings.LastIndexByte(value, '/')+1:]
}

// instanceSummary renders the instance fields of a create-operation response.
func instanceSummary(project string, record *instanceRecord) map[string]any {
	labels := maps.Clone(record.Labels)
	if labels == nil {
		labels = map[string]string{}
	}
	return map[string]any{
		keyName:       provider.InstanceName(project, record.Name),
		"displayName": record.DisplayName,
		fieldType:     record.Type,
		"edition":     record.Edition,
		fieldLabels:   labels,
		"state":       "READY",
	}
}

// instanceWire renders one instance document as instances.get returns it.
func instanceWire(project string, record *instanceRecord) map[string]any {
	wire := instanceSummary(project, record)
	wire["createTime"] = provider.Stamp(record.CreatedAt)
	wire["knowledgeCatalogRegion"] = region(record.Cluster.Location)
	return wire
}

// region derives the region of a zone such as us-central1-b.
func region(zone string) string {
	if index := strings.LastIndexByte(zone, '-'); index > 0 {
		return zone[:index]
	}
	return zone
}

func clusterWire(project, instance string, record *clusterRecord) map[string]any {
	return map[string]any{
		keyName:              clusterName(project, instance, record.Name),
		"location":           "projects/" + project + "/locations/" + record.Location,
		"serveNodes":         record.ServeNodes,
		"defaultStorageType": record.StorageType,
		"state":              "READY",
	}
}

// withInstance runs use on one instance under the scope lock, or reports it
// missing as GCP does. use must not retain the record.
func (p *Provider) withInstance(scope state.Scope, instance string, use func(*scopeState, *instanceRecord) error) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback's own errors
		record := s.instances[instance]
		if record == nil {
			return status.Errorf(codes.NotFound, "Instance %s not found.", provider.InstanceName(scope.Project, instance))
		}
		return use(s, record)
	})
}

// instanceDocument renders one instance document under the scope lock.
func (p *Provider) instanceDocument(scope state.Scope, instance string, render func(*instanceRecord) (map[string]any, error)) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withInstance(scope, instance, func(_ *scopeState, record *instanceRecord) error {
		var err error
		document, err = render(record)
		return err
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

func (p *Provider) instancesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.instanceDocument(scope, request.Path["instance"], func(record *instanceRecord) (map[string]any, error) {
		return instanceWire(scope.Project, record), nil
	})
}

// instancesList returns the project's instances by name. ListInstances has
// no page size, and GCP returns every instance and ignores pageToken
// (recorded: a bad token lists normally).
func (p *Provider) instancesList(scope state.Scope) (*gateway.RESTResponse, error) {
	var items []map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, id := range slices.Sorted(maps.Keys(s.instances)) {
			items = append(items, instanceWire(scope.Project, s.instances[id]))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: provider.ListBody("instances", items, "")}, nil
}

// The instance fields PartialUpdateInstance applies.
const (
	fieldDisplayName = "display_name"
	fieldLabels      = "labels"
	fieldType        = "type"
)

// instanceMask maps the PartialUpdateInstance mask paths the provider
// applies, in either spelling, to their proto field.
var instanceMask = map[string]string{
	fieldDisplayName: fieldDisplayName, "displayName": fieldDisplayName, fieldLabels: fieldLabels, fieldType: fieldType,
}

// instancePaths reads an updateMask: an empty one is GCP's 501 (recorded),
// and a path the provider doesn't apply is a gap.
func instancePaths(mask string, body *adminpb.Instance) (map[string]bool, error) {
	if mask == "" {
		return nil, status.Error(codes.Unimplemented, "A non-empty 'update_mask' must be specified. An empty 'update_mask' is currently not supported.")
	}
	paths := map[string]bool{}
	for path := range strings.SplitSeq(mask, ",") {
		field, ok := instanceMask[strings.TrimSpace(path)]
		if !ok {
			return nil, gap("instance update of " + strings.TrimSpace(path))
		}
		paths[field] = true
	}
	if paths[fieldType] && body.GetType() != adminpb.Instance_PRODUCTION {
		return nil, gap("instance type " + body.GetType().String())
	}
	return paths, nil
}

// instancesUpdate is PartialUpdateInstance: it changes only the fields its
// updateMask names and answers with an operation already done (recorded).
// An empty mask is GCP's 501.
func (p *Provider) instancesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body := &adminpb.Instance{}
	if err := provider.DecodeProto(request.JSON, "instance", body, gap, "tags"); err != nil {
		return nil, err
	}
	paths, err := instancePaths(request.Query.Get("updateMask"), body)
	if err != nil {
		return nil, err
	}
	instance := request.Path["instance"]
	var target string
	var result map[string]any
	if err := p.withInstance(scope, instance, func(_ *scopeState, record *instanceRecord) error {
		if paths[fieldDisplayName] {
			record.DisplayName = body.GetDisplayName()
		}
		if paths[fieldLabels] {
			record.Labels = maps.Clone(body.GetLabels())
		}
		target = operationTarget(provider.InstanceName(scope.Project, instance), record.Cluster.Location)
		result = instanceWire(scope.Project, record)
		return nil
	}); err != nil {
		return nil, err
	}
	// The update's response instance has no state (recorded).
	delete(result, "state")
	result["@type"] = typeInstance
	id, err := p.operations.Start(scope, provider.Launch{Target: target, Verb: verbUpdateInstance, Immediate: true})
	if err != nil {
		return nil, err
	}
	return p.started(scope, id, result, nil)
}

// forget unregisters a deleted resource; the index removal drops its IAM
// policy, so a recreated resource of the same name starts without bindings.
func (p *Provider) forget(scope state.Scope, resource string) error {
	if err := p.index.Remove(serviceName, scope, resource); err != nil {
		return fmt.Errorf("unregister %s: %w", resource, err)
	}
	return nil
}

// forgetTable unpublishes a deleted table under the scope lock, which policy
// calls hold while they check, authorize, and change the policy. It doesn't
// require the instance: a table delete that finishes after its instance's
// delete must still unpublish the table.
func (p *Provider) forgetTable(scope state.Scope, instance, table string) error {
	return p.scopes.With(scope, func(*scopeState) error { //nolint:wrapcheck // forget wraps its own error
		return p.forget(scope, tableResource(scope.Project, instance, table))
	})
}

// emptyResponse is the 200 {} body GCP returns for admin deletes.
func emptyResponse() *gateway.RESTResponse {
	return &gateway.RESTResponse{JSON: map[string]any{}}
}

func (p *Provider) clustersList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.instanceDocument(scope, request.Path["instance"], func(record *instanceRecord) (map[string]any, error) {
		return map[string]any{"clusters": []any{clusterWire(scope.Project, record.Name, record.Cluster)}}, nil
	})
}

func (p *Provider) clustersGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.instanceDocument(scope, request.Path["instance"], func(record *instanceRecord) (map[string]any, error) {
		if record.Cluster.Name != request.Path["cluster"] {
			return nil, provider.NotFound(kindCluster, request.Path["cluster"])
		}
		return clusterWire(scope.Project, record.Name, record.Cluster), nil
	})
}
