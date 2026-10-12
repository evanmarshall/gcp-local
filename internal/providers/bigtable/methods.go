package bigtable

import (
	"context"
	"encoding/json"
	"fmt"

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
	case methodInstancesGetIam, methodTablesGetIam:
		if err := p.policyTarget(call, request, target); err != nil {
			return nil, err
		}
		return p.getIam(target)
	case methodInstancesSetIam, methodTablesSetIam:
		if err := p.policyTarget(call, request, target); err != nil {
			return nil, err
		}
		return p.setIam(request, target)
	default:
		return p.mutate(call, request, scope)
	}
}

// policyTarget requires an existing table for table policy methods; a table
// that was never published authorizes against its instance and has no policy.
func (p *Provider) policyTarget(call gateway.Call, request *gateway.RESTRequest, target iam.Target) error {
	if call.Method != methodTablesGetIam && call.Method != methodTablesSetIam {
		return nil
	}
	if target.Name != tableResource(request.Path["project"], request.Path["instance"], request.Path["table"]) {
		return provider.NotFound(kindTable, request.Path["table"])
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

// instanceCreateBody is the CreateInstance request; clusters map id to spec.
type instanceCreateBody struct {
	InstanceID string `json:"instanceId"`
	Instance   struct {
		DisplayName string            `json:"displayName"`
		Type        string            `json:"type"`
		Edition     string            `json:"edition"`
		Labels      map[string]string `json:"labels"`
	} `json:"instance"`
	Clusters map[string]struct {
		Location           string `json:"location"`
		ServeNodes         int    `json:"serveNodes"`
		DefaultStorageType string `json:"defaultStorageType"`
	} `json:"clusters"`
}

// instancesCreate validates and stores one instance with its cluster, then
// returns the completed long-running operation.
func (p *Provider) instancesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record, err := p.parseInstance(request)
	if err != nil {
		return nil, err
	}
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.instances[record.Name] != nil {
			return status.Errorf(codes.AlreadyExists, "Instance %s already exists", provider.InstanceName(scope.Project, record.Name))
		}
		s.instances[record.Name] = record
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: instanceResource(scope.Project, record.Name), Service: serviceName,
		Kind: kindInstance, Parent: provider.ProjectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the instance: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: p.createOperation(scope.Project, record)}, nil
}

// parseInstance decodes and validates one CreateInstance request.
func (p *Provider) parseInstance(request *gateway.RESTRequest) (*instanceRecord, error) {
	var body instanceCreateBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance body")
	}
	if !uid.MatchString(body.InstanceID) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", body.InstanceID)
	}
	if len(body.Clusters) != 1 {
		return nil, status.Error(codes.InvalidArgument, "Exactly one cluster is required")
	}
	var cluster *clusterRecord
	for id, spec := range body.Clusters {
		cluster = &clusterRecord{
			Name: id, Location: lastNamed(spec.Location),
			ServeNodes: spec.ServeNodes, StorageType: valueOr(spec.DefaultStorageType, "SSD"),
		}
	}
	if !uid.MatchString(cluster.Name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid cluster id %q", cluster.Name)
	}
	if cluster.ServeNodes <= 0 || cluster.Location == "" {
		return nil, status.Error(codes.InvalidArgument, "A cluster requires location and serveNodes")
	}
	return &instanceRecord{
		Name: body.InstanceID, DisplayName: body.Instance.DisplayName,
		Type:    valueOr(body.Instance.Type, "PRODUCTION"),
		Edition: valueOr(body.Instance.Edition, defaultEdition),
		Labels:  body.Instance.Labels, Cluster: cluster, CreatedAt: p.clock(),
	}, nil
}

// createOperation renders the completed CreateInstance operation. Bigtable
// names operations under the cluster's zone.
func (p *Provider) createOperation(project string, record *instanceRecord) map[string]any {
	number := p.operations.Add(1)
	result := instanceSummary(project, record)
	result["@type"] = typeInstance
	return map[string]any{
		keyName: fmt.Sprintf("operations/%s/locations/%s/operations/%d",
			provider.InstanceName(project, record.Name), record.Cluster.Location, number),
		"metadata": map[string]any{
			"@type":       typeCreateInstanceMetadata,
			"requestTime": provider.Stamp(record.CreatedAt),
			"finishTime":  provider.Stamp(record.CreatedAt),
		},
		"done":     true,
		"response": result,
	}
}

// valueOr returns value, or fallback when value is empty.
func valueOr(value, fallback string) string {
	if value == "" {
		return fallback
	}
	return value
}

// lastIndex finds the final separator, or -1.
func lastIndex(value string, separator byte) int {
	for i := len(value) - 1; i >= 0; i-- {
		if value[i] == separator {
			return i
		}
	}
	return -1
}

// lastNamed returns the id after the final '/', or the value itself.
func lastNamed(value string) string {
	if index := lastIndex(value, '/'); index >= 0 {
		return value[index+1:]
	}
	return value
}

// instanceSummary renders the instance fields of a create-operation response.
func instanceSummary(project string, record *instanceRecord) map[string]any {
	labels := record.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return map[string]any{
		keyName:       provider.InstanceName(project, record.Name),
		"displayName": record.DisplayName,
		"type":        record.Type,
		"edition":     record.Edition,
		"labels":      labels,
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
	if index := lastIndex(zone, '-'); index > 0 {
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

// instanceLocked returns a copy of one instance read under the scope lock.
func (p *Provider) instanceLocked(scope state.Scope, instance string) *instanceRecord {
	var record *instanceRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if stored := s.instances[instance]; stored != nil {
			copied := *stored // Updates replace fields; a shallow copy is a stable read.
			record = &copied
		}
		return nil
	})
	return record
}

func (p *Provider) instancesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, provider.NotFound(kindInstance, request.Path["instance"])
	}
	return &gateway.RESTResponse{JSON: instanceWire(scope.Project, record)}, nil
}

// instancesList returns the project's instances.
func (p *Provider) instancesList(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.instances {
			items = append(items, instanceWire(scope.Project, record))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"instances": items}}, nil
}

// instancesUpdate replaces display name and labels.
func (p *Provider) instancesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		DisplayName string            `json:"displayName"`
		Labels      map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance update")
	}
	var wire map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.instances[request.Path["instance"]]
		if record == nil {
			return provider.NotFound(kindInstance, request.Path["instance"])
		}
		record.DisplayName = body.DisplayName
		record.Labels = body.Labels
		wire = instanceWire(scope.Project, record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	return &gateway.RESTResponse{JSON: wire}, nil
}

// forget unregisters a deleted resource and drops its IAM policy so a
// recreated resource of the same name starts without bindings.
func (p *Provider) forget(scope state.Scope, resource string) error {
	if err := p.index.Remove(serviceName, scope, resource); err != nil {
		return fmt.Errorf("unregister %s: %w", resource, err)
	}
	p.engine.Forget(resource)
	return nil
}

// emptyResponse is the 200 {} body GCP returns for admin deletes.
func emptyResponse() *gateway.RESTResponse {
	return &gateway.RESTResponse{JSON: map[string]any{}}
}

func (p *Provider) clustersList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, provider.NotFound(kindInstance, request.Path["instance"])
	}
	return &gateway.RESTResponse{JSON: map[string]any{"clusters": []any{clusterWire(scope.Project, record.Name, record.Cluster)}}}, nil
}

func (p *Provider) clustersGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, provider.NotFound(kindInstance, request.Path["instance"])
	}
	if record.Cluster.Name != request.Path["cluster"] {
		return nil, provider.NotFound(kindCluster, request.Path["cluster"])
	}
	return &gateway.RESTResponse{JSON: clusterWire(scope.Project, record.Name, record.Cluster)}, nil
}

// getIam returns the stored policy of an instance or table.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Bigtable policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// setIam stores the policy of an instance or table.
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}
