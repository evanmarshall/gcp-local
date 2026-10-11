package bigtable

import (
	"encoding/json"
	"fmt"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized Bigtable request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodInstancesCreate:
		return p.instancesCreate(request, scope)
	case methodInstancesGet:
		return p.instancesGet(request, scope)
	case methodInstancesList:
		return p.instancesList(scope)
	case methodInstancesUpdate:
		return p.instancesUpdate(request, scope)
	case methodInstancesDelete:
		return p.instancesDelete(request, scope)
	case methodInstancesGetIam, methodTablesGetIam:
		return p.getIam(target)
	case methodInstancesSetIam, methodTablesSetIam:
		return p.setIam(request, target)
	default:
		return p.executeChild(call, request, scope)
	}
}

// executeChild dispatches one authorized cluster or table request.
func (p *Provider) executeChild(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodClustersList:
		return p.clustersList(request, scope)
	case methodClustersGet:
		return p.clustersGet(request, scope)
	case methodTablesCreate:
		return p.tablesCreate(request, scope)
	case methodTablesGet:
		return p.tablesGet(request, scope)
	case methodTablesList:
		return p.tablesList(request, scope)
	case methodTablesUpdate:
		return p.tablesUpdate(request, scope)
	case methodTablesModifyFam:
		return p.tablesModifyFamilies(request, scope)
	default:
		return p.tablesDelete(request, scope)
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
			return status.Errorf(codes.AlreadyExists, "Instance %s already exists", instanceName(scope.Project, record.Name))
		}
		s.instances[record.Name] = record
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: instanceResource(scope.Project, record.Name), Service: serviceName,
		Kind: kindInstance, Parent: projectResource(scope.Project), Scope: scope,
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
			instanceName(project, record.Name), record.Cluster.Location, number),
		"metadata": map[string]any{
			"@type":       typeCreateInstanceMetadata,
			"requestTime": stamp(record.CreatedAt),
			"finishTime":  stamp(record.CreatedAt),
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
		keyName:       instanceName(project, record.Name),
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
	wire["createTime"] = stamp(record.CreatedAt)
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

// clusterWire renders one cluster document.
func clusterWire(project, instance string, record *clusterRecord) map[string]any {
	return map[string]any{
		keyName:              clusterName(project, instance, record.Name),
		"location":           "projects/" + project + "/locations/" + record.Location,
		"serveNodes":         record.ServeNodes,
		"defaultStorageType": record.StorageType,
		"state":              "READY",
	}
}

// instanceLocked reads one instance under the scope lock.
func (p *Provider) instanceLocked(scope state.Scope, instance string) *instanceRecord {
	var record *instanceRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.instances[instance]; return nil })
	return record
}

// instancesGet returns one instance.
func (p *Provider) instancesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
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
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
	}
	var body struct {
		DisplayName string            `json:"displayName"`
		Labels      map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance update")
	}
	record.DisplayName = body.DisplayName
	record.Labels = body.Labels
	return &gateway.RESTResponse{JSON: instanceWire(scope.Project, record)}, nil
}

// instancesDelete removes one instance and, as in GCP, all of its tables.
func (p *Provider) instancesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	var tables []string
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.instances[instance] == nil {
			return notFound(kindInstance, instance)
		}
		for key, record := range s.tables {
			if record.Instance == instance {
				tables = append(tables, record.Name)
				delete(s.tables, key)
			}
		}
		delete(s.instances, instance)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	for _, table := range tables {
		if forgetErr := p.forget(scope, tableResource(scope.Project, instance, table)); forgetErr != nil {
			return nil, forgetErr
		}
	}
	if forgetErr := p.forget(scope, instanceResource(scope.Project, instance)); forgetErr != nil {
		return nil, forgetErr
	}
	return emptyResponse(), nil
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

// clustersList returns the instance's clusters.
func (p *Provider) clustersList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
	}
	return &gateway.RESTResponse{JSON: map[string]any{"clusters": []any{clusterWire(scope.Project, record.Name, record.Cluster)}}}, nil
}

// clustersGet returns one cluster.
func (p *Provider) clustersGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
	}
	if record.Cluster.Name != request.Path["cluster"] {
		return nil, notFound(kindCluster, request.Path["cluster"])
	}
	return &gateway.RESTResponse{JSON: clusterWire(scope.Project, record.Name, record.Cluster)}, nil
}

// tablesCreate validates and stores one table with its column families.
func (p *Provider) tablesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	if p.instanceLocked(scope, instance) == nil {
		return nil, notFound(kindInstance, instance)
	}
	var body struct {
		TableID string `json:"tableId"`
		Table   struct {
			Granularity    string `json:"granularity"`
			ColumnFamilies map[string]struct {
				GCRule map[string]any `json:"gcRule"`
			} `json:"columnFamilies"`
		} `json:"table"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid table body")
	}
	name := body.TableID
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "A table requires tableId")
	}
	if !tableID.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid table id %q", name)
	}
	granularity := body.Table.Granularity
	if granularity == "" {
		granularity = "MILLIS"
	}
	now := p.clock()
	families := map[string]*familyRecord{}
	for family, spec := range body.Table.ColumnFamilies {
		families[family] = &familyRecord{Name: family, GCRule: spec.GCRule}
	}
	var created *tableRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := instance + "/" + name
		if s.tables[key] != nil {
			return status.Errorf(codes.AlreadyExists, "Table %s already exists", tableName(scope.Project, instance, name))
		}
		created = &tableRecord{
			Instance: instance, Name: name, Families: families,
			Granularity: granularity, CreatedAt: now,
		}
		s.tables[key] = created
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: tableResource(scope.Project, instance, name), Service: serviceName,
		Kind: kindTable, Parent: instanceResource(scope.Project, instance), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the table: %w", indexErr)
	}
	record := p.tableLocked(scope, instance, name)
	return &gateway.RESTResponse{JSON: tableWire(scope.Project, instance, record)}, nil
}

// tableLocked reads one table under the scope lock.
func (p *Provider) tableLocked(scope state.Scope, instance, table string) *tableRecord {
	var record *tableRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.tables[instance+"/"+table]; return nil })
	return record
}

// tableWire renders one table in the default (SCHEMA_VIEW) shape: a family
// without a garbage-collection rule renders as an empty object.
func tableWire(project, instance string, record *tableRecord) map[string]any {
	families := map[string]any{}
	for _, family := range record.Families {
		wire := map[string]any{}
		if len(family.GCRule) > 0 {
			wire["gcRule"] = family.GCRule
		}
		families[family.Name] = wire
	}
	return map[string]any{
		keyName:          tableName(project, instance, record.Name),
		"columnFamilies": families,
		"granularity":    record.Granularity,
	}
}

// tablesGet returns one table.
func (p *Provider) tablesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.tableLocked(scope, request.Path["instance"], request.Path["table"])
	if record == nil {
		return nil, notFound(kindTable, request.Path["table"])
	}
	return &gateway.RESTResponse{JSON: tableWire(scope.Project, request.Path["instance"], record)}, nil
}

// tablesList returns the instance's tables.
func (p *Provider) tablesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.tables {
			if record.Instance == instance {
				items = append(items, tableWire(scope.Project, instance, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"tables": items}}, nil
}

// tablesUpdate patches one table's families through the modify shape.
func (p *Provider) tablesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.tablesModifyFamilies(request, scope)
}

// tablesModifyFamilies applies create/update/drop column-family modifications.
func (p *Provider) tablesModifyFamilies(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance, table := request.Path["instance"], request.Path["table"]
	var body struct {
		Modifications []struct {
			ID     string `json:"id"`
			Create struct {
				GCRule map[string]any `json:"gcRule"`
			} `json:"create"`
			Update struct {
				GCRule map[string]any `json:"gcRule"`
			} `json:"update"`
			Drop bool `json:"drop"`
		} `json:"modifications"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid column-family modifications")
	}
	var wire map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.tables[instance+"/"+table]
		if record == nil {
			return notFound(kindTable, table)
		}
		for _, mod := range body.Modifications {
			applyModification(record, mod.ID, mod.Drop, valueOrRule(mod.Create.GCRule, mod.Update.GCRule))
		}
		wire = tableWire(scope.Project, instance, record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	return &gateway.RESTResponse{JSON: wire}, nil
}

// applyModification creates, updates, or drops one column family.
func applyModification(record *tableRecord, family string, drop bool, rule map[string]any) {
	if drop {
		delete(record.Families, family)
		return
	}
	record.Families[family] = &familyRecord{Name: family, GCRule: rule}
}

// valueOrRule returns the create rule, or the update rule when absent.
func valueOrRule(create, update map[string]any) map[string]any {
	if create != nil {
		return create
	}
	return update
}

// tablesDelete removes one table.
func (p *Provider) tablesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance, table := request.Path["instance"], request.Path["table"]
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := instance + "/" + table
		if s.tables[key] == nil {
			return notFound(kindTable, table)
		}
		delete(s.tables, key)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // already a wire status
	}
	if forgetErr := p.forget(scope, tableResource(scope.Project, instance, table)); forgetErr != nil {
		return nil, forgetErr
	}
	return emptyResponse(), nil
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
