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
	case methodClustersList:
		return p.clustersList(request, scope)
	case methodClustersGet:
		return p.clustersGet(request, scope)
	case methodInstancesGetIam, methodTablesGetIam:
		return p.getIam(target)
	case methodInstancesSetIam, methodTablesSetIam:
		return p.setIam(request, target)
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

// instancesCreate validates and stores one instance with its cluster.
func (p *Provider) instancesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		InstanceID string `json:"instanceId"`
		Instance   struct {
			Name        string            `json:"name"`
			DisplayName string            `json:"displayName"`
			Type        string            `json:"type"`
			Labels      map[string]string `json:"labels"`
		} `json:"instance"`
		Clusters []struct {
			Name               string `json:"name"`
			ClusterID          string `json:"clusterId"`
			Location           string `json:"location"`
			ServeNodes         int    `json:"serveNodes"`
			DefaultStorageType string `json:"defaultStorageType"`
		} `json:"clusters"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance body")
	}
	name := body.InstanceID
	if name == "" {
		name = body.Instance.Name
	}
	if index := lastSegment(name); index >= 0 {
		name = name[index+1:]
	}
	if !uid.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", name)
	}
	if len(body.Clusters) != 1 {
		return nil, status.Error(codes.InvalidArgument, "Exactly one cluster is required")
	}
	cluster := body.Clusters[0]
	clusterID := cluster.ClusterID
	if clusterID == "" {
		clusterID = lastNamed(cluster.Name)
	}
	if !uid.MatchString(clusterID) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid cluster id %q", clusterID)
	}
	if cluster.ServeNodes <= 0 {
		return nil, status.Error(codes.InvalidArgument, "A cluster requires serveNodes")
	}
	location := cluster.Location
	if index := lastSegment(location); index >= 0 {
		location = location[index+1:]
	}
	storage := cluster.DefaultStorageType
	if storage == "" {
		storage = "SSD"
	}
	instanceType := body.Instance.Type
	if instanceType == "" {
		instanceType = "PRODUCTION"
	}
	now := p.clock()
	var created *instanceRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.instances[name] != nil {
			return status.Errorf(codes.AlreadyExists, "Instance %s already exists", name)
		}
		created = &instanceRecord{
			Name: name, DisplayName: body.Instance.DisplayName, Type: instanceType,
			Labels: body.Instance.Labels, CreatedAt: now,
			Cluster: &clusterRecord{
				Name: clusterID, Location: location,
				ServeNodes: cluster.ServeNodes, StorageType: storage,
				CreatedAt: now,
			},
		}
		s.instances[name] = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: instanceResource(scope.Project, name), Service: serviceName,
		Kind: kindInstance, Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the instance: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: instanceWire(scope.Project, created)}, nil
}

// lastSegment finds the final '/' separator, or -1.
func lastSegment(value string) int {
	for i := len(value) - 1; i >= 0; i-- {
		if value[i] == '/' {
			return i
		}
	}
	return -1
}

// lastNamed returns the id after the final '/', or the value itself.
func lastNamed(value string) string {
	if index := lastSegment(value); index >= 0 {
		return value[index+1:]
	}
	return value
}

// instanceWire renders one instance document.
func instanceWire(project string, record *instanceRecord) map[string]any {
	labels := record.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return map[string]any{
		keyName:       instanceName(project, record.Name),
		"displayName": record.DisplayName,
		"type":        record.Type,
		"labels":      labels,
		"state":       "READY",
		"createTime":  stamp(record.CreatedAt),
	}
}

// clusterWire renders one cluster document.
func clusterWire(project, instance string, record *clusterRecord) map[string]any {
	return map[string]any{
		keyName:              clusterName(project, instance, record.Name),
		"location":           "projects/" + project + "/locations/" + record.Location,
		"serveNodes":         record.ServeNodes,
		"defaultStorageType": record.StorageType,
		"state":              "READY",
		"createTime":         stamp(record.CreatedAt),
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

// instancesDelete removes one instance without tables.
func (p *Provider) instancesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	if p.instanceLocked(scope, instance) == nil {
		return nil, notFound(kindInstance, instance)
	}
	nonEmpty := false
	err := p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.tables {
			if record.Instance == instance {
				nonEmpty = true
				return nil
			}
		}
		delete(s.instances, instance)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("remove the instance: %w", err)
	}
	if nonEmpty {
		return nil, status.Errorf(codes.FailedPrecondition, "Instance %s still contains tables", instance)
	}
	if indexErr := p.index.Remove(serviceName, scope, instanceResource(scope.Project, instance)); indexErr != nil {
		return nil, fmt.Errorf("unregister the instance: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: 204}, nil
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
	if !uid.MatchString(name) {
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
			return status.Errorf(codes.AlreadyExists, "Table %s already exists", name)
		}
		created = &tableRecord{
			Instance: instance, Name: name, Families: families,
			Granularity: granularity, CreatedAt: now,
		}
		s.tables[key] = created
		return nil
	})
	if err != nil {
		return nil, err
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

// tableWire renders one table document.
func tableWire(project, instance string, record *tableRecord) map[string]any {
	families := map[string]any{}
	for _, family := range record.Families {
		rule := family.GCRule
		if rule == nil {
			rule = map[string]any{}
		}
		families[family.Name] = map[string]any{"gcRule": rule}
	}
	return map[string]any{
		keyName:          tableName(project, instance, record.Name),
		"columnFamilies": families,
		"granularity":    record.Granularity,
		"createTime":     stamp(record.CreatedAt),
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
	record := p.tableLocked(scope, instance, table)
	if record == nil {
		return nil, notFound(kindTable, table)
	}
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
	for _, mod := range body.Modifications {
		if mod.Drop {
			delete(record.Families, mod.ID)
			continue
		}
		rule := mod.Create.GCRule
		if rule == nil {
			rule = mod.Update.GCRule
		}
		record.Families[mod.ID] = &familyRecord{Name: mod.ID, GCRule: rule}
	}
	return &gateway.RESTResponse{JSON: tableWire(scope.Project, instance, record)}, nil
}

// tablesDelete removes one table.
func (p *Provider) tablesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance, table := request.Path["instance"], request.Path["table"]
	if p.tableLocked(scope, instance, table) == nil {
		return nil, notFound(kindTable, table)
	}
	var indexErr error
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.tables, instance+"/"+table)
		indexErr = p.index.Remove(serviceName, scope, tableResource(scope.Project, instance, table))
		return nil
	})
	if indexErr != nil {
		return nil, fmt.Errorf("unregister the table: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// getIam returns the stored policy of an instance or table.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Bigtable policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
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
		return nil, status.Errorf(codes.Internal, "write the Bigtable policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
