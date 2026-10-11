package spanner

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized Spanner request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodConfigsGet:
		return p.configGet(request)
	case methodConfigsList:
		return p.configList()
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
	case methodInstancesGetIam, methodDatabasesGetIam:
		return p.getIam(target)
	case methodInstancesSetIam, methodDatabasesSetIam:
		return p.setIam(request, target)
	case methodDatabasesCreate:
		return p.databasesCreate(request, scope)
	case methodDatabasesGet:
		return p.databasesGet(request, scope)
	case methodDatabasesList:
		return p.databasesList(request, scope)
	case methodDatabasesUpdate:
		return p.databasesUpdate(request, scope)
	default:
		return p.databasesDelete(request, scope)
	}
}

// modeledInstanceConfigs are the regional configs the local slice models.
var modeledInstanceConfigs = []instanceConfig{
	{Name: "regional-us-central1", DisplayName: "us-central1"},
	{Name: "regional-us-east1", DisplayName: "us-east1"},
	{Name: "regional-europe-west1", DisplayName: "europe-west1"},
}

// configGet returns one modeled instance config.
func (p *Provider) configGet(request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	config := request.Path["instanceConfig"]
	for _, record := range modeledInstanceConfigs {
		if record.Name == config {
			return &gateway.RESTResponse{JSON: map[string]any{
				keyName:       "projects/" + request.Path["project"] + "/instanceConfigs/" + config,
				"displayName": record.DisplayName,
				"configType":  "GOOGLE_MANAGED",
				"replicas":    []any{map[string]any{"locationId": record.DisplayName, "type": "READ_WRITE"}},
			}}, nil
		}
	}
	return nil, notFound("instanceConfig", config)
}

// configList returns the modeled instance configs.
func (p *Provider) configList() (*gateway.RESTResponse, error) {
	items := []any{}
	for _, record := range modeledInstanceConfigs {
		items = append(items, map[string]any{
			keyName:       "configs/" + record.Name,
			"displayName": record.DisplayName,
			"configType":  "GOOGLE_MANAGED",
		})
	}
	return &gateway.RESTResponse{JSON: map[string]any{"instanceConfigs": items}}, nil
}

// instancesCreate validates and stores one instance.
func (p *Provider) instancesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		Name            string            `json:"name"`
		DisplayName     string            `json:"displayName"`
		Config          string            `json:"config"`
		NodeCount       int               `json:"nodeCount"`
		ProcessingUnits int               `json:"processingUnits"`
		Labels          map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance body")
	}
	id := strconv.Itoa(len(body.Name))
	_ = id
	name := body.Name
	if index := lastSegment(name); index >= 0 {
		name = name[index+1:]
	}
	if !uid.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", name)
	}
	config := body.Config
	if index := lastSegment(config); index >= 0 {
		config = config[index+1:]
	}
	found := false
	for _, record := range modeledInstanceConfigs {
		if record.Name == config {
			found = true
			break
		}
	}
	if !found {
		return nil, status.Errorf(codes.InvalidArgument, "Unknown instance config %q", config)
	}
	if body.NodeCount == 0 && body.ProcessingUnits == 0 {
		return nil, status.Error(codes.InvalidArgument, "An instance requires nodeCount or processingUnits")
	}
	now := p.clock()
	var created *instanceRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.instances[name] != nil {
			return status.Errorf(codes.AlreadyExists, "Instance %s already exists", name)
		}
		created = &instanceRecord{
			Name: name, Config: config, DisplayName: body.DisplayName,
			ProcessingUnits: body.ProcessingUnits, Labels: body.Labels, CreatedAt: now,
		}
		if body.NodeCount > 0 {
			created.ProcessingUnits = body.NodeCount * 1000
		}
		if created.ProcessingUnits == 0 {
			created.ProcessingUnits = 1000
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

// instanceWire renders one instance document.
func instanceWire(project string, record *instanceRecord) map[string]any {
	labels := record.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	return map[string]any{
		keyName:           instanceName(project, record.Name),
		"displayName":     record.DisplayName,
		"config":          instanceName("", record.Config),
		"nodeCount":       record.ProcessingUnits / 1000,
		"processingUnits": record.ProcessingUnits,
		"labels":          labels,
		"state":           "READY",
		"createTime":      stamp(record.CreatedAt),
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

// instancesUpdate replaces display name, labels, and node count.
func (p *Provider) instancesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.instanceLocked(scope, request.Path["instance"])
	if record == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
	}
	var body struct {
		DisplayName string            `json:"displayName"`
		NodeCount   int               `json:"nodeCount"`
		Labels      map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance update")
	}
	record.DisplayName = body.DisplayName
	record.Labels = body.Labels
	if body.NodeCount > 0 {
		record.ProcessingUnits = body.NodeCount * 1000
	}
	return &gateway.RESTResponse{JSON: instanceWire(scope.Project, record)}, nil
}

// instancesDelete removes one instance without databases.
func (p *Provider) instancesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	if p.instanceLocked(scope, instance) == nil {
		return nil, notFound(kindInstance, instance)
	}
	nonEmpty := false
	err := p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.databases {
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
		return nil, status.Errorf(codes.FailedPrecondition, "Instance %s still contains databases", instance)
	}
	if indexErr := p.index.Remove(serviceName, scope, instanceResource(scope.Project, instance)); indexErr != nil {
		return nil, fmt.Errorf("unregister the instance: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// databasesCreate validates and stores one database.
func (p *Provider) databasesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	if p.instanceLocked(scope, instance) == nil {
		return nil, notFound(kindInstance, instance)
	}
	var body struct {
		CreateStatement string   `json:"createStatement"`
		ExtraStatements []string `json:"extraStatements"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid database body")
	}
	name := databaseIDFromStatement(body.CreateStatement)
	if !uid.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid database id %q", name)
	}
	now := p.clock()
	err := p.scopes.With(scope, func(s *scopeState) error {
		key := instance + "/" + name
		if s.databases[key] != nil {
			return status.Errorf(codes.AlreadyExists, "Database %s already exists", name)
		}
		s.databases[key] = &databaseRecord{
			Instance: instance, Name: name, DDL: body.ExtraStatements,
			State: "READY", CreatedAt: now,
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: databaseResource(scope.Project, instance, name), Service: serviceName,
		Kind: kindDatabase, Parent: instanceResource(scope.Project, instance), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the database: %w", indexErr)
	}
	record := p.databaseLocked(scope, instance, name)
	return &gateway.RESTResponse{JSON: p.databaseWire(scope.Project, instance, record)}, nil
}

// databaseIDFromStatement extracts the id after DATABASE in a DDL statement.
func databaseIDFromStatement(statement string) string {
	index := -1
	for i := 0; i+9 <= len(statement); i++ {
		if statement[i:i+9] == "DATABASE " {
			index = i + 9
			break
		}
	}
	if index < 0 {
		return ""
	}
	rest := strings.TrimLeft(statement[index:], "` \t")
	for i := 0; i < len(rest); i++ {
		if rest[i] == '`' || rest[i] == ' ' {
			return rest[:i]
		}
	}
	return rest
}

// databaseLocked reads one database under the scope lock.
func (p *Provider) databaseLocked(scope state.Scope, instance, database string) *databaseRecord {
	var record *databaseRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.databases[instance+"/"+database]; return nil })
	return record
}

// databaseWire renders one database document.
func (p *Provider) databaseWire(project, instance string, record *databaseRecord) map[string]any {
	return map[string]any{
		keyName:      databaseName(project, instance, record.Name),
		"state":      record.State,
		"createTime": stamp(record.CreatedAt),
	}
}

// databasesGet returns one database.
func (p *Provider) databasesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.databaseLocked(scope, request.Path["instance"], request.Path["database"])
	if record == nil {
		return nil, notFound(kindDatabase, request.Path["database"])
	}
	return &gateway.RESTResponse{JSON: p.databaseWire(scope.Project, request.Path["instance"], record)}, nil
}

// databasesList returns the instance's databases.
func (p *Provider) databasesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.databases {
			if record.Instance == instance {
				items = append(items, p.databaseWire(scope.Project, instance, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"databases": items}}, nil
}

// databasesUpdate applies DDL statements to one database.
func (p *Provider) databasesUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.databaseLocked(scope, request.Path["instance"], request.Path["database"])
	if record == nil {
		return nil, notFound(kindDatabase, request.Path["database"])
	}
	var body struct {
		Statements []string `json:"statements"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid database update")
	}
	record.DDL = append(record.DDL, body.Statements...)
	return &gateway.RESTResponse{JSON: p.databaseWire(scope.Project, request.Path["instance"], record)}, nil
}

// databasesDelete removes one database.
func (p *Provider) databasesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance, database := request.Path["instance"], request.Path["database"]
	if p.databaseLocked(scope, instance, database) == nil {
		return nil, notFound(kindDatabase, database)
	}
	var indexErr error
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.databases, instance+"/"+database)
		indexErr = p.index.Remove(serviceName, scope, databaseResource(scope.Project, instance, database))
		return nil
	})
	if indexErr != nil {
		return nil, fmt.Errorf("unregister the database: %w", indexErr)
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// getIam returns the stored policy of an instance or database.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Spanner policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

// setIam stores the policy of an instance or database.
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write the Spanner policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
