package spanner

import (
	"encoding/json"
	"fmt"
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
	case methodInstancesGetIam, methodDatabasesGetIam:
		return p.getIam(target)
	case methodInstancesSetIam, methodDatabasesSetIam:
		return p.setIam(request, target)
	default:
		return p.executeInstances(call, request, scope)
	}
}

// executeInstances dispatches the instance methods, then database methods.
func (p *Provider) executeInstances(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
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
	default:
		return p.executeDatabases(call, request, scope)
	}
}

// executeDatabases dispatches the database and backup methods.
func (p *Provider) executeDatabases(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodDatabasesCreate:
		return p.databasesCreate(request, scope)
	case methodDatabasesGet:
		return p.databasesGet(request, scope)
	case methodDatabasesList:
		return p.databasesList(request, scope)
	case methodDatabasesUpdate:
		return p.databasesUpdate(request, scope)
	case methodDatabasesApplyDdl:
		return p.databasesApplyDdl(request, scope)
	case methodDatabasesGetDdl:
		return p.databasesGetDdl(request, scope)
	case methodBackupsList:
		return p.backupsList(request, scope)
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
				keyName:        "projects/" + request.Path["project"] + "/instanceConfigs/" + config,
				keyDisplayName: record.DisplayName,
				"configType":   "GOOGLE_MANAGED",
				"replicas":     []any{map[string]any{"locationId": record.DisplayName, "type": "READ_WRITE"}},
			}}, nil
		}
	}
	return nil, notFound("instanceConfig", config)
}

// configList returns the modeled instance configs.
func (p *Provider) configList() (*gateway.RESTResponse, error) {
	items := make([]any, 0, len(modeledInstanceConfigs))
	for _, record := range modeledInstanceConfigs {
		items = append(items, map[string]any{
			keyName:        "configs/" + record.Name,
			keyDisplayName: record.DisplayName,
			"configType":   "GOOGLE_MANAGED",
		})
	}
	return &gateway.RESTResponse{JSON: map[string]any{"instanceConfigs": items}}, nil
}

// instanceRequest is the decoded instances.create body.
type instanceRequest struct {
	InstanceID string `json:"instanceId"`
	Instance   struct {
		Name            string            `json:"name"`
		DisplayName     string            `json:"displayName"`
		Config          string            `json:"config"`
		NodeCount       int               `json:"nodeCount"`
		ProcessingUnits int               `json:"processingUnits"`
		Labels          map[string]string `json:"labels"`
	} `json:"instance"`
}

// parseInstanceRequest validates instances.create and returns the new record.
func parseInstanceRequest(request *gateway.RESTRequest) (*instanceRecord, error) {
	var body instanceRequest
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance body")
	}
	name := body.InstanceID
	if name == "" {
		name = body.Instance.Name
	}
	name = name[lastSegment(name)+1:]
	if !uid.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid instance id %q", name)
	}
	config := body.Instance.Config
	config = config[lastSegment(config)+1:]
	if configLocation(config) == "" {
		return nil, status.Errorf(codes.InvalidArgument, "Unknown instance config %q", config)
	}
	units := body.Instance.ProcessingUnits
	if body.Instance.NodeCount > 0 {
		units = body.Instance.NodeCount * 1000
	}
	if units == 0 {
		return nil, status.Error(codes.InvalidArgument, "An instance requires nodeCount or processingUnits")
	}
	return &instanceRecord{
		Name: name, Config: config, DisplayName: body.Instance.DisplayName,
		ProcessingUnits: units, Labels: body.Instance.Labels,
	}, nil
}

// configLocation returns a modeled config's location, or "" when unknown.
func configLocation(config string) string {
	for _, record := range modeledInstanceConfigs {
		if record.Name == config {
			return record.DisplayName
		}
	}
	return ""
}

// instancesCreate validates and stores one instance, returning a done LRO.
func (p *Provider) instancesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	created, err := parseInstanceRequest(request)
	if err != nil {
		return nil, err
	}
	now := p.clock()
	created.CreatedAt, created.UpdatedAt = now, now
	err = p.locked(scope, func(s *scopeState) error {
		if s.instances[created.Name] != nil {
			return status.Errorf(codes.AlreadyExists, "Instance already exists: %s", instanceName(scope.Project, created.Name))
		}
		s.instances[created.Name] = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: instanceResource(scope.Project, created.Name), Service: serviceName,
		Kind: kindInstance, Parent: projectResource(scope.Project), Scope: scope,
	}); indexErr != nil {
		return nil, fmt.Errorf("index the instance: %w", indexErr)
	}
	wire := instanceWire(scope.Project, created)
	return &gateway.RESTResponse{JSON: doneOperation(instanceName(scope.Project, created.Name),
		typeCreateInstanceMetadata, map[string]any{
			kindInstance: wire, "startTime": stamp(now), "endTime": stamp(now),
		}, typeInstance, wire)}, nil
}

// lastSegment finds the final '/' separator, or -1.
func lastSegment(value string) int {
	return strings.LastIndexByte(value, '/')
}

// instanceWire renders one instance document with the recorded field set.
func instanceWire(project string, record *instanceRecord) map[string]any {
	labels := record.Labels
	if labels == nil {
		labels = map[string]string{}
	}
	location := configLocation(record.Config)
	wire := map[string]any{
		keyName:           instanceName(project, record.Name),
		keyDisplayName:    record.DisplayName,
		"config":          "projects/" + project + "/instanceConfigs/" + record.Config,
		"processingUnits": record.ProcessingUnits,
		"labels":          labels,
		"state":           "READY",
		"createTime":      stamp(record.CreatedAt),
		"updateTime":      stamp(record.UpdatedAt),
		// Local defaults, not recorded values: the recording compares only
		// the presence of these fields.
		"instanceType":              "PROVISIONED",
		"edition":                   "STANDARD",
		"defaultBackupScheduleType": "AUTOMATIC",
		"resourceLocation":          location,
		"replicaComputeCapacity": []any{map[string]any{
			"replicaSelection": map[string]any{"location": location},
			"processingUnits":  record.ProcessingUnits,
		}},
	}
	// proto3 JSON omits a zero nodeCount; sub-node instances (the recorded
	// 100 processing units) therefore carry no nodeCount field.
	if nodes := record.ProcessingUnits / 1000; nodes > 0 {
		wire["nodeCount"] = nodes
	}
	return wire
}

// instanceLocked reads one instance under the scope lock.
func (p *Provider) instanceLocked(scope state.Scope, instance string) *instanceRecord {
	var record *instanceRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.instances[instance]; return nil })
	return record
}

// instancesGet returns one instance.
func (p *Provider) instancesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var wire map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.instances[request.Path["instance"]]; record != nil {
			wire = instanceWire(scope.Project, record)
		}
		return nil
	})
	if wire == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
	}
	return &gateway.RESTResponse{JSON: wire}, nil
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
	var body struct {
		DisplayName string            `json:"displayName"`
		NodeCount   int               `json:"nodeCount"`
		Labels      map[string]string `json:"labels"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid instance update")
	}
	now := p.clock()
	var wire map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record := s.instances[request.Path["instance"]]
		if record == nil {
			return nil
		}
		record.DisplayName, record.Labels, record.UpdatedAt = body.DisplayName, body.Labels, now
		if body.NodeCount > 0 {
			record.ProcessingUnits = body.NodeCount * 1000
		}
		wire = instanceWire(scope.Project, record)
		return nil
	})
	if wire == nil {
		return nil, notFound(kindInstance, request.Path["instance"])
	}
	return &gateway.RESTResponse{JSON: wire}, nil
}

// instancesDelete removes one instance without databases; GCP answers 200 {}.
func (p *Provider) instancesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	err := p.locked(scope, func(s *scopeState) error {
		if s.instances[instance] == nil {
			return notFound(kindInstance, instance)
		}
		for _, record := range s.databases {
			if record.Instance == instance {
				return status.Errorf(codes.FailedPrecondition, "Instance %s still contains databases", instance)
			}
		}
		delete(s.instances, instance)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Remove(serviceName, scope, instanceResource(scope.Project, instance)); indexErr != nil {
		return nil, fmt.Errorf("unregister the instance: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// backupsList returns the instance's modeled backups: none exist locally.
func (p *Provider) backupsList(_ *gateway.RESTRequest, _ state.Scope) (*gateway.RESTResponse, error) {
	return &gateway.RESTResponse{JSON: map[string]any{"backups": []any{}}}, nil
}

// getIam returns the stored policy of an instance or database.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the Spanner policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
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
