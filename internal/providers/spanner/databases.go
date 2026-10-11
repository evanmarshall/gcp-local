package spanner

import (
	"encoding/json"
	"fmt"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Local database defaults. They are plausible GCP defaults, not recorded
// values: the recording compares only the presence of these fields.
const (
	defaultDialect          = "GOOGLE_STANDARD_SQL"
	defaultVersionRetention = "1h"
	defaultEncryptionType   = "GOOGLE_DEFAULT_ENCRYPTION"
)

// databasesCreate validates the DDL, stores one database, and returns a done LRO.
func (p *Provider) databasesCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	var body struct {
		CreateStatement string   `json:"createStatement"`
		ExtraStatements []string `json:"extraStatements"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid database body")
	}
	name := databaseIDFromStatement(body.CreateStatement)
	if !databaseID.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid database id %q", name)
	}
	tables, err := applyDDL(nil, body.ExtraStatements)
	if err != nil {
		return nil, err
	}
	record := &databaseRecord{Instance: instance, Name: name, Tables: tables, State: "READY", CreatedAt: p.clock()}
	var location string
	err = p.locked(scope, func(s *scopeState) error {
		parent := s.instances[instance]
		if parent == nil {
			return notFound(kindInstance, instance)
		}
		if s.databases[instance+"/"+name] != nil {
			return status.Errorf(codes.AlreadyExists, "Database already exists: %s", databaseName(scope.Project, instance, name))
		}
		location = configLocation(parent.Config)
		s.databases[instance+"/"+name] = record
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
	full := databaseName(scope.Project, instance, name)
	response := databaseCore(full, record)
	response["resourceLocationId"] = location
	return &gateway.RESTResponse{JSON: doneOperation(full, typeCreateDatabaseMetadata,
		map[string]any{kindDatabase: full}, typeDatabase, response)}, nil
}

// databaseCore renders the fields shared by the create response and GET.
func databaseCore(name string, record *databaseRecord) map[string]any {
	return map[string]any{
		keyName:                  name,
		"state":                  record.State,
		"createTime":             stamp(record.CreatedAt),
		"databaseDialect":        defaultDialect,
		"earliestVersionTime":    stamp(record.CreatedAt),
		"versionRetentionPeriod": defaultVersionRetention,
	}
}

// databaseWire renders the recorded GET document, which carries
// encryptionInfo but not resourceLocationId.
func databaseWire(project string, record *databaseRecord) map[string]any {
	wire := databaseCore(databaseName(project, record.Instance, record.Name), record)
	wire["encryptionInfo"] = []any{map[string]any{"encryptionType": defaultEncryptionType}}
	return wire
}

// databaseLocked reads one database under the scope lock.
func (p *Provider) databaseLocked(scope state.Scope, instance, database string) *databaseRecord {
	var record *databaseRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.databases[instance+"/"+database]; return nil })
	return record
}

// withDatabase runs use on one stored database under the scope lock.
func (p *Provider) withDatabase(request *gateway.RESTRequest, scope state.Scope, use func(*databaseRecord) error) error {
	instance, database := request.Path["instance"], request.Path["database"]
	return p.locked(scope, func(s *scopeState) error {
		record := s.databases[instance+"/"+database]
		if record == nil {
			return notFound(kindDatabase, database)
		}
		return use(record)
	})
}

// databasesGet returns one database.
func (p *Provider) databasesGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var wire map[string]any
	if err := p.withDatabase(request, scope, func(record *databaseRecord) error {
		wire = databaseWire(scope.Project, record)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: wire}, nil
}

// databasesList returns the instance's databases.
func (p *Provider) databasesList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance := request.Path["instance"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.databases {
			if record.Instance == instance {
				items = append(items, databaseWire(scope.Project, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"databases": items}}, nil
}

// databasesUpdate is an explicit gap: database field updates are not modeled.
func (p *Provider) databasesUpdate(_ *gateway.RESTRequest, _ state.Scope) (*gateway.RESTResponse, error) {
	return nil, status.Error(codes.Unimplemented, "gcp-local does not model Spanner database field updates")
}

// databasesDelete drops one database; GCP answers 200 {}.
func (p *Provider) databasesDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	instance, database := request.Path["instance"], request.Path["database"]
	err := p.locked(scope, func(s *scopeState) error {
		if s.databases[instance+"/"+database] == nil {
			return notFound(kindDatabase, database)
		}
		delete(s.databases, instance+"/"+database)
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Remove(serviceName, scope, databaseResource(scope.Project, instance, database)); indexErr != nil {
		return nil, fmt.Errorf("unregister the database: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// databasesGetDdl returns the canonical schema statements.
func (p *Provider) databasesGetDdl(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var statements []string
	if err := p.withDatabase(request, scope, func(record *databaseRecord) error {
		statements = renderDDL(record.Tables)
		return nil
	}); err != nil {
		return nil, err
	}
	body := map[string]any{}
	if len(statements) > 0 { // proto3 JSON omits an empty repeated field.
		body["statements"] = statements
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// databasesApplyDdl applies a DDL batch atomically and returns a done LRO.
func (p *Provider) databasesApplyDdl(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		Statements []string `json:"statements"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid DDL body")
	}
	if len(body.Statements) == 0 {
		return nil, status.Error(codes.InvalidArgument, "At least one DDL statement is required")
	}
	if err := p.withDatabase(request, scope, func(record *databaseRecord) error {
		tables, err := applyDDL(record.Tables, body.Statements)
		if err == nil {
			record.Tables = tables
		}
		return err
	}); err != nil {
		return nil, err
	}
	full := databaseName(scope.Project, request.Path["instance"], request.Path["database"])
	return &gateway.RESTResponse{JSON: doneOperation(full, typeUpdateDdlMetadata, map[string]any{
		kindDatabase: full, "statements": body.Statements, "commitTimestamps": []any{stamp(p.clock())},
	}, typeEmpty, nil)}, nil
}
