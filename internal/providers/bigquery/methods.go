package bigquery

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"maps"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized BigQuery request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodDatasetsCreate:
		return p.datasetsCreate(request, scope)
	case methodDatasetsGet:
		return p.datasetsGet(request, scope)
	case methodDatasetsList:
		return p.datasetsList(scope)
	case methodDatasetsUpdate:
		return p.datasetsUpdate(request, scope)
	case methodDatasetsGetIam, methodDatasetsSetIam:
		// Recorded: dataset IAM methods need allowlisting; access is per table.
		return provider.WireError(400, "INVALID_ARGUMENT", "This feature requires allowlisting."), nil
	case methodTablesGetIam, methodTablesSetIam:
		return p.tableIam(call, request, scope)
	case methodDatasetsDelete:
		return p.datasetsDelete(call, request, scope)
	default:
		return p.executeTables(call, request, scope)
	}
}

// executeTables dispatches the table and row methods.
func (p *Provider) executeTables(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodTablesCreate:
		return p.tablesCreate(call, request, scope)
	case methodTablesGet:
		return p.tablesGet(request, scope)
	case methodTablesList:
		return p.tablesList(request, scope)
	case methodTablesUpdate:
		return p.tablesUpdate(request, scope)
	case methodTablesDelete:
		return p.tablesDelete(call, request, scope)
	case methodRowsInsert:
		return p.rowsInsert(call, request, scope)
	default:
		return p.executeJobs(call, request, scope)
	}
}

// executeJobs dispatches the job methods and tabledata.list.
func (p *Provider) executeJobs(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodJobsQuery:
		return p.jobsQuery(call, request, scope)
	case methodJobsResults:
		return p.jobsResults(request, scope)
	case methodJobsInsert:
		return p.jobsInsert(call, request, scope)
	case methodJobsGet:
		return p.jobsGet(request, scope)
	default:
		return p.rowsList(call, request, scope)
	}
}

// datasetCopy returns a copy of one dataset taken under the scope lock, or
// nil; callers render the copy, never the stored record.
func (p *Provider) datasetCopy(scope state.Scope, dataset string) *datasetRecord {
	var copied *datasetRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.datasets[dataset]; record != nil {
			clone := *record
			clone.Labels = maps.Clone(record.Labels)
			copied = &clone
		}
		return nil
	})
	return copied
}

// datasetsCreate validates and stores one dataset.
func (p *Provider) datasetsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		DatasetReference struct {
			DatasetID string `json:"datasetId"`
		} `json:"datasetReference"`
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
		Location     string            `json:"location"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid dataset body")
	}
	dataset := cmp.Or(request.Path["datasetId"], body.DatasetReference.DatasetID)
	if !validDatasetID(dataset) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid dataset id %q", dataset)
	}
	created := &datasetRecord{
		ID: dataset, Labels: body.Labels, FriendlyName: body.FriendlyName, Location: cmp.Or(body.Location, "US"), CreatedAt: p.clock(),
	}
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.datasets[dataset] != nil {
			return status.Errorf(codes.AlreadyExists, "Dataset %s already exists", dataset)
		}
		for id := range s.datasets {
			if strings.EqualFold(id, dataset) {
				// The engine folds name case, so it cannot keep the two apart.
				return gap("datasets " + id + " and " + dataset + " differ only in case")
			}
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: datasetResource(scope.Project, dataset), Service: serviceName,
			Kind: kindDataset, Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return fmt.Errorf("index the dataset: %w", err)
		}
		s.datasets[dataset] = created
		document = datasetWire(scope.Project, dataset, created)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("create the dataset: %w", err)
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// datasetWire renders one dataset document.
func datasetWire(project, dataset string, record *datasetRecord) map[string]any {
	created := record.CreatedAt
	document := map[string]any{
		keyKind:            "bigquery#dataset",
		"id":               project + ":" + dataset,
		keyETag:            etagOf(created),
		keySelfLink:        selfLink + project + "/datasets/" + dataset,
		"datasetReference": map[string]any{"datasetId": dataset, keyProjectID: project},
		keyLocation:        cmp.Or(record.Location, "US"),
		keyType:            "DEFAULT",
		"access":           defaultAccess(),
		keyCreationTime:    strconv.FormatInt(created.UnixMilli(), 10),
		"lastModifiedTime": strconv.FormatInt(created.UnixMilli(), 10),
	}
	addDescriptors(document, record.Labels, record.FriendlyName)
	return document
}

// addDescriptors sets the optional labels and friendlyName, which the real
// surface omits when empty.
func addDescriptors(document map[string]any, labels map[string]string, friendlyName string) {
	if len(labels) > 0 {
		document["labels"] = maps.Clone(labels)
	}
	if friendlyName != "" {
		document["friendlyName"] = friendlyName
	}
}

func (p *Provider) datasetsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	record := p.datasetCopy(scope, dataset)
	if record == nil {
		return nil, provider.NotFound(kindDataset, dataset)
	}
	return &gateway.RESTResponse{JSON: datasetWire(scope.Project, dataset, record)}, nil
}

// datasetsList returns the project's datasets.
func (p *Provider) datasetsList(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.datasets {
			items = append(items, datasetWire(scope.Project, record.ID, record))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"datasets": items}}, nil
}

// datasetsUpdate replaces labels and the friendly name.
func (p *Provider) datasetsUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	var body struct {
		Labels       map[string]string `json:"labels"`
		FriendlyName string            `json:"friendlyName"`
	}
	// The whole body decodes before anything changes, so a bad field
	// changes nothing.
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid dataset update")
	}
	var document map[string]any
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.datasets[dataset]
		if record == nil {
			return provider.NotFound(kindDataset, dataset)
		}
		record.Labels, record.FriendlyName = maps.Clone(body.Labels), body.FriendlyName
		document = datasetWire(scope.Project, dataset, record)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // the callback returns a wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// datasetsDelete removes one dataset. A dataset with tables is refused unless
// deleteContents is set, which drops its tables from the engine first.
func (p *Provider) datasetsDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path["datasetId"]
	if p.datasetCopy(scope, dataset) == nil {
		return nil, provider.NotFound(kindDataset, dataset)
	}
	contents := request.Query.Get("deleteContents") == "true"
	// The engine gate keeps queries from creating tables in the dataset
	// while it goes.
	if err := p.useEngine(call.Context, scope, func(ctx context.Context, engine *engineBackend) error {
		tables := p.datasetTables(scope, dataset)
		if len(tables) > 0 && !contents {
			return status.Errorf(codes.InvalidArgument, "Dataset %s is still in use", dataset)
		}
		if err := p.dropTables(ctx, engine, scope, tables); err != nil {
			return err
		}
		return p.removeDataset(scope, dataset)
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{Status: 204}, nil
}

// removeDataset deletes an empty dataset's metadata, index entry, and policy.
func (p *Provider) removeDataset(scope state.Scope, dataset string) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // wire statuses and wrapped index errors
		if s.datasets[dataset] == nil {
			return provider.NotFound(kindDataset, dataset)
		}
		for key := range s.tables {
			if strings.HasPrefix(key, dataset+"/") {
				return status.Errorf(codes.InvalidArgument, "Dataset %s is still in use", dataset)
			}
		}
		if err := p.index.Remove(serviceName, scope, datasetResource(scope.Project, dataset)); err != nil {
			return fmt.Errorf("unregister the dataset: %w", err)
		}
		delete(s.datasets, dataset)
		p.engine.Forget(datasetResource(scope.Project, dataset))
		return nil
	})
}

// datasetTables lists one dataset's tables.
func (p *Provider) datasetTables(scope state.Scope, dataset string) []*tableRecord {
	var tables []*tableRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for key, table := range s.tables {
			if strings.HasPrefix(key, dataset+"/") {
				tables = append(tables, table)
			}
		}
		return nil
	})
	return tables
}

// unindexTables removes deleted tables from the resource index.
func (p *Provider) unindexTables(scope state.Scope, dataset string, tables []string) error {
	for _, table := range tables {
		resource := tableResource(scope.Project, dataset, table)
		if err := p.index.Remove(serviceName, scope, resource); err != nil {
			return fmt.Errorf("unregister the table: %w", err)
		}
		// A deleted table's policy goes with it; a new table of the same name
		// starts with none.
		p.engine.Forget(resource)
	}
	return nil
}
