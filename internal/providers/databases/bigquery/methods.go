package bigquery

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/url"
	"slices"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
)

// execute dispatches one authorized BigQuery request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodDatasetsCreate:
		return p.datasetsCreate(request, scope)
	case methodDatasetsGet:
		return p.datasetsGet(request, scope)
	case methodDatasetsList:
		return p.datasetsList(request, scope)
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
	case methodJobsList:
		return p.jobsList(call, request, scope)
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

// datasetFields lists the Dataset schema fields the provider doesn't model
// (BigQuery v2 discovery, Dataset), each refused with a gap when set, and its
// output-only fields, dropped as GCP ignores them. GCP ignores names its
// schema lacks (recorded), and so does the provider.
var datasetFields = provider.Fields{
	Unmodeled: []string{
		"storageBillingModel", "isCaseInsensitive", "resourceTags", "externalDatasetReference",
		"defaultEncryptionConfiguration", "defaultCollation", "defaultRoundingMode", "externalCatalogDatasetOptions",
		"linkedDatasetSource",
	},
	OutputOnly: []string{
		"kind", "satisfiesPzs", "type", "linkedDatasetMetadata", "id", "restrictions", keyLastModified, "selfLink",
		keyETag, keyCreationTime, "satisfiesPzi", "catalogSource", "tags",
	},
	IgnoreUnknown: true,
}

// datasetBody is the slice of the Dataset resource the provider models.
// access and maxTimeTravelHours are accepted only at the values GCP assigns
// (a client's read-modify-write sends them back); anything else is a gap.
type datasetBody struct {
	DatasetReference struct {
		ProjectID string `json:"projectId"`
		DatasetID string `json:"datasetId"`
	} `json:"datasetReference"`
	Labels                       map[string]*string `json:"labels"`
	FriendlyName                 patchText          `json:"friendlyName"`
	Description                  patchText          `json:"description"`
	Location                     string             `json:"location"`
	Access                       json.RawMessage    `json:"access"`
	MaxTimeTravelHours           json.RawMessage    `json:"maxTimeTravelHours"`
	DefaultTableExpirationMs     optionalInt        `json:"defaultTableExpirationMs"`
	DefaultPartitionExpirationMs optionalInt        `json:"defaultPartitionExpirationMs"`
}

// minTableExpiration is the shortest default table expiration, in
// milliseconds (recorded refusal).
const minTableExpiration = 3_600_000

// defaults applies the body's default expirations to record, after checking
// both; null clears one.
func (body datasetBody) defaults(record *datasetRecord) error {
	table, partition := body.DefaultTableExpirationMs, body.DefaultPartitionExpirationMs
	if table.present() && table.Value < minTableExpiration {
		return failure(codes.InvalidArgument, reasonInvalid, "Table expiration is less than 3600000ms")
	}
	if partition.present() && partition.Value <= 0 {
		// Not recorded.
		return failure(codes.InvalidArgument, reasonInvalid, "Partition expiration must be positive")
	}
	if table.Set {
		record.DefaultTableExpirationMs = table.Value
	}
	if partition.Set {
		record.DefaultPartitionExpirationMs = partition.Value
	}
	return nil
}

// patchText is a string field of a patch: Set when the body names it, and
// "" when it is null, which clears it.
type patchText struct {
	Set   bool
	Value string
}

// apply sets field to the patch's value when the patch names it.
func (t patchText) apply(field *string) {
	if t.Set {
		*field = t.Value
	}
}

func (t *patchText) UnmarshalJSON(raw []byte) error {
	t.Set = true
	if string(raw) == jsonNull {
		t.Value = ""
		return nil
	}
	return json.Unmarshal(raw, &t.Value) //nolint:wrapcheck // DecodeJSON reports it
}

// defaultTimeTravel is the time-travel window GCP reports, in hours.
const defaultTimeTravel = "168"

// decodeDataset strictly decodes a dataset body and refuses access lists and
// time-travel windows other than GCP's defaults.
func decodeDataset(request *gateway.RESTRequest) (datasetBody, error) {
	var body datasetBody
	if err := provider.DecodeJSON(request.JSON, "", &body, gap, datasetFields); err != nil {
		return body, err
	}
	if !defaultAccessList(body.Access) {
		return body, gap("dataset access entries other than the defaults; see issue #90")
	}
	if hours := strings.Trim(string(body.MaxTimeTravelHours), `"`); hours != "" && hours != jsonNull && hours != defaultTimeTravel {
		return body, gap("maxTimeTravelHours other than " + defaultTimeTravel)
	}
	return body, nil
}

// defaultAccessList reports an absent or empty access list, or one equal to
// the default list, in any order.
func defaultAccessList(raw json.RawMessage) bool {
	var entries []map[string]any
	if len(raw) == 0 || string(raw) == jsonNull {
		return true
	}
	if json.Unmarshal(raw, &entries) != nil {
		return false
	}
	if len(entries) == 0 {
		return true
	}
	want := map[string]bool{}
	for _, entry := range defaultAccess() {
		encoded, _ := json.Marshal(entry) //nolint:errchkjson // plain maps
		want[string(encoded)] = true
	}
	for _, entry := range entries {
		encoded, _ := json.Marshal(entry) //nolint:errchkjson // plain maps
		if !want[string(encoded)] {
			return false
		}
	}
	return len(entries) == len(want)
}

// refuseOptions refuses the documented query parameters a method doesn't
// model, naming each.
func refuseOptions(query url.Values, names ...string) error {
	for _, name := range names {
		if query.Get(name) != "" {
			return gap("query parameter " + name)
		}
	}
	return nil
}

// policyVersion reads accessPolicyVersion, which the google Terraform
// provider sends on dataset inserts, reads, and patches. GCP accepts any
// int32 (recorded: 1, 2, and 3) and refuses anything else with a BadRequest.
// The provider's access lists hold no conditions, so every version renders
// them the same.
func policyVersion(query url.Values) error {
	text := query.Get("accessPolicyVersion")
	if text == "" {
		return nil
	}
	if _, err := strconv.ParseInt(text, 10, 32); err != nil {
		description := fmt.Sprintf("Invalid value at 'access_policy_version' (TYPE_INT32), %q", text)
		return provider.FieldViolation("access_policy_version", description)
	}
	return nil
}

// datasetsCreate validates and stores one dataset.
func (p *Provider) datasetsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := policyVersion(request.Query); err != nil {
		return nil, err
	}
	body, err := decodeDataset(request)
	if err != nil {
		return nil, err
	}
	dataset := cmp.Or(request.Path[keyDatasetID], body.DatasetReference.DatasetID)
	if !validDatasetID(dataset) {
		return nil, failure(codes.InvalidArgument, reasonInvalid, fmt.Sprintf("Invalid dataset ID %q.", dataset))
	}
	now := p.clock()
	created := &datasetRecord{
		ID: dataset, Labels: setLabels(nil, body.Labels), FriendlyName: body.FriendlyName.Value, Description: body.Description.Value,
		Location: cmp.Or(body.Location, "US"), CreatedAt: now, ModifiedAt: now,
	}
	if err := body.defaults(created); err != nil {
		return nil, err
	}
	var document map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.datasets[dataset] != nil {
			return failure(codes.AlreadyExists, reasonDuplicate, "Already Exists: Dataset "+scope.Project+":"+dataset)
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

// setLabels merges a patch's labels into current, as GCP does (recorded:
// omitted keys stay); a null value removes its key.
func setLabels(current map[string]string, patch map[string]*string) map[string]string {
	merged := maps.Clone(current)
	if merged == nil {
		merged = map[string]string{}
	}
	for key, value := range patch {
		if value == nil {
			delete(merged, key)
			continue
		}
		merged[key] = *value
	}
	return merged
}

// datasetNotFound is GCP's 404 for a missing dataset (recorded).
func datasetNotFound(project, dataset string) error {
	return failure(codes.NotFound, reasonNotFound, "Not found: Dataset "+project+":"+dataset)
}

// datasetWire renders one dataset document. A patched dataset reports its
// time-travel window, as GCP does (recorded).
func datasetWire(project, dataset string, record *datasetRecord) map[string]any {
	document := map[string]any{
		keyKind:            "bigquery#dataset",
		"id":               project + ":" + dataset,
		keyETag:            etagOf(record.ModifiedAt),
		keySelfLink:        selfLink + project + "/datasets/" + dataset,
		"datasetReference": map[string]any{keyDatasetID: dataset, keyProjectID: project},
		keyLocation:        cmp.Or(record.Location, "US"),
		keyType:            "DEFAULT",
		"access":           defaultAccess(),
		keyCreationTime:    strconv.FormatInt(record.CreatedAt.UnixMilli(), 10),
		keyLastModified:    strconv.FormatInt(record.ModifiedAt.UnixMilli(), 10),
	}
	if record.Patched {
		document["maxTimeTravelHours"] = defaultTimeTravel
	}
	if record.DefaultTableExpirationMs > 0 {
		document["defaultTableExpirationMs"] = strconv.FormatInt(record.DefaultTableExpirationMs, 10)
	}
	if record.DefaultPartitionExpirationMs > 0 {
		document["defaultPartitionExpirationMs"] = strconv.FormatInt(record.DefaultPartitionExpirationMs, 10)
	}
	addDescriptors(document, record.Labels, record.FriendlyName, record.Description)
	return document
}

// addDescriptors sets the optional labels, friendlyName, and description,
// which the real surface omits when empty.
func addDescriptors(document map[string]any, labels map[string]string, friendlyName, description string) {
	if len(labels) > 0 {
		document["labels"] = maps.Clone(labels)
	}
	if friendlyName != "" {
		document["friendlyName"] = friendlyName
	}
	if description != "" {
		document["description"] = description
	}
}

func (p *Provider) datasetsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := refuseOptions(request.Query, "datasetView"); err != nil {
		return nil, err
	}
	if err := policyVersion(request.Query); err != nil {
		return nil, err
	}
	dataset := request.Path[keyDatasetID]
	record := p.datasetCopy(scope, dataset)
	if record == nil {
		return nil, datasetNotFound(scope.Project, dataset)
	}
	return &gateway.RESTResponse{JSON: datasetWire(scope.Project, dataset, record)}, nil
}

// errBadPageToken marks a page token the provider didn't issue.
var errBadPageToken = errors.New("bad page token")

// listPaging is the datasets and tables list paging; maxResults defaults are
// not recorded.
var listPaging = provider.Paging{Default: 1000, Max: 1000, InvalidToken: errBadPageToken}

// listPage pages ordered items by maxResults and pageToken. maxResults must
// be an unsigned 32-bit integer (recorded refusal); a token the provider
// didn't issue returns errBadPageToken.
func listPage[T any](items []T, query url.Values) ([]T, string, error) {
	paging := url.Values{"pageToken": {query.Get(optionPageToken)}}
	if text := query.Get(optionMaxResults); text != "" {
		if _, err := strconv.ParseUint(text, 10, 32); err != nil {
			description := fmt.Sprintf("Invalid value at 'max_results.value' (TYPE_UINT32), %q", text)
			return nil, "", provider.FieldViolation("max_results.value", description)
		}
		paging.Set("pageSize", text)
	}
	return provider.Page(items, paging, listPaging, gap)
}

// datasetsList returns one page of the project's datasets by ID, as GCP
// orders them (recorded). Hidden datasets (IDs starting with "_") are listed
// only with all=true; a filter is a gap. A page token the provider didn't
// issue lists nothing, as GCP does (recorded).
func (p *Provider) datasetsList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := refuseOptions(request.Query, "filter"); err != nil {
		return nil, err
	}
	all := request.Query.Get("all") == textTrue
	var items []map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, id := range slices.Sorted(maps.Keys(s.datasets)) {
			if strings.HasPrefix(id, "_") && !all {
				continue
			}
			items = append(items, datasetItem(scope.Project, s.datasets[id]))
		}
		return nil
	})
	page, next, err := listPage(items, request.Query)
	if errors.Is(err, errBadPageToken) {
		page, next, err = nil, "", nil
	}
	if err != nil {
		return nil, err
	}
	document := provider.ListBody("datasets", page, next)
	document[keyKind], document[keyETag] = "bigquery#datasetList", etagOf(p.clock())
	return &gateway.RESTResponse{JSON: document}, nil
}

// datasetItem is one datasets.list entry (recorded fields).
func datasetItem(project string, record *datasetRecord) map[string]any {
	item := map[string]any{
		keyKind: "bigquery#dataset", "id": project + ":" + record.ID,
		"datasetReference": map[string]any{keyDatasetID: record.ID, keyProjectID: project},
		keyLocation:        cmp.Or(record.Location, "US"), keyType: "DEFAULT",
	}
	addDescriptors(item, record.Labels, record.FriendlyName, "")
	return item
}

// datasetsUpdate is datasets.patch: labels merge, and friendlyName,
// description, and the default expirations change only when the body names
// them (recorded). The whole body is validated before anything changes.
func (p *Provider) datasetsUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := refuseOptions(request.Query, "updateMode"); err != nil {
		return nil, err
	}
	if err := policyVersion(request.Query); err != nil {
		return nil, err
	}
	dataset := request.Path[keyDatasetID]
	body, err := decodeDataset(request)
	if err != nil {
		return nil, err
	}
	if id := body.DatasetReference.DatasetID; id != "" && id != dataset {
		return nil, failure(codes.InvalidArgument, reasonInvalid, "Dataset reference disagrees with the request path")
	}
	var document map[string]any
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.datasets[dataset]
		if record == nil {
			return datasetNotFound(scope.Project, dataset)
		}
		if err := patchDataset(record, body); err != nil {
			return err
		}
		record.Labels = setLabels(record.Labels, body.Labels)
		body.FriendlyName.apply(&record.FriendlyName)
		body.Description.apply(&record.Description)
		record.Patched, record.ModifiedAt = true, p.clock()
		document = datasetWire(scope.Project, dataset, record)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // the callback returns a wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// patchDataset checks a patch's location, which cannot change, and applies
// its default expirations after checking both.
func patchDataset(record *datasetRecord, body datasetBody) error {
	if body.Location != "" && !strings.EqualFold(body.Location, record.Location) {
		return failure(codes.InvalidArgument, reasonInvalid, "A dataset's location cannot change")
	}
	return body.defaults(record)
}

// datasetsDelete removes one dataset. A dataset with tables is refused unless
// deleteContents is set, which drops its tables from the engine first.
func (p *Provider) datasetsDelete(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	dataset := request.Path[keyDatasetID]
	if p.datasetCopy(scope, dataset) == nil {
		return nil, datasetNotFound(scope.Project, dataset)
	}
	contents := request.Query.Get("deleteContents") == textTrue
	// The engine gate keeps queries from creating tables in the dataset
	// while it goes.
	if err := p.useEngine(call.Context, scope, func(ctx context.Context, engine *engineBackend) error {
		tables := p.datasetTables(scope, dataset)
		if len(tables) > 0 && !contents {
			return datasetInUse(scope.Project, dataset)
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

// datasetInUse is GCP's refusal to delete a dataset with tables (recorded).
func datasetInUse(project, dataset string) error {
	return failure(codes.InvalidArgument, reasonInUse, "Dataset "+project+":"+dataset+" is still in use")
}

// removeDataset deletes an empty dataset's metadata, index entry, and policy.
func (p *Provider) removeDataset(scope state.Scope, dataset string) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // wire statuses and wrapped index errors
		if s.datasets[dataset] == nil {
			return datasetNotFound(scope.Project, dataset)
		}
		for key := range s.tables {
			if strings.HasPrefix(key, dataset+"/") {
				return datasetInUse(scope.Project, dataset)
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
			if strings.HasPrefix(key, dataset+"/") && !table.expired(p.clock()) {
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
