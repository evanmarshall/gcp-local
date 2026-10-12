package compute

import (
	"encoding/json"
	"net"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Network Management Connectivity Tests v1: create (?testId=), get, list,
// delete, and rerun. Tests are analyzed when created or rerun and store that
// result, as GCP does; reads do not re-evaluate the topology.

// connectivityEndpoint is one IP-address or VM-instance endpoint of a test.
type connectivityEndpoint struct {
	IPAddress string `json:"ipAddress"`
	Instance  string `json:"instance"`
	Network   string `json:"network"`
	ProjectID string `json:"projectId"`
	Port      int    `json:"port"`
}

// connectivityRequest is the posted ConnectivityTest body.
type connectivityRequest struct {
	Description string               `json:"description"`
	Source      connectivityEndpoint `json:"source"`
	Destination connectivityEndpoint `json:"destination"`
	Protocol    string               `json:"protocol"`
}

// namedTest is one stored connectivity test with its last analysis.
type namedTest struct {
	Name        string
	Description string
	Source      map[string]any
	Destination map[string]any
	Spec        flow
	Protocol    string
	Result      analysis
	CreatedAt   time.Time
	UpdatedAt   time.Time
}

// executeConnectivity dispatches the connectivity test methods.
func (p *Provider) executeConnectivity(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodConnectivityCreate:
		return p.createConnectivity(request, scope)
	case methodConnectivityList:
		return p.listConnectivity(scope)
	case methodConnectivityGet:
		return p.getConnectivity(request, scope)
	case methodConnectivityRerun:
		return p.rerunConnectivity(request, scope)
	default:
		return p.deleteConnectivity(request, scope)
	}
}

// parseConnectivity validates one ConnectivityTest body into a stored test.
func parseConnectivity(request *gateway.RESTRequest, project, id string) (*namedTest, error) {
	var spec connectivityRequest
	if json.Unmarshal(request.JSON, &spec) != nil {
		return nil, statusInvalid("Invalid connectivity test specification")
	}
	for _, endpoint := range []connectivityEndpoint{spec.Source, spec.Destination} {
		if endpoint.ProjectID != "" && endpoint.ProjectID != project {
			return nil, gap("cross-project connectivity test endpoints are not modeled")
		}
	}
	protocol := textOr(spec.Protocol, "TCP")
	flowSpec, err := connectivityFlow(project, protocol, spec)
	if err != nil {
		return nil, err
	}
	var body map[string]any
	_ = json.Unmarshal(request.JSON, &body)
	source, _ := body[keySource].(map[string]any)
	destination, _ := body[keyDestination].(map[string]any)
	return &namedTest{
		Name: id, Description: spec.Description, Source: source, Destination: destination,
		Spec: flowSpec, Protocol: protocol,
	}, nil
}

// connectivityFlow builds the analyzer flow of a test. Instance endpoints are
// resolved by the analyzer; an endpoint without an instance needs an address.
func connectivityFlow(project, protocol string, spec connectivityRequest) (flow, error) {
	source, err := instanceEndpoint(project, spec.Source.Instance)
	if err != nil {
		return flow{}, err
	}
	destination, err := instanceEndpoint(project, spec.Destination.Instance)
	if err != nil {
		return flow{}, err
	}
	if source == "" && destination == "" {
		return newFlow(project, spec.Source.IPAddress, spec.Source.Network, spec.Destination.IPAddress, protocol, spec.Destination.Port)
	}
	result := flow{
		project: project, sourceInstance: source, sourceNetwork: networkName(spec.Source.Network), destInstance: destination,
		protocol: strings.ToLower(protocol), port: spec.Destination.Port,
	}
	for _, endpoint := range []struct {
		instance, address string
		target            *net.IP
	}{{source, spec.Source.IPAddress, &result.sourceIP}, {destination, spec.Destination.IPAddress, &result.destIP}} {
		if endpoint.instance != "" {
			continue
		}
		if *endpoint.target = net.ParseIP(strings.TrimSpace(endpoint.address)); *endpoint.target == nil {
			return flow{}, statusInvalid("Connectivity test endpoints require a valid IP address or an instance")
		}
	}
	return result, nil
}

// instanceEndpoint parses an instance URI (projects/P/zones/Z/instances/N,
// optionally a full URL) into its zone/name key. Instances of other projects
// are an explicit gap.
func instanceEndpoint(project, reference string) (string, error) {
	if reference == "" {
		return "", nil
	}
	_, rest, _ := strings.Cut(reference, "projects/")
	parts := strings.Split(rest, "/")
	if len(parts) != 5 || parts[1] != "zones" || parts[3] != "instances" || !uidPattern.MatchString(parts[4]) {
		return "", statusInvalid("Invalid instance endpoint " + reference)
	}
	if parts[0] != project {
		return "", gap("cross-project connectivity test endpoints are not modeled")
	}
	return instanceKey(parts[2], parts[4]), nil
}

// createConnectivity analyzes and stores one test named by ?testId=, then
// reports the finished long-running operation.
func (p *Provider) createConnectivity(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	id := request.Query.Get("testId")
	if !uidPattern.MatchString(id) {
		return nil, statusInvalid("A connectivity test requires a valid testId")
	}
	record, err := parseConnectivity(request, scope.Project, id)
	if err != nil {
		return nil, err
	}
	record.Result = p.analyze(record.Spec)
	if record.Result.gap != "" {
		return nil, gap(record.Result.gap)
	}
	record.CreatedAt = p.clock()
	record.UpdatedAt = record.CreatedAt
	document := connectivityDocument(scope.Project, record)
	exists := false
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if exists = s.connectivity[id] != nil; !exists {
			s.connectivity[id] = record
		}
		return nil
	})
	if exists {
		return nil, status.Errorf(codes.AlreadyExists, "Resource '%s' already exists", testName(scope.Project, id))
	}
	return p.connectivityOperation(scope, "create", testName(scope.Project, id), document)
}

// rerunConnectivity re-analyzes one stored test against the current topology.
func (p *Provider) rerunConnectivity(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record, err := p.storedTest(request, scope)
	if err != nil {
		return nil, err
	}
	result := p.analyze(record.Spec)
	if result.gap != "" {
		return nil, gap(result.gap)
	}
	var document map[string]any
	_ = p.scopes.With(scope, func(*scopeState) error {
		record.Result, record.UpdatedAt = result, p.clock()
		document = connectivityDocument(scope.Project, record)
		return nil
	})
	return p.connectivityOperation(scope, "rerun", testName(scope.Project, record.Name), document)
}

// storedTest reads one stored test or reports NOT_FOUND.
func (p *Provider) storedTest(request *gateway.RESTRequest, scope state.Scope) (*namedTest, error) {
	var record *namedTest
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record = s.connectivity[request.Path["test"]]
		return nil
	})
	if record == nil {
		return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", testName(scope.Project, request.Path["test"]))
	}
	return record, nil
}

// getConnectivity returns one stored test with its last analysis.
func (p *Provider) getConnectivity(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.connectivity[request.Path["test"]]; record != nil {
			document = connectivityDocument(scope.Project, record)
		}
		return nil
	})
	if document == nil {
		return nil, status.Errorf(codes.NotFound, "Resource '%s' was not found", testName(scope.Project, request.Path["test"]))
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// listConnectivity returns the stored tests in name order.
func (p *Provider) listConnectivity(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, name := range sortedKeys(s.connectivity) {
			items = append(items, connectivityDocument(scope.Project, s.connectivity[name]))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"resources": items}}, nil
}

// deleteConnectivity removes one stored test and reports the operation.
func (p *Provider) deleteConnectivity(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record, err := p.storedTest(request, scope)
	if err != nil {
		return nil, err
	}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.connectivity, record.Name)
		return nil
	})
	return p.connectivityOperation(scope, "delete", testName(scope.Project, record.Name),
		map[string]any{"@type": "type.googleapis.com/google.protobuf.Empty"})
}

// testName renders the full ConnectivityTest resource name.
func testName(project, id string) string {
	return "projects/" + project + "/locations/global/connectivityTests/" + id
}

// connectivityDocument renders the recorded ConnectivityTest field set.
func connectivityDocument(project string, record *namedTest) map[string]any {
	verified := stampRFC3339(record.UpdatedAt)
	trace := map[string]any{
		"endpointInfo": map[string]any{
			"sourceIp": record.Result.sourceIP.String(), "destinationIp": record.Result.destIP.String(),
			"protocol": record.Protocol, "destinationPort": record.Spec.port,
			"sourceNetworkUri": relativeNetwork(project, record.Result.network),
		},
		"steps": record.Result.steps, "forwardTraceId": 1,
	}
	document := map[string]any{
		keyName:        testName(project, record.Name),
		keySource:      record.Source,
		keyDestination: record.Destination,
		keyProtocol:    record.Protocol,
		"createTime":   stampRFC3339(record.CreatedAt),
		"updateTime":   verified,
		"reachabilityDetails": map[string]any{
			"result": record.Result.result, "verifyTime": verified, "traces": []any{trace},
		},
	}
	setText(document, keyDescription, record.Description)
	return document
}

// stampRFC3339 renders the Network Management timestamp shape.
func stampRFC3339(moment time.Time) string {
	return moment.UTC().Format(time.RFC3339Nano)
}

// connectivityOperation renders one finished Network Management operation.
func (p *Provider) connectivityOperation(scope state.Scope, verb, target string, response map[string]any) (*gateway.RESTResponse, error) {
	var generation int64
	_ = p.scopes.With(scope, func(s *scopeState) error {
		generation = s.nextGeneration()
		return nil
	})
	now := stampRFC3339(p.clock())
	if _, typed := response["@type"]; !typed {
		response["@type"] = "type.googleapis.com/google.cloud.networkmanagement.v1.ConnectivityTest"
	}
	return &gateway.RESTResponse{JSON: map[string]any{
		keyName: "projects/" + scope.Project + "/locations/global/operations/operation-" + strconv.FormatInt(generation, 10),
		"metadata": map[string]any{
			"@type":      "type.googleapis.com/google.cloud.networkmanagement.v1.OperationMetadata",
			"createTime": now, "endTime": now, "target": target, "verb": verb, "apiVersion": "v1",
		},
		"done":     true,
		"response": response,
	}}, nil
}
