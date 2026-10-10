package compute

import (
	"net/http"
	"sort"
	"strconv"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized method.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodConnectivityRun, methodConnectivityCreate, methodConnectivityGet, methodConnectivityList, methodConnectivityDelete:
		return p.executeConnectivity(call, request, scope)
	case methodNetworksCreate, methodNetworksGet, methodNetworksList, methodNetworksDelete,
		methodNetworksAddPeering, methodNetworksRemovePeering:
		return p.executeNetwork(call, request, scope)
	case methodSubnetworksCreate, methodSubnetworksGet, methodSubnetworksList, methodSubnetworksDelete:
		return p.executeSubnetwork(call, request, scope)
	case methodFirewallsCreate, methodFirewallsGet, methodFirewallsList, methodFirewallsDelete, methodFirewallsUpdate:
		return p.executeFirewall(call, request, scope)
	case methodRoutesCreate, methodRoutesGet, methodRoutesList, methodRoutesDelete:
		return p.executeRoute(call, request, scope)
	case methodOperationsGet:
		return p.operationGet(request, scope, "global")
	case methodRegionOperationsGet:
		return p.operationGet(request, scope, "region")
	case methodOperationsList:
		return p.operationsList(request, scope, "global")
	default:
		return nil, gap("this method is not implemented")
	}
}

// networkDocument renders the network wire shape.
func (p *Provider) networkDocument(project string, record *networkRecord, cachedSubnets []any) map[string]any {
	return map[string]any{
		"id":                    idOf(record.Name, record.Generation),
		keyName:                 record.Name,
		keySelfLink:             selfLink(project, "networks", record.Name),
		keyTimestamp:            stamp(record.CreatedAt),
		keyKind:                 "compute#network",
		"autoCreateSubnetworks": record.AutoCreateSubnetworks,
		"mtu":                   record.MTU,
		"routingConfig":         map[string]any{"routingMode": "REGIONAL"},
		"subnetworks":           cachedSubnets,
		keyDescription:          "",
	}
}

// subnetworksOf lists the subnetwork self-links of one network. Networks live
// in the project's global scope, so the lookup walks the single scope.
func (p *Provider) subnetworksOf(project, network string) []any {
	links := []any{}
	scope := scopeOf(project, stateGlobal)
	_ = p.scopes.With(scope, func(s *scopeState) error {
		names := make([]string, 0, len(s.subnetworks))
		for name := range s.subnetworks {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			if record := s.subnetworks[name]; record.Network == network {
				links = append(links, regionalSelfLink(project, record.Region, "subnetworks", name))
			}
		}
		return nil
	})
	return links
}

// subnetworkDocument renders the subnetwork wire shape.
func subnetworkDocument(project string, record *subnetworkRecord) map[string]any {
	return map[string]any{
		"id":                    idOf(record.Name, record.Generation),
		keyName:                 record.Name,
		keySelfLink:             regionalSelfLink(project, record.Region, "subnetworks", record.Name),
		keyTimestamp:            stamp(record.CreatedAt),
		keyKind:                 "compute#subnetwork",
		keyNetwork:              selfLink(project, "networks", record.Network),
		"region":                regionalSelfLink(project, record.Region, "regions", record.Region),
		"ipCidrRange":           record.IPCIDRRange,
		"gatewayAddress":        record.Gateway,
		"privateIpGoogleAccess": record.PrivateAccess,
		"secondaryIpRanges":     []any{},
		keyDescription:          "",
	}
}

// firewallDocument renders the firewall rule wire shape.
func firewallDocument(project string, record *firewallRecord) map[string]any {
	allowed, denied := record.Allowed, record.Denied
	if allowed == nil {
		allowed = []map[string]any{}
	}
	if denied == nil {
		denied = []map[string]any{}
	}
	sourceRanges := record.SourceRanges
	if sourceRanges == nil {
		sourceRanges = []string{}
	}
	targetTags := record.TargetTags
	if targetTags == nil {
		targetTags = []string{}
	}
	return map[string]any{
		"id":           idOf(record.Name, record.Generation),
		keyName:        record.Name,
		keySelfLink:    selfLink(project, "firewalls", record.Name),
		keyTimestamp:   stamp(record.CreatedAt),
		keyKind:        "compute#firewall",
		keyNetwork:     selfLink(project, "networks", record.Network),
		"direction":    record.Direction,
		"allowed":      allowed,
		"denied":       denied,
		"sourceRanges": sourceRanges,
		"targetTags":   targetTags,
		"priority":     record.Priority,
		"disabled":     record.Disabled,
		keyDescription: "",
	}
}

// routeDocument renders the route wire shape.
func routeDocument(project string, record *routeRecord) map[string]any {
	tags := record.Tags
	if tags == nil {
		tags = []string{}
	}
	return map[string]any{
		"id":             idOf(record.Name, record.Generation),
		keyName:          record.Name,
		keySelfLink:      selfLink(project, "routes", record.Name),
		keyTimestamp:     stamp(record.CreatedAt),
		keyKind:          "compute#route",
		keyNetwork:       selfLink(project, "networks", record.Network),
		keyDestRange:     record.DestRange,
		"nextHopGateway": record.NextHopGateway,
		"priority":       record.Priority,
		"tags":           tags,
		keyDescription:   "",
	}
}

// operationDocument renders one operation in the compute shape.
func operationDocument(record *operationRecord) map[string]any {
	entry := map[string]any{
		"id":            record.ID,
		keyName:         record.Name,
		"status":        "RUNNING",
		"progress":      0,
		"insertTime":    stamp(record.Created),
		keySelfLink:     "",
		"operationType": record.Type,
		"targetLink":    record.Target,
		keyKind:         "compute#operation",
		"zone":          "",
		"user":          "gcp-local",
	}
	if !record.Done {
		return entry
	}
	entry["status"] = "DONE"
	entry["progress"] = 100
	if record.Error != "" {
		entry[keyError] = map[string]any{"errors": []any{map[string]any{keyCode: 400, keyMessage: record.Error}}}
	}
	return entry
}

// parseInsert reads one insert body as a validated map.
func parseInsert(request *gateway.RESTRequest) (map[string]any, error) {
	body, err := decodeBody(request.JSON)
	if err != nil {
		return nil, err
	}
	name := textOf(body["name"])
	if name == "" {
		name = request.Path["network"]
	}
	if name == "" {
		name = request.Path["subnetwork"]
	}
	if name == "" {
		name = request.Path["firewall"]
	}
	if name == "" {
		name = request.Path["route"]
	}
	if !uidPattern.MatchString(name) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid resource name %q", name)
	}
	body["name"] = name
	return body, nil
}

// textOf reads a string field as empty-safe text.
func textOf(value any) string {
	if value, ok := value.(string); ok {
		return value
	}
	return ""
}

// intOf reads a numeric field with a default.
func intOf(value any, fallback int) int {
	if number, ok := value.(float64); ok {
		return int(number)
	}
	return fallback
}

// boolOf reads a boolean field with a default.
func boolOf(value any, fallback bool) bool {
	if value, ok := value.(bool); ok {
		return value
	}
	return fallback
}

// stringListOf reads a JSON string array field.
func stringListOf(value any) []string {
	entries, _ := value.([]any)
	result := make([]string, 0, len(entries))
	for _, entry := range entries {
		if text, ok := entry.(string); ok {
			result = append(result, text)
		}
	}
	return result
}

// listOfMaps reads a JSON array of objects field.
func listOfMaps(value any) []map[string]any {
	entries, _ := value.([]any)
	result := make([]map[string]any, 0, len(entries))
	for _, entry := range entries {
		if object, ok := entry.(map[string]any); ok {
			result = append(result, object)
		}
	}
	return result
}

// parseCidr validates one CIDR range field and returns it.
func parseCidr(body map[string]any, key string) (string, error) {
	value := textOf(body[key])
	if value == "" || !cidrPattern.MatchString(value) {
		return "", status.Errorf(codes.InvalidArgument, "Invalid %s %q", key, value)
	}
	return value, nil
}

// operationGet returns one operation document.
func (p *Provider) operationGet(request *gateway.RESTRequest, scope state.Scope, kind string) (*gateway.RESTResponse, error) {
	name := scope.Project + "/" + kind + "/" + request.Path["operation"]
	p.mu.RLock()
	record := p.records[name]
	p.mu.RUnlock()
	if record == nil {
		return nil, notFound("operations", request.Path["operation"])
	}
	return &gateway.RESTResponse{JSON: operationDocument(record)}, nil
}

// operationsList returns the scope's operations collection.
func (p *Provider) operationsList(_ *gateway.RESTRequest, _ state.Scope, kind string) (*gateway.RESTResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.records))
	for name, record := range p.records {
		if record.Kind == kind {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	items := make([]any, 0, len(names))
	for _, name := range names {
		items = append(items, operationDocument(p.records[name]))
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

// newOperation registers the completed compute operation for a realized
// mutation: the record stores the outcome, and reads report it as DONE.
func (p *Provider) newOperation(scope state.Scope, kind, region, opType, target string) (*gateway.RESTResponse, error) {
	return p.newOperationAt(scope, kind, region, opType, target, time.Now())
}

// newOperationAt records one realized mutation as a DONE operation stamped at
// the given moment.
func (p *Provider) newOperationAt(scope state.Scope, kind, region, opType, target string, now time.Time) (*gateway.RESTResponse, error) {
	var generation int64
	if scopeErr := p.scopes.With(scope, func(s *scopeState) error {
		generation = s.nextGeneration()
		return nil
	}); scopeErr != nil {
		return nil, gap("allocate the compute operation")
	}
	suffix := strconv.FormatInt(generation, 10)
	record := &operationRecord{
		ID: "op-" + suffix, Name: "operation-" + suffix, Kind: kind, Region: region,
		Type: opType, Target: target, Created: now, Done: true,
	}
	p.mu.Lock()
	p.records[scope.Project+"/"+kind+"/"+record.Name] = record
	p.mu.Unlock()
	return &gateway.RESTResponse{Status: 200, Headers: http.Header{}, JSON: operationDocument(record)}, nil
}
