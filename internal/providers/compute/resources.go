package compute

import (
	"encoding/base64"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized method.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodNetworksCreate, methodNetworksGet, methodNetworksList, methodNetworksDelete,
		methodNetworksAddPeering, methodNetworksRemovePeering:
		return p.executeNetwork(call, request, scope)
	case methodSubnetworksCreate, methodSubnetworksGet, methodSubnetworksList, methodSubnetworksDelete:
		return p.executeSubnetwork(call, request, scope)
	case methodFirewallsCreate, methodFirewallsGet, methodFirewallsList, methodFirewallsDelete, methodFirewallsUpdate:
		return p.executeFirewall(call, request, scope)
	case methodRoutesCreate, methodRoutesGet, methodRoutesList, methodRoutesDelete:
		return p.executeRoute(call, request, scope)
	case methodRoutersCreate, methodRoutersGet, methodRoutersList, methodRoutersDelete:
		return p.executeRouters(call, request, scope)
	default:
		return p.executeAnalysis(call, request, scope)
	}
}

// executeAnalysis handles the analyzer and long-running-operation surfaces.
func (p *Provider) executeAnalysis(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodConnectivityRerun, methodConnectivityCreate, methodConnectivityGet, methodConnectivityList, methodConnectivityDelete:
		return p.executeConnectivity(call, request, scope)
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

// networkDocument renders the recorded network wire shape. Optional fields
// appear only when set, as the recorded document omits them.
func (p *Provider) networkDocument(project string, record *networkRecord, subnets []any, peerings []any) map[string]any {
	id := idOf(record.Name, record.Generation)
	document := map[string]any{
		"id":                                    id,
		keyName:                                 record.Name,
		keySelfLink:                             selfLink(project, "networks", record.Name),
		"selfLinkWithId":                        selfLink(project, "networks", id),
		keyTimestamp:                            stamp(record.CreatedAt),
		keyKind:                                 "compute#network",
		keyAutoSubnets:                          record.AutoCreateSubnetworks,
		"routingConfig":                         map[string]any{"routingMode": record.RoutingMode, "bgpBestPathSelectionMode": "LEGACY"},
		"networkFirewallPolicyEnforcementOrder": "AFTER_CLASSIC_FIREWALL",
	}
	setText(document, keyDescription, record.Description)
	if record.MTU != 0 {
		document["mtu"] = record.MTU
	}
	if len(subnets) > 0 {
		document["subnetworks"] = subnets
	}
	if len(peerings) > 0 {
		document["peerings"] = peerings
	}
	return document
}

// setText sets one optional text field when it is non-empty.
func setText(document map[string]any, key, value string) {
	if value != "" {
		document[key] = value
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

// subnetworkDocument renders the recorded subnetwork wire shape.
func subnetworkDocument(project string, record *subnetworkRecord) map[string]any {
	document := map[string]any{
		"id":                           idOf(record.Name, record.Generation),
		keyName:                        record.Name,
		keySelfLink:                    regionalSelfLink(project, record.Region, "subnetworks", record.Name),
		keyTimestamp:                   stamp(record.CreatedAt),
		keyKind:                        "compute#subnetwork",
		keyNetwork:                     selfLink(project, "networks", record.Network),
		keyRegion:                      regionLink(project, record.Region),
		keyIPCIDR:                      record.IPCIDRRange,
		"gatewayAddress":               record.Gateway,
		"privateIpGoogleAccess":        record.PrivateAccess,
		"privateIpv6GoogleAccess":      "DISABLE_GOOGLE_ACCESS",
		"allowSubnetCidrRoutesOverlap": false,
		"fingerprint":                  fingerprint(record.Name, record.Generation),
		"purpose":                      "PRIVATE",
		keyStackType:                   stackIPv4Only,
	}
	setText(document, keyDescription, record.Description)
	return document
}

// fingerprint derives a stable opaque fingerprint for one resource version.
func fingerprint(name string, generation int64) string {
	return base64.StdEncoding.EncodeToString([]byte(idOf(name, generation)))
}

// firewallDocument renders the recorded firewall rule wire shape: empty
// lists are omitted, and description is always present.
func firewallDocument(project string, record *firewallRecord) map[string]any {
	document := map[string]any{
		"id":           idOf(record.Name, record.Generation),
		keyName:        record.Name,
		keySelfLink:    selfLink(project, "firewalls", record.Name),
		keyTimestamp:   stamp(record.CreatedAt),
		keyKind:        "compute#firewall",
		keyNetwork:     selfLink(project, "networks", record.Network),
		keyDirection:   record.Direction,
		keyPriority:    record.Priority,
		"disabled":     record.Disabled,
		"logConfig":    map[string]any{"enable": false},
		keyDescription: record.Description,
	}
	setList(document, "allowed", record.Allowed)
	setList(document, "denied", record.Denied)
	setList(document, "sourceRanges", record.SourceRanges)
	setList(document, "destinationRanges", record.DestinationRanges)
	setList(document, "targetTags", record.TargetTags)
	return document
}

// setList sets one optional list field when it is non-empty.
func setList[T any](document map[string]any, key string, values []T) {
	if len(values) > 0 {
		document[key] = values
	}
}

// routeDocument renders the route wire shape.
func routeDocument(project string, record *routeRecord) map[string]any {
	document := map[string]any{
		"id":           idOf(record.Name, record.Generation),
		keyName:        record.Name,
		keySelfLink:    selfLink(project, "routes", record.Name),
		keyTimestamp:   stamp(record.CreatedAt),
		keyKind:        "compute#route",
		keyNetwork:     selfLink(project, "networks", record.Network),
		keyDestRange:   record.DestRange,
		keyPriority:    record.Priority,
		keyDescription: "",
	}
	setText(document, "nextHopGateway", record.NextHopGateway)
	setText(document, "nextHopIp", record.NextHopIP)
	setList(document, "tags", record.Tags)
	return document
}

// operationDocument renders one operation in the compute shape. A failed
// operation still finishes DONE and carries the error compute reports.
func operationDocument(project string, record *operationRecord) map[string]any {
	entry := map[string]any{
		"id":            record.ID,
		keyName:         record.Name,
		"status":        "RUNNING",
		"progress":      0,
		"insertTime":    stamp(record.Created),
		"operationType": record.Type,
		"targetLink":    record.Target,
		keyKind:         "compute#operation",
		"user":          "gcp-local",
	}
	if record.Kind == "region" {
		entry[keySelfLink] = regionalSelfLink(project, record.Region, "operations", record.Name)
		entry[keyRegion] = regionLink(project, record.Region)
	} else {
		entry[keySelfLink] = selfLink(project, "operations", record.Name)
	}
	if !record.Done {
		return entry
	}
	entry["status"] = "DONE"
	entry["progress"] = 100
	entry["endTime"] = stamp(record.Created)
	if record.Error != nil {
		entry["httpErrorStatusCode"] = 400
		entry["httpErrorMessage"] = "BAD REQUEST"
		entry[keyError] = map[string]any{"errors": []any{map[string]any{keyCode: record.Error.Code, keyMessage: record.Error.Message}}}
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
	return &gateway.RESTResponse{JSON: operationDocument(scope.Project, record)}, nil
}

// operationsList returns the scope's operations collection.
func (p *Provider) operationsList(_ *gateway.RESTRequest, scope state.Scope, kind string) (*gateway.RESTResponse, error) {
	p.mu.RLock()
	defer p.mu.RUnlock()
	names := make([]string, 0, len(p.records))
	for name, record := range p.records {
		if record.Kind == kind && strings.HasPrefix(name, scope.Project+"/") {
			names = append(names, name)
		}
	}
	sort.Strings(names)
	items := make([]any, 0, len(names))
	for _, name := range names {
		items = append(items, operationDocument(scope.Project, p.records[name]))
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

// newOperation registers the completed compute operation for a realized
// mutation: the record stores the outcome, and reads report it as DONE.
func (p *Provider) newOperation(scope state.Scope, kind, region, opType, target string) (*gateway.RESTResponse, error) {
	return p.finishOperation(scope, kind, region, opType, target, nil)
}

// failedOperation reports a request compute refuses asynchronously: the call
// returns 200 and the DONE operation carries the error; nothing changed.
func (p *Provider) failedOperation(scope state.Scope, kind, region, opType, target string, failure *operationError) (*gateway.RESTResponse, error) {
	return p.finishOperation(scope, kind, region, opType, target, failure)
}

// finishOperation records one DONE operation with its optional error.
func (p *Provider) finishOperation(scope state.Scope, kind, region, opType, target string, failure *operationError) (*gateway.RESTResponse, error) {
	now := p.clock()
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
		Type: opType, Target: target, Created: now, Done: true, Error: failure,
	}
	p.mu.Lock()
	p.records[scope.Project+"/"+kind+"/"+record.Name] = record
	p.mu.Unlock()
	return &gateway.RESTResponse{Status: 200, Headers: http.Header{}, JSON: operationDocument(scope.Project, record)}, nil
}

// routerDocument renders the router wire shape.
func routerDocument(project string, record *routerRecord) map[string]any {
	nats := make([]any, 0, len(record.NATs))
	for _, nat := range record.NATs {
		ranges := make([]string, 0, len(nat.SourceSubnetworks))
		ranges = append(ranges, nat.SourceSubnetworks...)
		nats = append(nats, map[string]any{
			keyName: nat.Name, "sourceSubnetworkIpRanges": ranges,
		})
	}
	return map[string]any{
		"id":           idOf(record.Name, record.Generation),
		keyName:        record.Name,
		keySelfLink:    regionalSelfLink(project, record.Region, "routers", record.Name),
		keyTimestamp:   stamp(record.CreatedAt),
		keyKind:        "compute#router",
		keyNetwork:     selfLink(project, "networks", record.Network),
		"nats":         nats,
		keyDescription: "",
	}
}
