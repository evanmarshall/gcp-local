package compute

import (
	"encoding/base64"
	"net/http"
	"slices"
	"sort"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
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
	case methodInstancesInsert, methodInstancesGet, methodInstancesList, methodInstancesDelete:
		return p.executeInstances(call, request, scope)
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
	case methodZoneOperationsGet:
		return p.operationGet(request, scope, keyZone)
	case methodOperationsList:
		return p.operationsList(request, scope, "global")
	default:
		return nil, gap("this method is not implemented")
	}
}

// networkDocument renders the recorded network wire shape. Optional fields
// appear only when set, as the recorded document omits them.
func networkDocument(project string, record *networkRecord, subnets []any, peerings []any) map[string]any {
	id := idOf(record.Name, record.Generation)
	document := map[string]any{
		"id":                                    id,
		keyName:                                 record.Name,
		keySelfLink:                             selfLink(project, "networks", record.Name),
		"selfLinkWithId":                        selfLink(project, "networks", id),
		keyTimestamp:                            stamp(record.CreatedAt),
		keyKind:                                 "compute#network",
		keyAutoSubnets:                          record.AutoCreateSubnetworks,
		keyRoutingConfig:                        map[string]any{"routingMode": record.RoutingMode, "bgpBestPathSelectionMode": "LEGACY"},
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
// in the project's global scope, so the lookup walks the single scope. The
// caller holds the scope lock.
func (s *scopeState) subnetworksOf(project, network string) []any {
	links := []any{}
	for _, key := range sortedKeys(s.subnetworks) {
		if record := s.subnetworks[key]; record.Network == network {
			links = append(links, regionalSelfLink(project, record.Region, "subnetworks", record.Name))
		}
	}
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
		keyPrivateIPv6:                 "DISABLE_GOOGLE_ACCESS",
		"allowSubnetCidrRoutesOverlap": false,
		keyFingerprint:                 fingerprint(record.Name, record.Generation),
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
		keyLogConfig:   map[string]any{"enable": false},
		keyDescription: record.Description,
	}
	setList(document, "allowed", record.Allowed)
	setList(document, "denied", record.Denied)
	setList(document, "sourceRanges", record.SourceRanges)
	setList(document, "destinationRanges", record.DestinationRanges)
	setList(document, "targetTags", record.TargetTags)
	setList(document, "sourceTags", record.SourceTags)
	setList(document, "targetServiceAccounts", record.TargetServiceAccounts)
	setList(document, "sourceServiceAccounts", record.SourceServiceAccounts)
	return document
}

// setList sets one optional list field when it is non-empty. The document
// gets a copy, so it never aliases a stored record.
func setList[T any](document map[string]any, key string, values []T) {
	if len(values) > 0 {
		document[key] = slices.Clone(values)
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
		keyDescription: record.Description,
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
		keyStatus:       "RUNNING",
		"progress":      0,
		"insertTime":    stamp(record.Created),
		"operationType": record.Type,
		"targetLink":    record.Target,
		keyKind:         "compute#operation",
		"user":          "gcp-local",
	}
	switch record.Kind {
	case "region":
		entry[keySelfLink] = regionalSelfLink(project, record.Region, "operations", record.Name)
		entry[keyRegion] = regionLink(project, record.Region)
	case keyZone:
		entry[keySelfLink] = zoneLink(project, record.Region) + "/operations/" + record.Name
		entry[keyZone] = zoneLink(project, record.Region)
	default:
		entry[keySelfLink] = selfLink(project, "operations", record.Name)
	}
	if !record.Done {
		return entry
	}
	entry[keyStatus] = "DONE"
	entry["progress"] = 100
	entry["endTime"] = stamp(record.Created)
	if record.Error != nil {
		entry["httpErrorStatusCode"] = 400
		entry["httpErrorMessage"] = "BAD REQUEST"
		entry[keyError] = map[string]any{keyErrors: []any{map[string]any{keyCode: record.Error.Code, keyMessage: record.Error.Message}}}
	}
	return entry
}

func (p *Provider) operationGet(request *gateway.RESTRequest, scope state.Scope, kind string) (*gateway.RESTResponse, error) {
	name := scope.Project + "/" + kind + "/" + request.Path["operation"]
	p.mu.RLock()
	var document map[string]any
	if record := p.records[name]; record != nil {
		document = operationDocument(scope.Project, record)
	}
	p.mu.RUnlock()
	if document == nil {
		return nil, notFound(operationRef(scope.Project, kind, request.Path["region"], request.Path[keyZone], request.Path["operation"]))
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// operationRef names one operation in Compute's not-found message.
func operationRef(project, kind, region, zone, name string) string {
	switch kind {
	case "region":
		return regionalRef(project, region, "operations", name)
	case keyZone:
		return zonalRef(project, zone, "operations", name)
	default:
		return globalRef(project, "operations", name)
	}
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
		entry := map[string]any{
			keyName: nat.Name, "natIpAllocateOption": nat.Allocation, "sourceSubnetworkIpRangesToNat": nat.Ranges,
		}
		subnets := make([]any, 0, len(nat.Subnetworks))
		for _, subnet := range nat.Subnetworks {
			subnets = append(subnets, map[string]any{
				keyName: regionalSelfLink(project, record.Region, "subnetworks", subnet), "sourceIpRangesToNat": []any{"ALL_IP_RANGES"},
			})
		}
		setList(entry, "subnetworks", subnets)
		nats = append(nats, entry)
	}
	return map[string]any{
		"id":           idOf(record.Name, record.Generation),
		keyName:        record.Name,
		keySelfLink:    regionalSelfLink(project, record.Region, "routers", record.Name),
		keyTimestamp:   stamp(record.CreatedAt),
		keyKind:        "compute#router",
		keyNetwork:     selfLink(project, "networks", record.Network),
		keyNATs:        nats,
		keyDescription: record.Description,
	}
}
