package compute

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// execute dispatches one authorized method.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodNetworksCreate, methodNetworksGet, methodNetworksList, methodNetworksDelete,
		methodNetworksAddPeering, methodNetworksRemovePeering:
		return p.executeNetwork(call, request, scope)
	case methodSubnetworksCreate, methodSubnetworksGet, methodSubnetworksList, methodSubnetworksDelete,
		methodSubnetworksGetIam, methodSubnetworksSetIam:
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
	case methodOperationsGet, methodRegionOperationsGet, methodZoneOperationsGet,
		methodOperationsWait, methodRegionOperationsWait, methodZoneOperationsWait:
		// Every operation is DONE, so wait returns at once, as it does for a
		// finished operation.
		return p.operationGet(request, scope, operationKind(request.Path))
	case methodOperationsList, methodRegionOperationsList, methodZoneOperationsList:
		return p.operationsList(request, scope, operationKind(request.Path))
	case methodOperationsDelete, methodRegionOperationsDelete, methodZoneOperationsDelete:
		return p.operationDelete(request, scope, operationKind(request.Path))
	default:
		return nil, gap("this method is not implemented")
	}
}

// operationKind is the scope of the operations collection a path addresses.
func operationKind(path map[string]string) string {
	switch {
	case path[keyZone] != "":
		return keyZone
	case path[keyRegion] != "":
		return keyRegion
	default:
		return "global"
	}
}

// operationLocation is the region or zone a path's operations collection is in.
func operationLocation(path map[string]string) string {
	return path[keyRegion] + path[keyZone]
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
	case keyRegion:
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
	var document map[string]any
	p.mu.RLock()
	if record := p.records[operationKey(scope, kind, request.Path)]; record != nil {
		document = operationDocument(scope.Project, record)
	}
	p.mu.RUnlock()
	if document == nil {
		return nil, notFound(operationRef(scope.Project, kind, request.Path[keyRegion], request.Path[keyZone], request.Path["operation"]))
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// operationKey is an operation record's key: project, scope kind, its region
// or zone, and name, so a regional operation is found only in its region.
func operationKey(scope state.Scope, kind string, path map[string]string) string {
	return scope.Project + "/" + kind + "/" + operationLocation(path) + "/" + path["operation"]
}

// operationDelete forgets one operation; its work is unaffected.
func (p *Provider) operationDelete(request *gateway.RESTRequest, scope state.Scope, kind string) (*gateway.RESTResponse, error) {
	key := operationKey(scope, kind, request.Path)
	p.mu.Lock()
	_, found := p.records[key]
	delete(p.records, key)
	p.mu.Unlock()
	if !found {
		return nil, notFound(operationRef(scope.Project, kind, request.Path[keyRegion], request.Path[keyZone], request.Path["operation"]))
	}
	return &gateway.RESTResponse{Status: http.StatusNoContent}, nil
}

// operationRef names one operation in Compute's not-found message.
func operationRef(project, kind, region, zone, name string) string {
	switch kind {
	case keyRegion:
		return regionalRef(project, region, "operations", name)
	case keyZone:
		return zonalRef(project, zone, "operations", name)
	default:
		return globalRef(project, "operations", name)
	}
}

// operationsList returns one page of the request's region, zone, or global
// operations collection.
func (p *Provider) operationsList(request *gateway.RESTRequest, scope state.Scope, kind string) (*gateway.RESTResponse, error) {
	options, err := parseList(request.Query, false)
	if err != nil {
		return nil, err
	}
	prefix := scope.Project + "/" + kind + "/" + operationLocation(request.Path) + "/"
	p.mu.RLock()
	keys := make([]string, 0, len(p.records))
	for key := range p.records {
		if strings.HasPrefix(key, prefix) {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var items []map[string]any
	for _, key := range keys {
		if document := operationDocument(scope.Project, p.records[key]); options.match(document) {
			items = append(items, document)
		}
	}
	p.mu.RUnlock()
	return options.page(items, "compute#operationList")
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
	// Named like Compute's operation-MILLIS-..., so name order is creation order.
	record := &operationRecord{
		ID: strconv.FormatInt(generation, 10), Name: fmt.Sprintf("operation-%013d-%08d", now.UnixMilli(), generation),
		Kind: kind, Region: region, Type: opType, Target: target, Created: now, Done: true, Error: failure,
	}
	p.mu.Lock()
	p.records[scope.Project+"/"+kind+"/"+region+"/"+record.Name] = record
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

// listOptions is a list request's filter, order, and page, as the Compute v1
// list methods document them.
type listOptions struct {
	terms  []filterTerm
	newest bool       // orderBy "creationTimestamp desc"
	paging url.Values // pageSize and pageToken for provider.Page
	token  string
}

// filterTerm is one comparison of a list filter: an AIP-160 = or != on a
// value, or a regular-expression eq or ne that must match the whole field.
type filterTerm struct {
	field  string
	negate bool
	value  string
	regex  *regexp.Regexp
}

// maxListResults is Compute's default and largest page size.
const maxListResults = 500

// parseList reads a list request's options (recorded: compute-lists-operations-iam).
// labeled reports whether the collection has labels a filter can name.
func parseList(query url.Values, labeled bool) (listOptions, error) {
	options := listOptions{paging: url.Values{}}
	filter, order := strings.TrimSpace(query.Get("filter")), query.Get("orderBy")
	if filter != "" && order != "" {
		return listOptions{}, invalidField(fmt.Sprintf("Invalid value for field 'orderBy': '%s'. Specifying both a list filter and sort order is not currently supported", order))
	}
	newest, err := parseOrder(order)
	if err != nil {
		return listOptions{}, err
	}
	options.newest = newest
	// maxResults is a uint32 from 0 to 500: 0 is the default, and a larger
	// value is accepted as 500.
	if text := query.Get("maxResults"); text != "" {
		size, err := strconv.ParseUint(text, 10, 32)
		if err != nil {
			return listOptions{}, invalidField("Invalid value at 'header.max_results' (TYPE_UINT32), " + strconv.Quote(text))
		}
		if size > 0 {
			options.paging.Set("pageSize", strconv.FormatUint(size, 10))
		}
	}
	token := query.Get("pageToken")
	options.paging.Set("pageToken", token)
	options.token = token
	terms, err := parseFilter(filter, labeled)
	options.terms = terms
	return options, err
}

// parseOrder reads orderBy: name (the default) or creationTimestamp desc,
// which reports newest. Compute refuses the other direction of either and any
// other field (recorded: compute-lists-operations-iam).
func parseOrder(order string) (bool, error) {
	field, direction, _ := strings.Cut(strings.TrimSpace(order), " ")
	direction = strings.ToLower(strings.TrimSpace(direction))
	var reason string
	switch {
	case order == "" || (field == keyName && (direction == "" || direction == "asc")):
		return false, nil
	case field == keyTimestamp && direction == "desc":
		return true, nil
	case field == keyName:
		reason = "operator DESC not supported on the specified field."
	case field == keyTimestamp:
		reason = "operator ASC not supported on the specified field."
	default:
		reason = "sorting not supported on the specified field."
	}
	return false, invalidField(fmt.Sprintf("Invalid value for field 'orderBy': '%s'. Invalid sort order expression: %s", order, reason))
}

// filterComparison matches one comparison: a field, an operator, and the
// value, quoted or bare.
var filterComparison = regexp.MustCompile(`^([a-zA-Z][a-zA-Z0-9_.-]*)\s*(!=|=|\s(?:eq|ne)\s)\s*(.*)$`)

// parseFilter parses a Compute list filter: one bare comparison, or
// parenthesized comparisons joined by spaces or AND, all AIP-160 (=, !=) or
// all regular expressions (eq, ne). Fields are name and, on a labeled
// collection, labels.KEY. Every other form is a gap, never ignored.
func parseFilter(filter string, labeled bool) ([]filterTerm, error) {
	if filter == "" {
		return nil, nil
	}
	var parts []string
	if strings.HasPrefix(filter, "(") {
		for rest := filter; rest != ""; rest = strings.TrimSpace(strings.TrimPrefix(strings.TrimSpace(rest), "AND ")) {
			closing := strings.Index(rest, ")")
			if !strings.HasPrefix(rest, "(") || closing < 0 {
				return nil, gap("this list filter form is not modeled")
			}
			parts = append(parts, strings.TrimSpace(rest[1:closing]))
			rest = rest[closing+1:]
		}
	} else {
		parts = []string{filter}
	}
	terms := make([]filterTerm, 0, len(parts))
	for _, part := range parts {
		term, err := parseTerm(part, labeled)
		if err != nil {
			return nil, err
		}
		if len(terms) > 0 && (term.regex == nil) != (terms[0].regex == nil) {
			return nil, gap("a list filter mixing regular expressions and comparisons is not modeled")
		}
		terms = append(terms, term)
	}
	return terms, nil
}

// parseTerm parses one comparison of a filter.
func parseTerm(text string, labeled bool) (filterTerm, error) {
	match := filterComparison.FindStringSubmatch(text)
	if match == nil {
		return filterTerm{}, gap("this list filter form is not modeled")
	}
	field, operator, value := match[1], strings.TrimSpace(match[2]), unquote(strings.TrimSpace(match[3]))
	_, label := strings.CutPrefix(field, "labels.")
	if field != keyName && (!label || !labeled) {
		return filterTerm{}, gap("a list filter on " + field + " is not modeled")
	}
	term := filterTerm{field: field, negate: operator == "!=" || operator == "ne", value: value}
	if operator != "eq" && operator != "ne" {
		if strings.Contains(value, "*") {
			return filterTerm{}, gap("a list filter wildcard is not modeled")
		}
		return term, nil
	}
	pattern, err := filterRegex(value, label)
	term.regex = pattern
	return term, err
}

// filterRegex compiles an eq or ne filter value, which must match the whole
// field. Regular expressions on labels are a gap.
func filterRegex(value string, label bool) (*regexp.Regexp, error) {
	if label {
		return nil, gap("a regular-expression list filter on labels is not modeled")
	}
	pattern, err := regexp.Compile("^(?:" + value + ")$")
	if err != nil {
		return nil, gap("this list filter regular expression is not modeled")
	}
	return pattern, nil
}

// unquote strips one pair of double or single quotes.
func unquote(value string) string {
	if len(value) >= 2 && (value[0] == '"' || value[0] == '\'') && value[len(value)-1] == value[0] {
		return strings.ReplaceAll(value[1:len(value)-1], `\"`, `"`)
	}
	return value
}

// match reports whether a rendered document passes every filter term.
func (o listOptions) match(document map[string]any) bool {
	for _, term := range o.terms {
		// A missing label matches neither = nor != (recorded).
		value, present := fieldText(document, term.field)
		equal := value == term.value
		if term.regex != nil {
			equal = term.regex.MatchString(value)
		}
		if !present || equal == term.negate {
			return false
		}
	}
	return true
}

// fieldText reads a filterable field of a rendered document: name or one
// label's value.
func fieldText(document map[string]any, field string) (string, bool) {
	if key, label := strings.CutPrefix(field, "labels."); label {
		labels, _ := document["labels"].(map[string]string)
		value, present := labels[key]
		return value, present
	}
	value, present := document[field].(string)
	return value, present
}

// page orders the matched documents and renders the requested page as a
// Compute list response.
func (o listOptions) page(items []map[string]any, kind string) (*gateway.RESTResponse, error) {
	if o.newest {
		slices.SortStableFunc(items, func(a, b map[string]any) int { return createdAt(b).Compare(createdAt(a)) })
	}
	page, next, err := provider.Page(items, o.paging, provider.Paging{
		Default: maxListResults, Max: maxListResults,
		InvalidToken: invalidField("Invalid value for field 'pageToken': '" + o.token + "'. Invalid pagination token"),
	}, gap)
	if err != nil {
		return nil, err
	}
	body := map[string]any{keyItems: page, keyKind: kind}
	if page == nil {
		body[keyItems] = []map[string]any{}
	}
	if next != "" {
		body["nextPageToken"] = next
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// createdAt reads a rendered document's creation time: creationTimestamp,
// or an operation's insertTime.
func createdAt(document map[string]any) time.Time {
	text, _ := document[keyTimestamp].(string)
	if text == "" {
		text, _ = document["insertTime"].(string)
	}
	moment, _ := time.Parse(time.RFC3339Nano, text)
	return moment
}
