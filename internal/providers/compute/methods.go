package compute

import (
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net"
	"slices"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func (p *Provider) executeNetwork(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodNetworksCreate:
		return p.createNetwork(request, scope)
	case methodNetworksGet:
		return p.getNetwork(request, scope)
	case methodNetworksList:
		return p.listNetworks(request, scope)
	case methodNetworksAddPeering:
		return p.addPeering(request, scope)
	case methodNetworksRemovePeering:
		return p.removePeering(request, scope)
	default:
		return p.deleteNetwork(call, request, scope)
	}
}

// createNetwork validates and stores one network, then reports its operation.
func (p *Provider) createNetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	project := scope.Project
	body, err := decodeBody[networkBody](request.JSON, networkSchema)
	if err != nil {
		return nil, err
	}
	name, err := checkName(body.Name, "")
	if err != nil {
		return nil, err
	}
	record := &networkRecord{
		Name: name, Description: body.Description, AutoCreateSubnetworks: body.AutoCreateSubnetworks == nil || bool(*body.AutoCreateSubnetworks),
		RoutingMode: "REGIONAL", MTU: int(body.MTU), CreatedAt: p.clock(),
	}
	if body.RoutingConfig != nil && body.RoutingConfig.RoutingMode != "" {
		record.RoutingMode = body.RoutingConfig.RoutingMode
	}
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.networks[name] != nil {
			return errAlreadyExists
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: globalName(project, collectionNetworks, name), Service: serviceName,
			Kind: kindNetwork, Parent: provider.ProjectResource(project), Scope: scope,
		}); err != nil {
			return gap("index the network")
		}
		record.Generation = s.nextGeneration()
		s.networks[name] = record
		return nil
	})
	if err != nil {
		return nil, createError(err, globalRef(project, "networks", name))
	}
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "networks", name))
}

// createError maps a create callback's failure onto its status: a duplicate
// is a conflict, and anything else (a missing reference, an index refusal) is
// already a wire status.
func createError(err error, path string) error {
	if errors.Is(err, errAlreadyExists) {
		return conflict(path)
	}
	return err
}

// checkName validates a body's resource name, or the request path's when the
// body names none (an update).
func checkName(name, fallback string) (string, error) {
	if name == "" {
		name = fallback
	}
	if !uidPattern.MatchString(name) {
		return "", status.Errorf(codes.InvalidArgument, "Invalid value for field 'resource.name': '%s'. Must be a match of regex '(?:[a-z](?:[-a-z0-9]{0,61}[a-z0-9])?)'", name)
	}
	return name, nil
}

// checkCIDR validates one IPv4 CIDR range field.
func checkCIDR(field, value string) (string, error) {
	if !cidrPattern.MatchString(value) {
		return "", status.Errorf(codes.InvalidArgument, "Invalid value for field 'resource.%s': '%s'. Must be a CIDR range.", field, value)
	}
	return value, nil
}

func (p *Provider) getNetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["network"]
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.networks[name]; record != nil {
			document = s.renderNetwork(scope.Project, record)
		}
		return nil
	})
	if document == nil {
		return nil, notFound(globalRef(scope.Project, "networks", name))
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// listRecords renders one page of a collection as Compute lists it: the
// records keep selects (the request's region or zone; nil keeps all) that
// match the request's filter, in name order unless orderBy says otherwise.
func listRecords[T any](p *Provider, request *gateway.RESTRequest, scope state.Scope, listKind string, records func(*scopeState) map[string]*T,
	keep func(*T) bool, render func(*scopeState, *T) map[string]any,
) (*gateway.RESTResponse, error) {
	options, err := parseList(request.Query, listKind == "compute#instanceList")
	if err != nil {
		return nil, err
	}
	var items []map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		values := records(s)
		for _, key := range sortedKeys(values) {
			if record := values[key]; keep == nil || keep(record) {
				if document := render(s, record); options.match(document) {
					items = append(items, document)
				}
			}
		}
		return nil
	})
	return options.page(items, listKind)
}

// listNetworks returns the scope's networks.
func (p *Provider) listNetworks(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, request, scope, "compute#networkList", func(s *scopeState) map[string]*networkRecord { return s.networks }, nil,
		func(s *scopeState, record *networkRecord) map[string]any {
			return s.renderNetwork(scope.Project, record)
		})
}

// deleteNetwork removes one network. Compute accepts the delete of a network
// that other resources still use and reports RESOURCE_IN_USE_BY_ANOTHER_RESOURCE
// in the finished operation, keeping the network.
func (p *Provider) deleteNetwork(_ gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["network"]
	target := selfLink(scope.Project, "networks", name)
	found, user, err := p.removeUnlessUsed(scope, globalName(scope.Project, collectionNetworks, name),
		func(s *scopeState) bool { return s.networks[name] != nil },
		func(s *scopeState) string { return s.networkUser(name) },
		func(s *scopeState) { delete(s.networks, name) })
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound(globalRef(scope.Project, "networks", name))
	}
	if user != "" {
		return p.failedOperation(scope, "global", "", "delete", target, &operationError{
			Code:    "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE",
			Message: "The network resource '" + target + "' is already being used by '" + user + "'",
		})
	}
	return p.newOperation(scope, "global", "", "delete", target)
}

// removeUnlessUsed deletes one resource and its index entry under the scope
// lock unless another resource still uses it. It reports whether the resource
// existed and, when the delete was refused, the resource that uses it.
func (p *Provider) removeUnlessUsed(scope state.Scope, resource string, exists func(*scopeState) bool,
	user func(*scopeState) string, remove func(*scopeState),
) (bool, string, error) {
	found, usedBy := false, ""
	err := p.scopes.With(scope, func(s *scopeState) error {
		if found = exists(s); !found {
			return nil
		}
		if usedBy = user(s); usedBy != "" {
			return nil
		}
		if err := p.unindex(scope, resource); err != nil {
			return err
		}
		remove(s)
		return nil
	})
	return found, usedBy, err //nolint:wrapcheck // unindex already wraps
}

// unindex removes one resource's index entry; the caller holds the scope lock
// and deletes the record only when this succeeds.
func (p *Provider) unindex(scope state.Scope, resource string) error {
	if err := p.index.Remove(serviceName, scope, resource); err != nil {
		return fmt.Errorf("unregister %s: %w", resource, err)
	}
	return nil
}

// networkUser names one resource that still references the network, if any.
func (s *scopeState) networkUser(network string) string {
	for _, user := range []string{
		firstUser("subnetworks", s.subnetworks, func(r *subnetworkRecord) bool { return r.Network == network }),
		firstUser("firewalls", s.firewalls, func(r *firewallRecord) bool { return r.Network == network }),
		firstUser("routes", s.routes, func(r *routeRecord) bool { return r.Network == network }),
		firstUser("routers", s.routers, func(r *routerRecord) bool { return r.Network == network }),
		s.instanceUser(func(r *instanceRecord) bool { return r.Network == network }),
	} {
		if user != "" {
			return user
		}
	}
	return ""
}

// firstUser names the first record, in key order, that uses a resource. A
// regional record's key is region/name, so it reads regions/R/kind/name.
func firstUser[T any](kind string, records map[string]T, uses func(T) bool) string {
	for _, key := range sortedKeys(records) {
		if !uses(records[key]) {
			continue
		}
		if region, name, regional := strings.Cut(key, "/"); regional {
			return "regions/" + region + "/" + kind + "/" + name
		}
		return kind + "/" + key
	}
	return ""
}

// regionalKey is the scope-state key of one regional resource. Subnetworks and
// routers are regional: the same name may exist once in each region.
func regionalKey(region, name string) string {
	return region + "/" + name
}

// instanceUser names the first instance, in zone/name order, that uses a resource.
func (s *scopeState) instanceUser(uses func(*instanceRecord) bool) string {
	for _, key := range sortedKeys(s.instances) {
		if record := s.instances[key]; uses(record) {
			return "zones/" + record.Zone + "/instances/" + record.Name
		}
	}
	return ""
}

// sortedKeys returns the map's keys in order, for deterministic choices.
func sortedKeys[T any](values map[string]T) []string {
	return slices.Sorted(maps.Keys(values))
}

// Sentinel record states for the shared handlers.
var (
	errMissing       = errors.New("missing")
	errAlreadyExists = errors.New("already exists")
)

func (p *Provider) executeSubnetwork(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodSubnetworksCreate:
		return p.createSubnetwork(request, scope)
	case methodSubnetworksGet:
		return p.getSubnetwork(request, scope)
	case methodSubnetworksList:
		return p.listSubnetworks(request, scope)
	case methodSubnetworksGetIam, methodSubnetworksSetIam:
		return p.subnetworkIam(call, request, scope)
	default:
		return p.deleteSubnetwork(request, scope)
	}
}

// createSubnetwork validates and stores one subnetwork. A range that overlaps
// another subnetwork of the network is refused asynchronously, as compute does:
// the operation finishes DONE with INVALID_USAGE and nothing is created.
func (p *Provider) createSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	project, region := scope.Project, request.Path["region"]
	body, name, cidr, err := parseSubnetwork(request, region)
	if err != nil {
		return nil, err
	}
	network, err := p.referencedNetwork(scope, body.Network)
	if err != nil {
		return nil, err
	}
	target := regionalSelfLink(project, region, "subnetworks", name)
	var overlap string
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.networks[network] == nil {
			return missingReference()
		}
		if s.subnetworks[regionalKey(region, name)] != nil {
			return errAlreadyExists
		}
		if overlap = s.overlappingSubnet(network, cidr); overlap != "" {
			return nil
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: regionalName(project, region, collectionSubnetworks, name), Service: serviceName,
			Kind: kindSubnetwork, Parent: provider.ProjectResource(project),
			Scope: scope,
		}); err != nil {
			return gap("index the subnetwork")
		}
		s.subnetworks[regionalKey(region, name)] = &subnetworkRecord{
			Name: name, Description: body.Description, Network: network, Region: region,
			IPCIDRRange: cidr, Gateway: defaultGateway(cidr), PrivateAccess: bool(body.PrivateAccess),
			Generation: 1, CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, regionalRef(project, region, "subnetworks", name))
	}
	if overlap != "" {
		return p.failedOperation(scope, "region", region, "insert", target, &operationError{
			Code:    "INVALID_USAGE",
			Message: "Invalid IPCidrRange: " + cidr + " conflicts with existing subnetwork " + overlap + ".",
		})
	}
	return p.newOperation(scope, "region", region, "insert", target)
}

// parseSubnetwork decodes and validates a subnetwork insert body, returning
// it with its name and range. Terraform sends the region as a URL; only the
// request's region is modeled.
func parseSubnetwork(request *gateway.RESTRequest, region string) (*subnetworkBody, string, string, error) {
	body, err := decodeBody[subnetworkBody](request.JSON, subnetworkSchema)
	if err != nil {
		return nil, "", "", err
	}
	name, err := checkName(body.Name, "")
	if err != nil {
		return nil, "", "", err
	}
	cidr, err := checkCIDR("ipCidrRange", body.IPCIDRRange)
	if err != nil {
		return nil, "", "", err
	}
	if body.Region != "" && provider.LastSegment(body.Region) != region {
		return nil, "", "", gap("a subnetwork body region other than the request's region is not modeled")
	}
	return body, name, cidr, nil
}

// overlappingSubnet names, with its region, a subnetwork of the network in
// any region whose range overlaps cidr: ranges are unique across a network.
func (s *scopeState) overlappingSubnet(network, cidr string) string {
	_, candidate, err := net.ParseCIDR(cidr)
	if err != nil {
		return ""
	}
	for _, key := range sortedKeys(s.subnetworks) {
		subnet := s.subnetworks[key]
		_, existing, parseErr := net.ParseCIDR(subnet.IPCIDRRange)
		if subnet.Network != network || parseErr != nil {
			continue
		}
		if existing.Contains(candidate.IP) || candidate.Contains(existing.IP) {
			return "'" + subnet.Name + "' in region '" + subnet.Region + "'"
		}
	}
	return ""
}

func (p *Provider) getSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	region, name := request.Path["region"], request.Path["subnetwork"]
	return getRecord(p, scope, regionalRef(scope.Project, region, "subnetworks", name), regionalKey(region, name),
		func(s *scopeState) map[string]*subnetworkRecord { return s.subnetworks }, subnetworkDocument)
}

// listSubnetworks returns the region's subnetworks.
func (p *Provider) listSubnetworks(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	region := request.Path["region"]
	return listRecords(p, request, scope, "compute#subnetworkList", func(s *scopeState) map[string]*subnetworkRecord { return s.subnetworks },
		func(record *subnetworkRecord) bool { return record.Region == region }, inProject(scope, subnetworkDocument))
}

// inProject adapts a document renderer that needs only the project.
func inProject[T any](scope state.Scope, render func(string, *T) map[string]any) func(*scopeState, *T) map[string]any {
	return func(_ *scopeState, record *T) map[string]any { return render(scope.Project, record) }
}

// deleteSubnetwork removes one subnetwork. Like a network, a subnetwork that
// an instance still uses is refused in the finished operation and kept.
func (p *Provider) deleteSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name, region := request.Path["subnetwork"], request.Path["region"]
	target := regionalSelfLink(scope.Project, region, "subnetworks", name)
	found, user, err := p.removeUnlessUsed(scope, regionalName(scope.Project, region, collectionSubnetworks, name),
		func(s *scopeState) bool { return s.subnetworks[regionalKey(region, name)] != nil },
		func(s *scopeState) string {
			return s.instanceUser(func(r *instanceRecord) bool { return r.Region == region && r.Subnetwork == name })
		},
		func(s *scopeState) { delete(s.subnetworks, regionalKey(region, name)) })
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound(regionalRef(scope.Project, region, "subnetworks", name))
	}
	if user != "" {
		return p.failedOperation(scope, "region", region, "delete", target, &operationError{
			Code:    "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE",
			Message: "The subnetwork resource '" + target + "' is already being used by '" + user + "'",
		})
	}
	return p.newOperation(scope, "region", region, "delete", target)
}

// networkName strips a full network self-link or name into the plain name.
func networkName(value string) string {
	if index := strings.LastIndex(value, "/networks/"); index >= 0 {
		return value[index+len("/networks/"):]
	}
	return value
}

// referencedNetwork resolves a body's network reference to a network of the
// scope: a reference into another project is Shared VPC, a gap, and a
// missing network is Compute's missing reference.
func (p *Provider) referencedNetwork(scope state.Scope, reference string) (string, error) {
	if err := sameProject(scope.Project, reference); err != nil {
		return "", err
	}
	name := networkName(reference)
	var found bool
	_ = p.scopes.With(scope, func(s *scopeState) error { found = s.networks[name] != nil; return nil })
	if !found {
		return "", missingReference()
	}
	return name, nil
}

// defaultGateway derives the first host address of a CIDR range.
func defaultGateway(cidr string) string {
	ip := net.ParseIP(strings.SplitN(cidr, "/", 2)[0]).To4()
	if ip == nil {
		return cidr
	}
	ip[3]++
	return ip.String()
}

func (p *Provider) executeFirewall(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodFirewallsCreate, methodFirewallsUpdate:
		return p.writeFirewall(call, request, scope)
	case methodFirewallsGet:
		return p.getFirewall(request, scope)
	case methodFirewallsList:
		return p.listFirewalls(request, scope)
	default:
		return p.deleteFirewall(request, scope)
	}
}

// writeFirewall creates or replaces one firewall rule.
func (p *Provider) writeFirewall(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeBody[firewallBody](request.JSON, firewallSchema)
	if err != nil {
		return nil, err
	}
	name, err := checkName(body.Name, request.Path["firewall"])
	if err != nil {
		return nil, err
	}
	record, err := firewallFrom(body)
	if err != nil {
		return nil, err
	}
	if _, err := p.referencedNetwork(scope, body.Network); err != nil {
		return nil, err
	}
	path := globalRef(scope.Project, "firewalls", name)
	err = p.storeFirewall(scope, record, name, call.Method == methodFirewallsCreate)
	if err != nil {
		if errors.Is(err, errMissing) {
			return nil, notFound(path)
		}
		return nil, createError(err, path)
	}
	operationType := "insert"
	if call.Method == methodFirewallsUpdate {
		operationType = "update"
	}
	return p.newOperation(scope, "global", "", operationType, selfLink(scope.Project, "firewalls", name))
}

// firewallFrom validates a firewall body as Compute does and returns the
// rule it describes (recorded: vm-firewall-service-accounts). Tags and
// service accounts can't be combined in one rule, in either role, and an
// EGRESS rule names no sources but ranges.
func firewallFrom(body *firewallBody) (*firewallRecord, error) {
	direction := body.Direction
	if direction == "" {
		direction = directionIngress
	}
	if err := checkScopes(body, direction); err != nil {
		return nil, err
	}
	priority := 1000
	if body.Priority != nil {
		priority = int(*body.Priority)
	}
	sources := body.SourceRanges
	if direction == directionIngress && len(sources) == 0 && !scopedSources(body) {
		sources = []string{"0.0.0.0/0"} // Compute stores the default source (recorded)
	}
	return &firewallRecord{
		Network: networkName(body.Network), Direction: direction, Description: body.Description,
		Allowed: body.Allowed, Denied: body.Denied, Priority: priority, Disabled: bool(body.Disabled),
		SourceRanges: sources, DestinationRanges: body.DestinationRanges,
		SourceTags: body.SourceTags, TargetTags: body.TargetTags,
		SourceServiceAccounts: body.SourceServiceAccounts, TargetServiceAccounts: body.TargetServiceAccounts,
	}, nil
}

// scopedSources reports whether a firewall body names source tags or accounts.
func scopedSources(body *firewallBody) bool {
	return len(body.SourceTags) > 0 || len(body.SourceServiceAccounts) > 0
}

// checkScopes refuses the scope combinations Compute refuses, with its words.
func checkScopes(body *firewallBody, direction string) error {
	tags := len(body.SourceTags) > 0 || len(body.TargetTags) > 0
	accounts := len(body.SourceServiceAccounts) > 0 || len(body.TargetServiceAccounts) > 0
	switch {
	case tags && accounts:
		return badRequest("ServiceAccounts and Tags are mutually exclusive and can't be combined in the same firewall rule.")
	case direction == directionEgress && scopedSources(body):
		return badRequest("Should not specify source tag or service account for egress direction.")
	case direction != directionIngress && direction != directionEgress:
		return status.Errorf(codes.InvalidArgument, "Invalid value for field 'resource.direction': '%s'.", direction)
	}
	return nil
}

// storeFirewall stores one validated firewall rule and its index entry under
// the scope lock; a refused write changes nothing.
func (p *Provider) storeFirewall(scope state.Scope, rule *firewallRecord, name string, create bool) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // sentinels and wire statuses
		if s.networks[rule.Network] == nil {
			return missingReference()
		}
		existing := s.firewalls[name]
		if create && existing != nil {
			return errAlreadyExists
		}
		if !create && existing == nil {
			return errMissing
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: globalName(scope.Project, collectionFirewalls, name), Service: serviceName,
			Kind: kindFirewall, Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return gap("index the firewall")
		}
		rule.Name = name
		if existing == nil {
			rule.CreatedAt, rule.Generation = p.clock(), s.nextGeneration()
		} else {
			rule.CreatedAt, rule.Generation = existing.CreatedAt, existing.Generation+1
		}
		s.firewalls[name] = rule
		return nil
	})
}

func (p *Provider) getFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["firewall"]
	return getRecord(p, scope, globalRef(scope.Project, "firewalls", name), name, func(s *scopeState) map[string]*firewallRecord { return s.firewalls }, firewallDocument)
}

// listFirewalls returns the scope's firewall rules.
func (p *Provider) listFirewalls(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, request, scope, "compute#firewallList", func(s *scopeState) map[string]*firewallRecord { return s.firewalls }, nil,
		inProject(scope, firewallDocument))
}

// deleteFirewall removes one firewall rule.
func (p *Provider) deleteFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return deleteGlobal(p, scope, collectionFirewalls, request.Path["firewall"], func(s *scopeState) map[string]*firewallRecord { return s.firewalls })
}

// deleteGlobal removes one global resource and its index entry under the
// scope lock and reports its operation; records picks the kind's map.
func deleteGlobal[T any](p *Provider, scope state.Scope, collection, name string, records func(*scopeState) map[string]T) (*gateway.RESTResponse, error) {
	found := false
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if _, found = records(s)[name]; !found {
			return nil
		}
		if err := p.unindex(scope, globalName(scope.Project, collection, name)); err != nil {
			return err
		}
		delete(records(s), name)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // unindex already wraps
	}
	if !found {
		return nil, notFound(globalRef(scope.Project, collection, name))
	}
	return p.newOperation(scope, "global", "", "delete", selfLink(scope.Project, collection, name))
}

// getRecord renders the record at key under the scope lock; records picks the
// kind's map, and path names the resource in a not-found refusal.
func getRecord[T any](p *Provider, scope state.Scope, path, key string, records func(*scopeState) map[string]*T, render func(string, *T) map[string]any) (*gateway.RESTResponse, error) {
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := records(s)[key]; record != nil {
			document = render(scope.Project, record)
		}
		return nil
	})
	if document == nil {
		return nil, notFound(path)
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// textOr returns the first non-empty text of the field and the fallback.
func textOr(value any, fallback string) string {
	if text := provider.Text(value); text != "" {
		return text
	}
	return fallback
}

func (p *Provider) executeRoute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodRoutesCreate:
		return p.createRoute(request, scope)
	case methodRoutesGet:
		return p.getRoute(request, scope)
	case methodRoutesList:
		return p.listRoutes(request, scope)
	default:
		return p.deleteRoute(request, scope)
	}
}

// createRoute validates and stores one route.
func (p *Provider) createRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	project := scope.Project
	body, err := decodeBody[routeBody](request.JSON, routeSchema)
	if err != nil {
		return nil, err
	}
	name, err := checkName(body.Name, "")
	if err != nil {
		return nil, err
	}
	cidr, err := checkCIDR("destRange", body.DestRange)
	if err != nil {
		return nil, err
	}
	network, err := p.referencedNetwork(scope, body.Network)
	if err != nil {
		return nil, err
	}
	priority := 1000
	if body.Priority != nil {
		priority = int(*body.Priority)
	}
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.networks[network] == nil {
			return missingReference()
		}
		if s.routes[name] != nil {
			return errAlreadyExists
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: globalName(project, collectionRoutes, name), Service: serviceName,
			Kind: kindRoute, Parent: provider.ProjectResource(project), Scope: scope,
		}); err != nil {
			return gap("index the route")
		}
		s.routes[name] = &routeRecord{
			Name: name, Description: body.Description, Network: network, DestRange: cidr,
			NextHopGateway: body.NextHopGateway, NextHopIP: body.NextHopIP,
			Priority: priority, Tags: body.Tags, Generation: 1, CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, globalRef(project, "routes", name))
	}
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "routes", name))
}

func (p *Provider) getRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["route"]
	return getRecord(p, scope, globalRef(scope.Project, "routes", name), name, func(s *scopeState) map[string]*routeRecord { return s.routes }, routeDocument)
}

// listRoutes returns the scope's routes.
func (p *Provider) listRoutes(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, request, scope, "compute#routeList", func(s *scopeState) map[string]*routeRecord { return s.routes }, nil,
		inProject(scope, routeDocument))
}

func (p *Provider) deleteRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return deleteGlobal(p, scope, collectionRoutes, request.Path["route"], func(s *scopeState) map[string]*routeRecord { return s.routes })
}

func (p *Provider) executeRouters(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodRoutersCreate:
		return p.createRouter(request, scope)
	case methodRoutersGet:
		return p.getRouter(request, scope)
	case methodRoutersList:
		return p.listRouters(request, scope)
	default:
		return p.deleteRouter(request, scope)
	}
}

// createRouter validates and stores one Cloud Router with NAT gateways.
func (p *Provider) createRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeBody[routerBody](request.JSON, routerSchema)
	if err != nil {
		return nil, err
	}
	name, err := checkName(body.Name, "")
	if err != nil {
		return nil, err
	}
	if networkName(body.Network) == "" {
		return nil, provider.Invalid("A router requires a network")
	}
	network, err := p.referencedNetwork(scope, body.Network)
	if err != nil {
		return nil, err
	}
	region := request.Path["region"]
	nats, natErr := p.parseNATs(body.NATs, region, scope)
	if natErr != nil {
		return nil, natErr
	}
	err = p.scopes.With(scope, func(s *scopeState) error {
		if !s.referencesExist(network, region, nats) {
			return missingReference()
		}
		if s.routers[regionalKey(region, name)] != nil {
			return errAlreadyExists
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: regionalName(scope.Project, region, collectionRouters, name), Service: serviceName,
			Kind: kindRouter, Parent: provider.ProjectResource(scope.Project),
			Scope: scope,
		}); err != nil {
			return gap("index the router")
		}
		s.routers[regionalKey(region, name)] = &routerRecord{
			Name: name, Description: body.Description, Region: region, Network: network,
			NATs: nats, Generation: 1, CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, regionalRef(scope.Project, region, "routers", name))
	}
	return p.newOperation(scope, "region", region, "insert", regionalSelfLink(scope.Project, region, "routers", name))
}

// referencesExist re-checks, under the scope lock, the network and NAT
// subnetworks a router names, so a concurrent delete cannot leave it dangling.
func (s *scopeState) referencesExist(network, region string, nats []natRecord) bool {
	if s.networks[network] == nil {
		return false
	}
	for _, nat := range nats {
		for _, subnet := range nat.Subnetworks {
			if s.subnetworks[regionalKey(region, subnet)] == nil {
				return false
			}
		}
	}
	return true
}

// allSubnetworks is Compute's NAT source selector for every subnetwork.
const allSubnetworks = "ALL_SUBNETWORKS_ALL_IP_RANGES"

// parseNATs validates the NAT gateways of one router body.
func (p *Provider) parseNATs(items []natBody, region string, scope state.Scope) ([]natRecord, error) {
	if len(items) == 0 {
		return nil, provider.Invalid("A router requires at least one NAT gateway")
	}
	nats := make([]natRecord, 0, len(items))
	for _, item := range items {
		nat, err := p.parseNAT(item, region, scope)
		if err != nil {
			return nil, err
		}
		nats = append(nats, nat)
	}
	return nats, nil
}

// NAT source selectors and the address allocation the recording used.
const (
	listOfSubnetworks = "LIST_OF_SUBNETWORKS"
	natAutoOnly       = "AUTO_ONLY"
)

// parseNAT validates one NAT gateway entry of a router body in the Compute
// v1 shape: sourceSubnetworkIpRangesToNat, and for LIST_OF_SUBNETWORKS the
// subnetworks[].name references. Manually allocated NAT addresses are not
// modeled.
func (p *Provider) parseNAT(gateway natBody, region string, scope state.Scope) (natRecord, error) {
	record := natRecord{Name: gateway.Name, Allocation: gateway.Allocation, Ranges: gateway.Ranges}
	if record.Allocation == "" {
		record.Allocation = natAutoOnly
	}
	if !uidPattern.MatchString(record.Name) || record.Ranges == "" {
		return natRecord{}, provider.Invalid("A NAT gateway requires a name and sourceSubnetworkIpRangesToNat")
	}
	if record.Allocation != natAutoOnly {
		return natRecord{}, gap("NAT gateways with manually allocated addresses are not modeled")
	}
	for _, entry := range gateway.Subnetworks {
		subnet, err := p.natSubnetwork(scope, region, entry.Name)
		if err != nil {
			return natRecord{}, err
		}
		record.Subnetworks = append(record.Subnetworks, subnet)
	}
	if (record.Ranges == listOfSubnetworks) != (len(record.Subnetworks) > 0) {
		return natRecord{}, provider.Invalid("Only a LIST_OF_SUBNETWORKS NAT gateway names subnetworks")
	}
	return record, nil
}

// subnetReference splits a subnetwork self-link, partial URL, or plain name
// into the region it names, if any, and the plain name.
func subnetReference(value string) (string, string) {
	index := strings.LastIndex(value, "/subnetworks/")
	if index < 0 {
		return "", value
	}
	name, prefix := value[index+len("/subnetworks/"):], value[:index]
	if at := strings.LastIndex(prefix, "regions/"); at >= 0 {
		return prefix[at+len("regions/"):], name
	}
	return "", name
}

// natSubnetwork resolves a NAT gateway's subnetwork reference to a
// subnetwork of the router's region.
func (p *Provider) natSubnetwork(scope state.Scope, region, reference string) (string, error) {
	if err := sameProject(scope.Project, reference); err != nil {
		return "", err
	}
	subnetRegion, name := subnetReference(reference)
	var found bool
	_ = p.scopes.With(scope, func(s *scopeState) error { found = s.subnetworks[regionalKey(region, name)] != nil; return nil })
	if (subnetRegion != "" && subnetRegion != region) || !found {
		return "", missingReference()
	}
	return name, nil
}

func (p *Provider) getRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	region, name := request.Path["region"], request.Path["router"]
	return getRecord(p, scope, regionalRef(scope.Project, region, "routers", name), regionalKey(region, name),
		func(s *scopeState) map[string]*routerRecord { return s.routers }, routerDocument)
}

// listRouters returns the region's routers.
func (p *Provider) listRouters(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	region := request.Path["region"]
	return listRecords(p, request, scope, "compute#routerList", func(s *scopeState) map[string]*routerRecord { return s.routers },
		func(record *routerRecord) bool { return record.Region == region }, inProject(scope, routerDocument))
}

// deleteRouter removes one router of the request's region.
func (p *Provider) deleteRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name, region := request.Path["router"], request.Path["region"]
	found, _, err := p.removeUnlessUsed(scope, regionalName(scope.Project, region, collectionRouters, name),
		func(s *scopeState) bool { return s.routers[regionalKey(region, name)] != nil },
		func(*scopeState) string { return "" },
		func(s *scopeState) { delete(s.routers, regionalKey(region, name)) })
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, notFound(regionalRef(scope.Project, region, "routers", name))
	}
	return p.newOperation(scope, "region", region, "delete", regionalSelfLink(scope.Project, region, "routers", name))
}

// subnetworkIam reads the subnetwork's policy at the requested version or
// writes it, under the scope lock that guards the subnetwork's existence.
func (p *Provider) subnetworkIam(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	region, name := request.Path["region"], request.Path["subnetwork"]
	target := regionalName(scope.Project, region, collectionSubnetworks, name)
	var policy iam.Policy
	var write *iam.Policy
	var version int
	if call.Method == methodSubnetworksGetIam {
		requested, err := provider.RequestedPolicyVersion(request, "optionsRequestedPolicyVersion", gap)
		if err != nil {
			return nil, err //nolint:wrapcheck // wire statuses
		}
		version = requested
	} else {
		decoded, err := decodeRegionPolicy(request.JSON)
		if err != nil {
			return nil, err
		}
		write, version = decoded, 3
	}
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.subnetworks[regionalKey(region, name)] == nil {
			return notFound(regionalRef(scope.Project, region, "subnetworks", name))
		}
		var policyErr error
		if write != nil {
			policy, policyErr = p.engine.SetPolicy(target, *write)
		} else {
			policy, policyErr = p.engine.GetPolicy(target)
		}
		return policyErr //nolint:wrapcheck // the engine's statuses
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // wire statuses
	}
	return &gateway.RESTResponse{JSON: policy.View(version).Wire()}, nil
}

// decodeRegionPolicy decodes a RegionSetPolicyRequest. Its deprecated
// top-level bindings and etag, which flatten the policy, are a gap.
func decodeRegionPolicy(raw []byte) (*iam.Policy, error) {
	var body struct {
		Policy   *iam.Policy     `json:"policy"`
		Bindings json.RawMessage `json:"bindings"`
		ETag     string          `json:"etag"`
	}
	if json.Unmarshal(raw, &body) != nil {
		return nil, badRequest("Invalid JSON payload received.")
	}
	if (len(body.Bindings) > 0 && string(body.Bindings) != jsonNull) || body.ETag != "" {
		return nil, gap("the deprecated top-level bindings and etag of setIamPolicy are not modeled; send policy")
	}
	if body.Policy == nil {
		return nil, badRequest("Required field 'policy' not specified")
	}
	return body.Policy, nil
}
