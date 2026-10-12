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
		return p.listNetworks(scope)
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
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	name := provider.Text(body["name"])
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.networks[name] != nil {
			return errAlreadyExists
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: globalName(project, kindNetwork, name), Service: serviceName,
			Kind: kindNetwork, Parent: provider.ProjectResource(project), Scope: scope,
		}); err != nil {
			return gap("index the network")
		}
		routing, _ := body["routingConfig"].(map[string]any)
		s.networks[name] = &networkRecord{
			Name: name, Description: provider.Text(body[keyDescription]),
			AutoCreateSubnetworks: boolOf(body[keyAutoSubnets], true),
			RoutingMode:           textOr(routing["routingMode"], "REGIONAL"),
			MTU:                   intOf(body["mtu"], 0), Generation: s.nextGeneration(), CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, "network", name)
	}
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "networks", name))
}

// createError maps a create callback's failure onto its status: a duplicate
// is a conflict, and anything else (a missing reference, an index refusal) is
// already a wire status.
func createError(err error, kind, name string) error {
	if errors.Is(err, errAlreadyExists) {
		return conflict(kind, name)
	}
	return err
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
		return nil, provider.NotFound(kindNetwork, name)
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// listRecords renders one collection in name order, as Compute lists it.
// keep selects the request's region or zone for a regional or zonal kind;
// nil keeps every record.
func listRecords[T any](p *Provider, scope state.Scope, listKind string, records func(*scopeState) map[string]*T,
	keep func(*T) bool, render func(*scopeState, *T) map[string]any,
) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		values := records(s)
		for _, key := range sortedKeys(values) {
			if record := values[key]; keep == nil || keep(record) {
				items = append(items, render(s, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items, keyKind: listKind}}, nil
}

// listNetworks returns the scope's networks.
func (p *Provider) listNetworks(scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, scope, "compute#networkList", func(s *scopeState) map[string]*networkRecord { return s.networks }, nil,
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
	found, user, err := p.removeUnlessUsed(scope, globalName(scope.Project, kindNetwork, name),
		func(s *scopeState) bool { return s.networks[name] != nil },
		func(s *scopeState) string { return s.networkUser(name) },
		func(s *scopeState) { delete(s.networks, name) })
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, provider.NotFound(kindNetwork, name)
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
		return p.listSubnetworks(request.Path["region"], scope)
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
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	cidr, cidrErr := parseCidr(body, "ipCidrRange")
	if cidrErr != nil {
		return nil, cidrErr
	}
	network := networkName(provider.Text(body["network"]))
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	name := provider.Text(body["name"])
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
			Name: regionalName(project, region, kindSubnetwork, name), Service: serviceName,
			Kind: kindSubnetwork, Parent: regionalName(project, region, kindLocation, region),
			Scope: scope,
		}); err != nil {
			return gap("index the subnetwork")
		}
		s.subnetworks[regionalKey(region, name)] = &subnetworkRecord{
			Name: name, Description: provider.Text(body[keyDescription]), Network: network, Region: region,
			IPCIDRRange: cidr, Gateway: defaultGateway(cidr),
			PrivateAccess: boolOf(body["privateIpGoogleAccess"], false),
			Generation:    1, CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, "subnetwork", name)
	}
	if overlap != "" {
		return p.failedOperation(scope, "region", region, "insert", target, &operationError{
			Code:    "INVALID_USAGE",
			Message: "Invalid IPCidrRange: " + cidr + " conflicts with existing subnetwork " + overlap + ".",
		})
	}
	return p.newOperation(scope, "region", region, "insert", target)
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
	return getRecord(p, scope, kindSubnetwork, regionalKey(request.Path["region"], request.Path["subnetwork"]),
		func(s *scopeState) map[string]*subnetworkRecord { return s.subnetworks }, subnetworkDocument)
}

// listSubnetworks returns the region's subnetworks.
func (p *Provider) listSubnetworks(region string, scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, scope, "compute#subnetworkList", func(s *scopeState) map[string]*subnetworkRecord { return s.subnetworks },
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
	found, user, err := p.removeUnlessUsed(scope, regionalName(scope.Project, region, kindSubnetwork, name),
		func(s *scopeState) bool { return s.subnetworks[regionalKey(region, name)] != nil },
		func(s *scopeState) string {
			return s.instanceUser(func(r *instanceRecord) bool { return r.Region == region && r.Subnetwork == name })
		},
		func(s *scopeState) { delete(s.subnetworks, regionalKey(region, name)) })
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, provider.NotFound(kindSubnetwork, name)
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

// networkExists reports whether the named network exists in the scope.
func (p *Provider) networkExists(scope state.Scope, name string) bool {
	var found bool
	_ = p.scopes.With(scope, func(s *scopeState) error { found = s.networks[name] != nil; return nil })
	return found
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
		return p.listFirewalls(scope)
	default:
		return p.deleteFirewall(request, scope)
	}
}

// writeFirewall creates or replaces one firewall rule.
func (p *Provider) writeFirewall(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	network := networkName(provider.Text(body["network"]))
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	name := provider.Text(body["name"])
	if call.Method == methodFirewallsUpdate && name == "" {
		name = request.Path["firewall"]
	}
	err = p.storeFirewall(scope, body, network, name, call.Method == methodFirewallsCreate)
	if err != nil {
		if errors.Is(err, errMissing) {
			return nil, provider.NotFound(kindFirewall, name)
		}
		return nil, createError(err, "firewall", name)
	}
	operationType := "insert"
	if call.Method == methodFirewallsUpdate {
		operationType = "update"
	}
	return p.newOperation(scope, "global", "", operationType, selfLink(scope.Project, "firewalls", name))
}

// storeFirewall stores one firewall record and its index entry under the
// scope lock. Every field is read from the validated body before the record
// changes, so a refused write changes nothing.
func (p *Provider) storeFirewall(scope state.Scope, body map[string]any, network, name string, create bool) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // sentinels and wire statuses
		if s.networks[network] == nil {
			return missingReference()
		}
		if create && s.firewalls[name] != nil {
			return errAlreadyExists
		}
		if !create && s.firewalls[name] == nil {
			return errMissing
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: globalName(scope.Project, kindFirewall, name), Service: serviceName,
			Kind: kindFirewall, Parent: provider.ProjectResource(scope.Project), Scope: scope,
		}); err != nil {
			return gap("index the firewall")
		}
		created := s.firewalls[name]
		if created == nil {
			created = &firewallRecord{Name: name, CreatedAt: p.clock()}
			created.Generation = s.nextGeneration()
			s.firewalls[name] = created
		} else {
			created.Generation++
		}
		created.Network = network
		created.Direction = textOr(body[keyDirection], directionIngress)
		created.Allowed = provider.MapList(body["allowed"])
		created.Denied = provider.MapList(body["denied"])
		created.Description = provider.Text(body[keyDescription])
		created.SourceRanges = stringListOf(body["sourceRanges"])
		created.DestinationRanges = stringListOf(body["destinationRanges"])
		created.TargetTags = stringListOf(body["targetTags"])
		created.SourceTags = stringListOf(body["sourceTags"])
		created.Priority = intOf(body[keyPriority], 1000)
		created.Disabled = boolOf(body["disabled"], false)
		return nil
	})
}

func (p *Provider) getFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return getRecord(p, scope, kindFirewall, request.Path["firewall"], func(s *scopeState) map[string]*firewallRecord { return s.firewalls }, firewallDocument)
}

// listFirewalls returns the scope's firewall rules.
func (p *Provider) listFirewalls(scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, scope, "compute#firewallList", func(s *scopeState) map[string]*firewallRecord { return s.firewalls }, nil,
		inProject(scope, firewallDocument))
}

// deleteFirewall removes one firewall rule.
func (p *Provider) deleteFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return deleteGlobal(p, scope, kindFirewall, request.Path["firewall"], func(s *scopeState) map[string]*firewallRecord { return s.firewalls })
}

// deleteGlobal removes one global resource and its index entry under the
// scope lock and reports its operation; records picks the kind's map.
func deleteGlobal[T any](p *Provider, scope state.Scope, kind, name string, records func(*scopeState) map[string]T) (*gateway.RESTResponse, error) {
	found := false
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if _, found = records(s)[name]; !found {
			return nil
		}
		if err := p.unindex(scope, globalName(scope.Project, kind, name)); err != nil {
			return err
		}
		delete(records(s), name)
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // unindex already wraps
	}
	if !found {
		return nil, provider.NotFound(kind, name)
	}
	return p.newOperation(scope, "global", "", "delete", selfLink(scope.Project, kind+"s", name))
}

// getRecord renders one resource under the scope lock; records picks the
// kind's map.
func getRecord[T any](p *Provider, scope state.Scope, kind, name string, records func(*scopeState) map[string]*T, render func(string, *T) map[string]any) (*gateway.RESTResponse, error) {
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := records(s)[name]; record != nil {
			document = render(scope.Project, record)
		}
		return nil
	})
	if document == nil {
		return nil, provider.NotFound(kind, name)
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
		return p.listRoutes(scope)
	default:
		return p.deleteRoute(request, scope)
	}
}

// createRoute validates and stores one route.
func (p *Provider) createRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	project := scope.Project
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	cidr, cidrErr := parseCidr(body, "destRange")
	if cidrErr != nil {
		return nil, cidrErr
	}
	network := networkName(provider.Text(body["network"]))
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	name := provider.Text(body["name"])
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.networks[network] == nil {
			return missingReference()
		}
		if s.routes[name] != nil {
			return errAlreadyExists
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: globalName(project, kindRoute, name), Service: serviceName,
			Kind: kindRoute, Parent: provider.ProjectResource(project), Scope: scope,
		}); err != nil {
			return gap("index the route")
		}
		s.routes[name] = &routeRecord{
			Name: name, Network: network, DestRange: cidr,
			NextHopGateway: provider.Text(body["nextHopGateway"]),
			NextHopIP:      provider.Text(body["nextHopIp"]),
			Priority:       intOf(body[keyPriority], 1000), Tags: stringListOf(body["tags"]),
			Generation: 1, CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, "route", name)
	}
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "routes", name))
}

func (p *Provider) getRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return getRecord(p, scope, kindRoute, request.Path["route"], func(s *scopeState) map[string]*routeRecord { return s.routes }, routeDocument)
}

// listRoutes returns the scope's routes.
func (p *Provider) listRoutes(scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, scope, "compute#routeList", func(s *scopeState) map[string]*routeRecord { return s.routes }, nil,
		inProject(scope, routeDocument))
}

func (p *Provider) deleteRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return deleteGlobal(p, scope, kindRoute, request.Path["route"], func(s *scopeState) map[string]*routeRecord { return s.routes })
}

func (p *Provider) executeRouters(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodRoutersCreate:
		return p.createRouter(request, scope)
	case methodRoutersGet:
		return p.getRouter(request, scope)
	case methodRoutersList:
		return p.listRouters(request.Path["region"], scope)
	default:
		return p.deleteRouter(request, scope)
	}
}

// createRouter validates and stores one Cloud Router with NAT gateways.
func (p *Provider) createRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	name := provider.Text(body["name"])
	network := networkName(provider.Text(body["network"]))
	if name == "" || network == "" {
		return nil, provider.Invalid("A router requires a name and a network")
	}
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	region := request.Path["region"]
	nats, natErr := p.parseNATs(body["nats"], region, scope)
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
			Name: regionalName(scope.Project, region, kindRouter, name), Service: serviceName,
			Kind: kindRouter, Parent: regionalName(scope.Project, region, kindLocation, region),
			Scope: scope,
		}); err != nil {
			return gap("index the router")
		}
		s.routers[regionalKey(region, name)] = &routerRecord{
			Name: name, Region: region, Network: network,
			NATs: nats, Generation: 1, CreatedAt: p.clock(),
		}
		return nil
	})
	if err != nil {
		return nil, createError(err, "router", name)
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
func (p *Provider) parseNATs(raw any, region string, scope state.Scope) ([]natRecord, error) {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
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
func (p *Provider) parseNAT(raw any, region string, scope state.Scope) (natRecord, error) {
	gateway, ok := raw.(map[string]any)
	if !ok {
		return natRecord{}, provider.Invalid("Invalid NAT gateway entry")
	}
	record := natRecord{
		Name: provider.Text(gateway["name"]), Allocation: textOr(gateway["natIpAllocateOption"], natAutoOnly),
		Ranges: provider.Text(gateway["sourceSubnetworkIpRangesToNat"]),
	}
	if !uidPattern.MatchString(record.Name) || record.Ranges == "" {
		return natRecord{}, provider.Invalid("A NAT gateway requires a name and sourceSubnetworkIpRangesToNat")
	}
	if record.Allocation != natAutoOnly {
		return natRecord{}, gap("NAT gateways with manually allocated addresses are not modeled")
	}
	for _, entry := range provider.MapList(gateway["subnetworks"]) {
		subnetRegion, subnet := subnetReference(provider.Text(entry["name"]))
		if (subnetRegion != "" && subnetRegion != region) || !p.subnetworkExists(scope, region, subnet) {
			return natRecord{}, missingReference()
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

// subnetworkExists reports whether the region's named subnetwork exists.
func (p *Provider) subnetworkExists(scope state.Scope, region, name string) bool {
	var found bool
	_ = p.scopes.With(scope, func(s *scopeState) error { found = s.subnetworks[regionalKey(region, name)] != nil; return nil })
	return found
}

func (p *Provider) getRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return getRecord(p, scope, kindRouter, regionalKey(request.Path["region"], request.Path["router"]),
		func(s *scopeState) map[string]*routerRecord { return s.routers }, routerDocument)
}

// listRouters returns the region's routers.
func (p *Provider) listRouters(region string, scope state.Scope) (*gateway.RESTResponse, error) {
	return listRecords(p, scope, "compute#routerList", func(s *scopeState) map[string]*routerRecord { return s.routers },
		func(record *routerRecord) bool { return record.Region == region }, inProject(scope, routerDocument))
}

// deleteRouter removes one router of the request's region.
func (p *Provider) deleteRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name, region := request.Path["router"], request.Path["region"]
	found, _, err := p.removeUnlessUsed(scope, regionalName(scope.Project, region, kindRouter, name),
		func(s *scopeState) bool { return s.routers[regionalKey(region, name)] != nil },
		func(*scopeState) string { return "" },
		func(s *scopeState) { delete(s.routers, regionalKey(region, name)) })
	if err != nil {
		return nil, err
	}
	if !found {
		return nil, provider.NotFound(kindRouter, name)
	}
	return p.newOperation(scope, "region", region, "delete", regionalSelfLink(scope.Project, region, "routers", name))
}

// subnetworkIam reads or writes the subnetwork's stored IAM policy.
func (p *Provider) subnetworkIam(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	target := subnetworkIamResource(scope.Project, request.Path["region"], request.Path["subnetwork"])
	if call.Method == methodSubnetworksGetIam {
		policy, err := p.engine.GetPolicy(target)
		if err != nil {
			return nil, status.Errorf(codes.Internal, "read the subnetwork policy: %v", err)
		}
		return &gateway.RESTResponse{JSON: policy}, nil
	}
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target, envelope.Policy)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "write the subnetwork policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}

func subnetworkIamResource(project, region, subnetwork string) string {
	return "//compute.googleapis.com/projects/" + project + "/regions/" + region + "/subnetworks/" + subnetwork
}
