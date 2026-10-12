package compute

import (
	"encoding/json"
	"errors"
	"fmt"
	"net"
	"sort"
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
	var created *networkRecord
	err = p.scopes.With(scope, func(s *scopeState) error {
		name := provider.Text(body["name"])
		if s.networks[name] != nil {
			return errAlreadyExists
		}
		routing, _ := body["routingConfig"].(map[string]any)
		created = &networkRecord{
			Name: name, Description: provider.Text(body[keyDescription]),
			AutoCreateSubnetworks: boolOf(body[keyAutoSubnets], true),
			RoutingMode:           textOr(routing["routingMode"], "REGIONAL"),
			MTU:                   intOf(body["mtu"], 0), Generation: s.nextGeneration(), CreatedAt: p.clock(),
		}
		s.networks[name] = created
		return nil
	})
	if err != nil {
		return nil, conflict("network", provider.Text(body["name"]))
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: globalName(project, kindNetwork, created.Name), Service: serviceName,
		Kind: kindNetwork, Parent: provider.ProjectResource(project), Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the network")
	}
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "networks", created.Name))
}

func (p *Provider) getNetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["network"]
	var record *networkRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.networks[name]; return nil })
	if record == nil {
		return nil, provider.NotFound(kindNetwork, name)
	}
	return &gateway.RESTResponse{JSON: p.renderNetwork(scope, record)}, nil
}

// listNetworks returns the scope's networks.
func (p *Provider) listNetworks(scope state.Scope) (*gateway.RESTResponse, error) {
	var networks []*networkRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.networks {
			networks = append(networks, s.networks[name])
		}
		return nil
	})
	items := make([]any, 0, len(networks))
	for _, record := range networks {
		items = append(items, p.renderNetwork(scope, record))
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

// deleteNetwork removes one network. Compute accepts the delete of a network
// that other resources still use and reports RESOURCE_IN_USE_BY_ANOTHER_RESOURCE
// in the finished operation, keeping the network.
func (p *Provider) deleteNetwork(_ gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["network"]
	target := selfLink(scope.Project, "networks", name)
	found, user := p.removeUnlessUsed(scope,
		func(s *scopeState) bool { return s.networks[name] != nil },
		func(s *scopeState) string { return s.networkUser(name) },
		func(s *scopeState) { delete(s.networks, name) })
	if !found {
		return nil, provider.NotFound(kindNetwork, name)
	}
	if user != "" {
		return p.failedOperation(scope, "global", "", "delete", target, &operationError{
			Code:    "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE",
			Message: "The network resource '" + target + "' is already being used by '" + user + "'",
		})
	}
	_ = p.index.Remove(serviceName, scope, globalName(scope.Project, kindNetwork, name))
	return p.newOperation(scope, "global", "", "delete", target)
}

// removeUnlessUsed deletes one resource under the scope lock unless another
// resource still uses it. It reports whether the resource existed and, when
// the delete was refused, the resource that uses it.
func (p *Provider) removeUnlessUsed(scope state.Scope, exists func(*scopeState) bool,
	user func(*scopeState) string, remove func(*scopeState),
) (bool, string) {
	found, usedBy := false, ""
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if found = exists(s); !found {
			return nil
		}
		if usedBy = user(s); usedBy == "" {
			remove(s)
		}
		return nil
	})
	return found, usedBy
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

// firstUser names the first record, in name order, that uses a resource.
func firstUser[T any](kind string, records map[string]T, uses func(T) bool) string {
	for _, name := range sortedKeys(records) {
		if uses(records[name]) {
			return kind + "/" + name
		}
	}
	return ""
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
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
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
		return p.listSubnetworks(scope)
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
	var created *subnetworkRecord
	var overlap string
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.subnetworks[name] != nil {
			return errAlreadyExists
		}
		if overlap = s.overlappingSubnet(network, cidr); overlap != "" {
			return nil
		}
		created = &subnetworkRecord{
			Name: name, Description: provider.Text(body[keyDescription]), Network: network, Region: region,
			IPCIDRRange: cidr, Gateway: defaultGateway(cidr),
			PrivateAccess: boolOf(body["privateIpGoogleAccess"], false),
			Generation:    1, CreatedAt: p.clock(),
		}
		s.subnetworks[name] = created
		return nil
	})
	if err != nil {
		return nil, conflict("subnetwork", name)
	}
	if overlap != "" {
		return p.failedOperation(scope, "region", region, "insert", target, &operationError{
			Code: "INVALID_USAGE",
			Message: "Invalid IPCidrRange: " + cidr + " conflicts with existing subnetwork '" +
				overlap + "' in region '" + region + "'.",
		})
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: regionalName(project, created.Region, kindSubnetwork, created.Name), Service: serviceName,
		Kind: kindSubnetwork, Parent: regionalName(project, created.Region, kindLocation, created.Region),
		Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the subnetwork")
	}
	return p.newOperation(scope, "region", region, "insert", target)
}

// overlappingSubnet names a subnetwork of the network whose range overlaps cidr.
func (s *scopeState) overlappingSubnet(network, cidr string) string {
	_, candidate, err := net.ParseCIDR(cidr)
	if err != nil {
		return ""
	}
	for _, name := range sortedKeys(s.subnetworks) {
		subnet := s.subnetworks[name]
		_, existing, parseErr := net.ParseCIDR(subnet.IPCIDRRange)
		if subnet.Network != network || parseErr != nil {
			continue
		}
		if existing.Contains(candidate.IP) || candidate.Contains(existing.IP) {
			return name
		}
	}
	return ""
}

func (p *Provider) getSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["subnetwork"]
	var record *subnetworkRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.subnetworks[name]; return nil })
	if record == nil {
		return nil, provider.NotFound(kindSubnetwork, name)
	}
	return &gateway.RESTResponse{JSON: subnetworkDocument(scope.Project, record)}, nil
}

// listSubnetworks returns the scope's subnetworks.
func (p *Provider) listSubnetworks(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.subnetworks {
			items = append(items, subnetworkDocument(scope.Project, s.subnetworks[name]))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

// deleteSubnetwork removes one subnetwork. Like a network, a subnetwork that
// an instance still uses is refused in the finished operation and kept.
func (p *Provider) deleteSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name, region := request.Path["subnetwork"], request.Path["region"]
	target := regionalSelfLink(scope.Project, region, "subnetworks", name)
	found, user := p.removeUnlessUsed(scope,
		func(s *scopeState) bool { return s.subnetworks[name] != nil },
		func(s *scopeState) string {
			return s.instanceUser(func(r *instanceRecord) bool { return r.Subnetwork == name })
		},
		func(s *scopeState) { delete(s.subnetworks, name) })
	if !found {
		return nil, provider.NotFound(kindSubnetwork, name)
	}
	if user != "" {
		return p.failedOperation(scope, "region", region, "delete", target, &operationError{
			Code:    "RESOURCE_IN_USE_BY_ANOTHER_RESOURCE",
			Message: "The subnetwork resource '" + target + "' is already being used by '" + user + "'",
		})
	}
	_ = p.index.Remove(serviceName, scope, regionalName(scope.Project, region, kindSubnetwork, name))
	return p.newOperation(scope, "region", region, "delete", target)
}

// removeRegionResource deletes one regional resource, unregisters it, and
// returns the delete operation. The closures are per kind because the scope
// state maps differ.
func (p *Provider) removeRegionResource(request *gateway.RESTRequest, scope state.Scope, plural, kind string,
	remove func(*scopeState, string),
	exists func(*scopeState, string) bool,
) (*gateway.RESTResponse, error) {
	project, name, region := scope.Project, request.Path[requestPath(plural)], request.Path["region"]
	found := false
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if found = exists(s, name); found {
			remove(s, name)
		}
		return nil
	})
	if !found {
		return nil, provider.NotFound(kind, name)
	}
	_ = p.index.Remove(serviceName, scope, regionalName(project, region, kind, name))
	return p.newOperation(scope, "region", region, "delete", regionalSelfLink(scope.Project, region, plural, name))
}

// requestPath maps a resource plural to its path segment name.
func requestPath(plural string) string {
	switch plural {
	case "routers":
		return "router"
	case "subnetworks":
		return "subnetwork"
	default:
		return strings.TrimSuffix(plural, "s")
	}
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
	project := scope.Project
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
		return nil, conflict("firewall", name)
	}
	_ = p.index.Put(serviceName, state.Resource{
		Name: globalName(project, kindFirewall, name), Service: serviceName,
		Kind: kindFirewall, Parent: provider.ProjectResource(project), Scope: scope,
	})
	operationType := "insert"
	if call.Method == methodFirewallsUpdate {
		operationType = "update"
	}
	return p.newOperation(scope, "global", "", operationType, selfLink(scope.Project, "firewalls", name))
}

// storeFirewall validates and stores one firewall record under the scope lock.
func (p *Provider) storeFirewall(scope state.Scope, body map[string]any, network, name string, create bool) error {
	err := p.scopes.With(scope, func(s *scopeState) error {
		if create && s.firewalls[name] != nil {
			return errAlreadyExists
		}
		if !create && s.firewalls[name] == nil {
			return errMissing
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
	if err != nil {
		return fmt.Errorf("store the firewall: %w", err)
	}
	return nil
}

func (p *Provider) getFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["firewall"]
	var record *firewallRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.firewalls[name]; return nil })
	if record == nil {
		return nil, provider.NotFound(kindFirewall, name)
	}
	return &gateway.RESTResponse{JSON: firewallDocument(scope.Project, record)}, nil
}

// listFirewalls returns the scope's firewall rules.
func (p *Provider) listFirewalls(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.firewalls {
			items = append(items, firewallDocument(scope.Project, s.firewalls[name]))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

// deleteFirewall removes one firewall rule.
func (p *Provider) deleteFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["firewall"]
	remove := func(s *scopeState) bool {
		found := s.firewalls[name] != nil
		delete(s.firewalls, name)
		return found
	}
	return p.deleteGlobal(scope, kindFirewall, name, remove)
}

// deleteGlobal removes one global resource, unindexes it, and reports its
// operation; the remove closure runs under the scope lock and reports whether
// the resource existed.
func (p *Provider) deleteGlobal(scope state.Scope, kind, name string, remove func(*scopeState) bool) (*gateway.RESTResponse, error) {
	project := scope.Project
	found := false
	_ = p.scopes.With(scope, func(s *scopeState) error {
		found = remove(s)
		return nil
	})
	if !found {
		return nil, provider.NotFound(kind, name)
	}
	_ = p.index.Remove(serviceName, scope, globalName(project, kind, name))
	return p.newOperation(scope, "global", "", "delete", selfLink(scope.Project, kind+"s", name))
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
	var created *routeRecord
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.routes[name] != nil {
			return errAlreadyExists
		}
		created = &routeRecord{
			Name: name, Network: network, DestRange: cidr,
			NextHopGateway: provider.Text(body["nextHopGateway"]),
			NextHopIP:      provider.Text(body["nextHopIp"]),
			Priority:       intOf(body[keyPriority], 1000), Tags: stringListOf(body["tags"]),
			Generation: 1, CreatedAt: p.clock(),
		}
		s.routes[name] = created
		return nil
	})
	if err != nil {
		return nil, conflict("route", name)
	}
	_ = p.index.Put(serviceName, state.Resource{
		Name: globalName(project, kindRoute, name), Service: serviceName,
		Kind: kindRoute, Parent: provider.ProjectResource(project), Scope: scope,
	})
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "routes", name))
}

func (p *Provider) getRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["route"]
	var record *routeRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.routes[name]; return nil })
	if record == nil {
		return nil, provider.NotFound(kindRoute, name)
	}
	return &gateway.RESTResponse{JSON: routeDocument(scope.Project, record)}, nil
}

// listRoutes returns the scope's routes.
func (p *Provider) listRoutes(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.routes {
			items = append(items, routeDocument(scope.Project, s.routes[name]))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

func (p *Provider) deleteRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["route"]
	remove := func(s *scopeState) bool {
		found := s.routes[name] != nil
		delete(s.routes, name)
		return found
	}
	return p.deleteGlobal(scope, kindRoute, name, remove)
}

func (p *Provider) executeRouters(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodRoutersCreate:
		return p.createRouter(request, scope)
	case methodRoutersGet:
		return p.getRouter(request, scope)
	case methodRoutersList:
		return p.listRouters(scope)
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
	nats, natErr := p.parseNATs(body["nats"], scope)
	if natErr != nil {
		return nil, natErr
	}
	var created *routerRecord
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.routers[name] != nil {
			return errAlreadyExists
		}
		created = &routerRecord{
			Name: name, Region: request.Path["region"], Network: network,
			NATs: nats, Generation: 1, CreatedAt: p.clock(),
		}
		s.routers[name] = created
		return nil
	})
	if err != nil {
		return nil, conflict("router", name)
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: regionalName(scope.Project, created.Region, kindRouter, created.Name), Service: serviceName,
		Kind: kindRouter, Parent: regionalName(scope.Project, created.Region, kindLocation, created.Region),
		Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the router")
	}
	return p.newOperation(scope, "region", created.Region, "insert", regionalSelfLink(scope.Project, created.Region, "routers", created.Name))
}

// allSubnetworks is Compute's NAT source selector for every subnetwork.
const allSubnetworks = "ALL_SUBNETWORKS_ALL_IP_RANGES"

// parseNATs validates the NAT gateways of one router body.
func (p *Provider) parseNATs(raw any, scope state.Scope) ([]natRecord, error) {
	items, ok := raw.([]any)
	if !ok || len(items) == 0 {
		return nil, provider.Invalid("A router requires at least one NAT gateway")
	}
	nats := make([]natRecord, 0, len(items))
	for _, item := range items {
		nat, err := p.parseNAT(item, scope)
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
func (p *Provider) parseNAT(raw any, scope state.Scope) (natRecord, error) {
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
		subnet := subnetName(provider.Text(entry["name"]))
		if !p.subnetworkExists(scope, subnet) {
			return natRecord{}, missingReference()
		}
		record.Subnetworks = append(record.Subnetworks, subnet)
	}
	if (record.Ranges == listOfSubnetworks) != (len(record.Subnetworks) > 0) {
		return natRecord{}, provider.Invalid("Only a LIST_OF_SUBNETWORKS NAT gateway names subnetworks")
	}
	return record, nil
}

// subnetName strips a subnetwork self-link or name into the plain name.
func subnetName(value string) string {
	if index := strings.LastIndex(value, "/subnetworks/"); index >= 0 {
		return value[index+len("/subnetworks/"):]
	}
	return value
}

// subnetworkExists reports whether the named subnetwork exists in the scope.
func (p *Provider) subnetworkExists(scope state.Scope, name string) bool {
	var found bool
	_ = p.scopes.With(scope, func(s *scopeState) error { found = s.subnetworks[name] != nil; return nil })
	return found
}

func (p *Provider) getRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["router"]
	var record *routerRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.routers[name]; return nil })
	if record == nil {
		return nil, provider.NotFound(kindRouter, name)
	}
	return &gateway.RESTResponse{JSON: routerDocument(scope.Project, record)}, nil
}

// listRouters returns the scope's routers.
func (p *Provider) listRouters(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for name := range s.routers {
			items = append(items, routerDocument(scope.Project, s.routers[name]))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

func (p *Provider) deleteRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.removeRegionResource(request, scope, "routers", kindRouter,
		func(s *scopeState, name string) { delete(s.routers, name) },
		func(s *scopeState, name string) bool { return s.routers[name] != nil },
	)
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
