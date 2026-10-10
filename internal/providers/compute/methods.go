package compute

import (
	"errors"
	"fmt"
	"net"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// executeNetwork handles the network methods.
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

// addPeering stores one peering of a network toward a peer network.
func (p *Provider) addPeering(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	name := textOf(body["name"])
	peer := networkName(textOf(body["peerNetwork"]))
	if name == "" || peer == "" {
		return nil, statusInvalid("A peering requires a name and a peerNetwork")
	}
	if !p.networkExists(scope, peer) {
		return nil, missingReference()
	}
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.networks[request.Path["network"]]
		if record == nil {
			return errMissing
		}
		for _, existing := range record.Peerings {
			if existing.Name == name {
				return errAlreadyExists
			}
		}
		record.Peerings = append(record.Peerings, peeringRecord{
			Name: name, PeerNetwork: peer,
			ImportCustomRoutes: boolOf(body["importCustomRoutes"], false),
			ExportCustomRoutes: boolOf(body["exportCustomRoutes"], false),
		})
		return nil
	}); err != nil {
		if errors.Is(err, errAlreadyExists) {
			return nil, conflict("peering", name)
		}
		return nil, notFound(kindNetwork, request.Path["network"])
	}
	return p.newOperation(scope, "global", "", "addPeering", selfLink(scope.Project, "networks", request.Path["network"]))
}

// removePeering deletes one named peering of a network.
func (p *Provider) removePeering(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	name := textOf(body["peering"])
	removed := false
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.networks[request.Path["network"]]
		if record == nil {
			return errMissing
		}
		for i, existing := range record.Peerings {
			if existing.Name == name {
				record.Peerings = append(record.Peerings[:i], record.Peerings[i+1:]...)
				removed = true
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, notFound(kindNetwork, request.Path["network"])
	}
	if !removed {
		return nil, notFound("peerings", name)
	}
	return p.newOperation(scope, "global", "", "removePeering", selfLink(scope.Project, "networks", request.Path["network"]))
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
		name := textOf(body["name"])
		if s.networks[name] != nil {
			return errAlreadyExists
		}
		created = &networkRecord{
			Name: name, AutoCreateSubnetworks: boolOf(body["autoCreateSubnetworks"], true),
			MTU: intOf(body["mtu"], 1460), Generation: s.nextGeneration(), CreatedAt: p.clock(),
		}
		s.networks[name] = created
		return nil
	})
	if err != nil {
		return nil, conflict("network", textOf(body["name"]))
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: globalName(project, kindNetwork, created.Name), Service: serviceName,
		Kind: kindNetwork, Parent: projectResource(project), Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the network")
	}
	return p.newOperation(scope, "global", "", "insert", selfLink(scope.Project, "networks", created.Name))
}

// getNetwork returns one network document.
func (p *Provider) getNetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["network"]
	var record *networkRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.networks[name]; return nil })
	if record == nil {
		return nil, notFound(kindNetwork, name)
	}
	return &gateway.RESTResponse{JSON: p.networkDocument(scope.Project, record, p.subnetworksOf(scope.Project, name))}, nil
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
		items = append(items, p.networkDocument(scope.Project, record, p.subnetworksOf(scope.Project, record.Name)))
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items}}, nil
}

// deleteNetwork removes one network, refusing while subnetworks reference it.
func (p *Provider) deleteNetwork(_ gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["network"]
	var errDependency error
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if s.networks[name] == nil {
			return errMissing
		}
		for _, subnet := range s.subnetworks {
			if subnet.Network == name {
				errDependency = dependencyError{
					message: "The network is still referenced by subnetwork " + subnet.Name,
				}
				return nil
			}
		}
		delete(s.networks, name)
		return nil
	})
	if errors.Is(errDependency, errMissing) {
		return nil, notFound(kindNetwork, name)
	}
	if errDependency != nil {
		return nil, status.Error(codes.FailedPrecondition, errDependency.Error())
	}
	_ = p.index.Remove(serviceName, scope, globalName(scope.Project, kindNetwork, name))
	return p.newOperation(scope, "global", "", "delete", selfLink(scope.Project, "networks", name))
}

// Sentinel record states for the shared handlers.
var (
	errMissing       = errors.New("missing")
	errAlreadyExists = errors.New("already exists")
)

// dependencyError reports a resource still referenced by another resource.
type dependencyError struct{ message string }

func (e dependencyError) Error() string { return e.message }

// executeSubnetwork handles the subnetwork methods.
func (p *Provider) executeSubnetwork(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodSubnetworksCreate:
		return p.createSubnetwork(request, scope)
	case methodSubnetworksGet:
		return p.getSubnetwork(request, scope)
	case methodSubnetworksList:
		return p.listSubnetworks(scope)
	default:
		return p.deleteSubnetwork(request, scope)
	}
}

// createSubnetwork validates and stores one subnetwork.
func (p *Provider) createSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	project := scope.Project
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	cidr, cidrErr := parseCidr(body, "ipCidrRange")
	if cidrErr != nil {
		return nil, cidrErr
	}
	network := networkName(textOf(body["network"]))
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	var created *subnetworkRecord
	err = p.scopes.With(scope, func(s *scopeState) error {
		name := textOf(body["name"])
		if s.subnetworks[name] != nil {
			return errAlreadyExists
		}
		created = &subnetworkRecord{
			Name: name, Network: network, Region: request.Path["region"],
			IPCIDRRange: cidr, Gateway: defaultGateway(cidr),
			PrivateAccess: boolOf(body["privateIpGoogleAccess"], false),
			Generation:    1, CreatedAt: p.clock(),
		}
		s.subnetworks[name] = created
		return nil
	})
	if err != nil {
		return nil, conflict("subnetwork", textOf(body["name"]))
	}

	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: regionalName(project, created.Region, kindSubnetwork, created.Name), Service: serviceName,
		Kind: kindSubnetwork, Parent: regionalName(project, created.Region, kindLocation, created.Region),
		Scope: scope,
	}); indexErr != nil {
		return nil, gap("index the subnetwork")
	}
	return p.newOperation(scope, "region", created.Region, "insert", regionalSelfLink(scope.Project, created.Region, "subnetworks", created.Name))
}

// getSubnetwork returns one subnetwork document.
func (p *Provider) getSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["subnetwork"]
	var record *subnetworkRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.subnetworks[name]; return nil })
	if record == nil {
		return nil, notFound(kindSubnetwork, name)
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

// deleteSubnetwork removes one subnetwork.
func (p *Provider) deleteSubnetwork(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.removeRegionResource(request, scope, "subnetworks", kindSubnetwork,
		func(s *scopeState, name string) { delete(s.subnetworks, name) },
		func(s *scopeState, name string) bool { return s.subnetworks[name] != nil },
	)
}

// removeRegionResource deletes one regional resource, unregisters it, and
// returns the delete operation. The closures are per kind because the scope
// state maps differ.
func (p *Provider) removeRegionResource(request *gateway.RESTRequest, scope state.Scope, plural, kind string,
	remove func(*scopeState, string),
	exists func(*scopeState, string) bool,
) (*gateway.RESTResponse, error) {
	project, name, region := scope.Project, request.Path[requestPath(plural)], request.Path["region"]
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if exists(s, name) {
			remove(s, name)
		}
		return nil
	})
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

// executeFirewall handles the firewall methods.
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
	network := networkName(textOf(body["network"]))
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	name := textOf(body["name"])
	if call.Method == methodFirewallsUpdate && name == "" {
		name = request.Path["firewall"]
	}
	err = p.storeFirewall(scope, body, network, name, call.Method == methodFirewallsCreate)
	if err != nil {
		if errors.Is(err, errMissing) {
			return nil, notFound(kindFirewall, name)
		}
		return nil, conflict("firewall", name)
	}
	_ = p.index.Put(serviceName, state.Resource{
		Name: globalName(project, kindFirewall, name), Service: serviceName,
		Kind: kindFirewall, Parent: projectResource(project), Scope: scope,
	})
	return p.newOperation(scope, "global", "", "insert", firewallTarget(scope, kindFirewall, name))
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
		created.Direction = textOr(body["direction"], "INGRESS")
		created.Allowed = listOfMaps(body["allowed"])
		created.Denied = listOfMaps(body["denied"])
		created.SourceRanges = stringListOf(body["sourceRanges"])
		created.TargetTags = stringListOf(body["targetTags"])
		created.Priority = intOf(body["priority"], 1000)
		created.Disabled = boolOf(body["disabled"], false)
		return nil
	})
	if err != nil {
		return fmt.Errorf("store the firewall: %w", err)
	}
	return nil
}

// getFirewall returns one firewall document.
func (p *Provider) getFirewall(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["firewall"]
	var record *firewallRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.firewalls[name]; return nil })
	if record == nil {
		return nil, notFound(kindFirewall, name)
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
	remove := func(s *scopeState) { delete(s.firewalls, name) }
	return p.deleteGlobal(scope, kindFirewall, name, remove)
}

// deleteGlobal removes one global resource, unindexes it, and reports its
// operation; the remove closure runs under the scope lock.
func (p *Provider) deleteGlobal(scope state.Scope, kind, name string, remove func(*scopeState)) (*gateway.RESTResponse, error) {
	project := scope.Project
	_ = p.scopes.With(scope, func(s *scopeState) error {
		remove(s)
		return nil
	})
	_ = p.index.Remove(serviceName, scope, globalName(project, kind, name))
	return p.newOperation(scope, "global", "", "delete", selfLink(scope.Project, "networks", name))
}

// textOr returns the first non-empty text of the field and the fallback.
func textOr(value any, fallback string) string {
	if text := textOf(value); text != "" {
		return text
	}
	return fallback
}

// executeRoute handles the route methods.
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
	network := networkName(textOf(body["network"]))
	if !p.networkExists(scope, network) {
		return nil, missingReference()
	}
	name := textOf(body["name"])
	var created *routeRecord
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.routes[name] != nil {
			return errAlreadyExists
		}
		created = &routeRecord{
			Name: name, Network: network, DestRange: cidr,
			NextHopGateway: textOr(body["nextHopGateway"], selfLink(project, "gateways", "default-internet-gateway")),
			Priority:       intOf(body["priority"], 1000), Tags: stringListOf(body["tags"]),
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
		Kind: kindRoute, Parent: projectResource(project), Scope: scope,
	})
	return p.newOperation(scope, "global", "", "insert", firewallTarget(scope, kindFirewall, name))
}

// getRoute returns one route document.
func (p *Provider) getRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["route"]
	var record *routeRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.routes[name]; return nil })
	if record == nil {
		return nil, notFound(kindRoute, name)
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

// deleteRoute removes one route.
func (p *Provider) deleteRoute(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["route"]
	remove := func(s *scopeState) { delete(s.routes, name) }
	return p.deleteGlobal(scope, kindRoute, name, remove)
}

// firewallTarget selects the collection for a global insert/delete target.
func firewallTarget(scope state.Scope, kind, name string) string {
	if kind == kindFirewall {
		return selfLink(scope.Project, "firewalls", name)
	}
	return selfLink(scope.Project, "routes", name)
}

// executeRouters handles the router methods.
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
	name := textOf(body["name"])
	network := networkName(textOf(body["network"]))
	if name == "" || network == "" {
		return nil, statusInvalid("A router requires a name and a network")
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
		return nil, statusInvalid("A router requires at least one NAT gateway")
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

// parseNAT validates one NAT gateway entry of a router body.
func (p *Provider) parseNAT(raw any, scope state.Scope) (natRecord, error) {
	gateway, ok := raw.(map[string]any)
	if !ok {
		return natRecord{}, statusInvalid("Invalid NAT gateway entry")
	}
	name := textOf(gateway["name"])
	if name == "" {
		return natRecord{}, statusInvalid("A NAT gateway requires a name")
	}
	ranges, ok := gateway["sourceSubnetworkIpRanges"].([]any)
	if !ok || len(ranges) == 0 {
		return natRecord{}, statusInvalid("A NAT gateway requires sourceSubnetworkIpRanges")
	}
	subnets := make([]string, 0, len(ranges))
	for _, value := range ranges {
		subnet := textOf(value)
		if subnet == allSubnetworks {
			subnets = append(subnets, allSubnetworks)
			continue
		}
		subnet = subnetName(subnet)
		if !p.subnetworkExists(scope, subnet) {
			return natRecord{}, missingReference()
		}
		subnets = append(subnets, subnet)
	}
	return natRecord{Name: name, SourceSubnetworks: subnets}, nil
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

// getRouter returns one router document.
func (p *Provider) getRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name := request.Path["router"]
	var record *routerRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.routers[name]; return nil })
	if record == nil {
		return nil, notFound(kindRouter, name)
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

// deleteRouter removes one router.
func (p *Provider) deleteRouter(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	return p.removeRegionResource(request, scope, "routers", kindRouter,
		func(s *scopeState, name string) { delete(s.routers, name) },
		func(s *scopeState, name string) bool { return s.routers[name] != nil },
	)
}
