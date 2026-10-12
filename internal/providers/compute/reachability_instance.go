package compute

import (
	"net"
)

// Connectivity Tests between VM instances follow the steps recorded against
// GCP in tests/conformance/recordings/vm-connectivity-nat-peering: a flow
// starts from the source instance, applies the source network's egress
// firewall rules, then its routes. A destination instance in the same network,
// or in a network peered in both directions with subnet routes exchanged, is
// reached through the subnet route and then the destination network's ingress
// rules decide (the implied default-deny-ingress drops unmatched flows). A
// destination in an unpeered network follows the default route and drops as
// PRIVATE_TRAFFIC_TO_INTERNET. An instance without an external address reaches
// the recorded internet destination only through Cloud NAT covering all
// subnetworks of its network and region; without NAT it drops as
// PRIVATE_GOOGLE_ACCESS_DISALLOWED. Every other variation is a coverage gap.

// Instance step states.
const (
	stepStartInstance  = "START_FROM_INSTANCE"
	stepArriveInstance = "ARRIVE_AT_INSTANCE"
)

// recordedInternetDestinations are the internet addresses the VM recording
// analyzed. Other public destinations may report other drop causes in GCP.
var recordedInternetDestinations = []string{"8.8.8.8"}

// natCoverage is how the Cloud NAT gateways of a network and region cover an
// instance's subnetwork.
type natCoverage int

const (
	natAbsent natCoverage = iota
	natAllSubnetworks
	natUnmodeled
)

// analyzeInstances evaluates a flow with at least one instance endpoint.
func (s *scopeState) analyzeInstances(spec flow) analysis {
	source, dest, resolved, reason := s.resolveInstances(spec)
	if reason != "" {
		return analysis{gap: reason}
	}
	outcome := s.instanceFlow(resolved, source, dest)
	outcome.network, outcome.sourceIP, outcome.destIP = source.Network, resolved.sourceIP, resolved.destIP
	return outcome
}

// resolveInstances finds the endpoint instances and resolves their addresses.
// The source must be an instance; the destination is an instance or an
// address no instance owns.
func (s *scopeState) resolveInstances(spec flow) (*instanceRecord, *instanceRecord, flow, string) {
	source := s.instances[spec.sourceInstance]
	if source == nil {
		return nil, nil, spec, "Instance flows are modeled only from an existing source instance"
	}
	if spec.sourceNetwork != "" && spec.sourceNetwork != source.Network {
		return nil, nil, spec, "The source network does not match the source instance"
	}
	spec.sourceIP = net.ParseIP(source.IP)
	if spec.destInstance == "" {
		if s.instanceWithIP(spec.destIP) != nil {
			return nil, nil, spec, "An IP-address endpoint that belongs to a modeled instance is not recorded; name the instance"
		}
		return source, nil, spec, ""
	}
	dest := s.instances[spec.destInstance]
	if dest == nil {
		return nil, nil, spec, "The destination instance is not a modeled instance"
	}
	spec.destIP = net.ParseIP(dest.IP)
	return source, dest, spec, ""
}

// instanceWithIP returns the instance whose internal address is ip, if any.
func (s *scopeState) instanceWithIP(ip net.IP) *instanceRecord {
	for _, key := range sortedKeys(s.instances) {
		if record := s.instances[key]; ip != nil && ip.Equal(net.ParseIP(record.IP)) {
			return record
		}
	}
	return nil
}

// instanceFlow applies the source instance's egress rules and routes.
func (s *scopeState) instanceFlow(spec flow, source, dest *instanceRecord) analysis {
	if s.taggedRuleMatches(source.Network, directionEgress, spec) {
		return analysis{gap: "Firewall rules with source or target tags are not modeled for instance endpoints"}
	}
	rule := s.egressRule(source.Network, spec)
	steps := []map[string]any{
		instanceStep(spec.project, stepStartInstance, source),
		firewallStep(spec.project, source.Network, directionEgress, rule),
	}
	if rule != nil && len(rule.Denied) > 0 {
		return dropped(spec, source.Network, steps, "FIREWALL_RULE", "Egress firewall rule "+rule.Name+" denies the flow")
	}
	if route := s.customRoute(source.Network, spec.destIP); route != "" {
		return analysis{gap: "Static route " + route + " matches the destination; route next hops are not modeled"}
	}
	if dest == nil {
		return s.toInternet(spec, source, steps)
	}
	return s.toInstance(spec, source, dest, steps)
}

// toInternet analyzes the recorded internet egress of an instance without an
// external address: Cloud NAT delivers it, and without NAT it drops.
func (s *scopeState) toInternet(spec flow, source *instanceRecord, steps []map[string]any) analysis {
	if !recordedInternet(spec.destIP) {
		return analysis{gap: "Instance flows to addresses other than the recorded internet destination 8.8.8.8 are not modeled"}
	}
	if subnet := s.subnetworks[source.Subnetwork]; subnet == nil || subnet.PrivateAccess {
		return analysis{gap: "Internet egress from a subnetwork with Private Google Access is not recorded"}
	}
	steps = append(steps, defaultRouteStep(spec.project, source.Network))
	switch s.natCoverage(source) {
	case natAllSubnetworks:
		steps = append(steps, deliverStep(spec.project, "INTERNET", map[string]any{keyIPAddress: spec.destIP.String()}))
		return analysis{result: resultReachable, steps: steps, reason: "Cloud NAT translates the instance's internet egress"}
	case natAbsent:
		return dropped(spec, source.Network, steps, "PRIVATE_GOOGLE_ACCESS_DISALLOWED",
			"The instance has no external address and no Cloud NAT covers its subnetwork")
	default:
		return analysis{gap: "Cloud NAT gateways other than ALL_SUBNETWORKS_ALL_IP_RANGES are not modeled"}
	}
}

// recordedInternet reports whether ip is a recorded internet destination.
func recordedInternet(ip net.IP) bool {
	for _, address := range recordedInternetDestinations {
		if ip.Equal(net.ParseIP(address)) {
			return true
		}
	}
	return false
}

// natCoverage reports how the Cloud NAT gateways of the instance's network
// and region cover it. Only ALL_SUBNETWORKS_ALL_IP_RANGES is recorded.
func (s *scopeState) natCoverage(source *instanceRecord) natCoverage {
	coverage := natAbsent
	for _, name := range sortedKeys(s.routers) {
		router := s.routers[name]
		if router.Network != source.Network || router.Region != source.Region {
			continue
		}
		for _, nat := range router.NATs {
			if nat.Ranges == allSubnetworks {
				return natAllSubnetworks
			}
			coverage = natUnmodeled
		}
	}
	return coverage
}

// toInstance routes to the destination instance and applies its network's
// ingress rules.
func (s *scopeState) toInstance(spec flow, source, dest *instanceRecord, steps []map[string]any) analysis {
	route, reason := s.instanceRoute(spec, source, dest)
	if reason != "" {
		return analysis{gap: reason}
	}
	if route == nil {
		steps = append(steps, defaultRouteStep(spec.project, source.Network))
		return dropped(spec, source.Network, steps, "PRIVATE_TRAFFIC_TO_INTERNET",
			"The networks are not peered; the default route sends private traffic to the internet")
	}
	if s.taggedRuleMatches(dest.Network, directionIngress, spec) {
		return analysis{gap: "Firewall rules with source or target tags are not modeled for instance endpoints"}
	}
	rule := s.decidingRule(dest.Network, directionIngress, spec, ingressMatches)
	steps = append(steps, route, instanceStep(spec.project, stepArriveInstance, dest),
		firewallStep(spec.project, dest.Network, directionIngress, rule))
	if rule == nil {
		return dropped(spec, dest.Network, steps, "FIREWALL_RULE", "No ingress rule allows the flow; the implied rule denies it")
	}
	if len(rule.Denied) > 0 {
		return dropped(spec, dest.Network, steps, "FIREWALL_RULE", "Ingress firewall rule "+rule.Name+" denies the flow")
	}
	steps = append(steps, deliverStep(spec.project, "INSTANCE", map[string]any{"resourceUri": instanceURI(spec.project, dest)}))
	return analysis{result: resultReachable, steps: steps, reason: "Ingress firewall rule " + rule.Name + " allows the flow"}
}

// instanceRoute returns the route step toward the destination instance, nil
// for an unpeered network (the default route applies), or a gap reason.
func (s *scopeState) instanceRoute(spec flow, source, dest *instanceRecord) (map[string]any, string) {
	subnet := s.subnetworks[dest.Subnetwork]
	if subnet == nil {
		return nil, "The destination instance's subnetwork is not modeled"
	}
	if source.Network == dest.Network {
		return subnetRouteStep(spec.project, subnet), ""
	}
	if s.subnetContaining(source.Network, spec.destIP) != nil {
		return nil, "Overlapping subnetwork ranges across networks are not modeled"
	}
	forward, back := s.peering(source.Network, dest.Network), s.peering(dest.Network, source.Network)
	switch {
	case forward == nil && back == nil:
		return nil, ""
	case forward == nil || back == nil:
		return nil, "One-sided (inactive) network peering is not recorded"
	case !forward.ExchangeSubnetRoutes || !back.ExchangeSubnetRoutes:
		return nil, "Network peering without subnet route exchange is not recorded"
	}
	return routeStep(spec.project, map[string]any{
		keyDisplayName: subnet.Name, keyRouteType: "PEERING_SUBNET", keyDestIPRange: subnet.IPCIDRRange,
		keyNextHopType: "NEXT_HOP_PEERING", keyNextHop: relativeNetwork(spec.project, dest.Network),
		keyNetworkURI: relativeNetwork(spec.project, source.Network), keyPriority: 0,
	}), ""
}

// peering returns the network's peering toward peer, if any.
func (s *scopeState) peering(network, peer string) *peeringRecord {
	record := s.networks[network]
	if record == nil {
		return nil
	}
	for i := range record.Peerings {
		if record.Peerings[i].PeerNetwork == peer {
			return &record.Peerings[i]
		}
	}
	return nil
}

// taggedRuleMatches reports whether an enabled rule with source or target
// tags could decide the flow; tags and service accounts are not modeled.
func (s *scopeState) taggedRuleMatches(network, direction string, spec flow) bool {
	for _, name := range sortedKeys(s.firewalls) {
		rule := s.firewalls[name]
		if rule.applies(network, direction, true) && (termsMatch(rule.Allowed, spec) || termsMatch(rule.Denied, spec)) {
			return true
		}
	}
	return false
}

// ingressMatches reports whether an INGRESS rule covers the flow's source,
// destination, protocol, and port. A rule without source ranges (and, being
// untagged, without source tags) covers every source, as in GCP.
func ingressMatches(rule *firewallRecord, spec flow) bool {
	if !termsMatch(rule.Allowed, spec) && !termsMatch(rule.Denied, spec) {
		return false
	}
	return rangesCover(rule.SourceRanges, spec.sourceIP) && rangesCover(rule.DestinationRanges, spec.destIP)
}

// rangesCover reports whether any CIDR range contains ip; no ranges cover all.
func rangesCover(ranges []string, ip net.IP) bool {
	if len(ranges) == 0 {
		return true
	}
	for _, cidr := range ranges {
		if cidrContains(cidr, ip) {
			return true
		}
	}
	return false
}

// instanceURI renders the relative instance URI Connectivity Tests uses.
func instanceURI(project string, record *instanceRecord) string {
	return "projects/" + project + "/zones/" + record.Zone + "/instances/" + record.Name
}

// instanceStep is the START_FROM_INSTANCE or ARRIVE_AT_INSTANCE step.
func instanceStep(project, stepState string, record *instanceRecord) map[string]any {
	description := "Initial state: packet originating from a Compute Engine instance."
	if stepState == stepArriveInstance {
		description = "Config checking state: packet arrives at a Compute Engine instance."
	}
	return map[string]any{
		keyState: stepState, keyDescription: description, keyProjectID: project, keyCausesDrop: false,
		kindInstance: map[string]any{
			keyDisplayName: record.Name, keyURI: instanceURI(project, record), "interface": "nic0",
			keyNetworkURI: relativeNetwork(project, record.Network), "internalIp": record.IP,
		},
	}
}

// deliverStep is the terminal DELIVER step toward an instance or the internet.
func deliverStep(project, target string, detail map[string]any) map[string]any {
	detail["target"] = target
	return map[string]any{
		keyState: "DELIVER", keyDescription: "Final state: packet delivered to " + target + ".",
		keyProjectID: project, keyCausesDrop: false, "deliver": detail,
	}
}

// dropped ends a trace with a DROP step and its cause.
func dropped(spec flow, network string, steps []map[string]any, cause, reason string) analysis {
	steps = append(steps, finalStep(spec, network, "DROP", "drop", cause))
	return analysis{result: resultUnreachable, steps: steps, reason: reason}
}
