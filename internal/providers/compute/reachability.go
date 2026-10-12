package compute

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// The analyzer follows the Connectivity Tests steps recorded against GCP for
// IP-address endpoints (tests/conformance/recordings/compute/vpc-firewall-connectivity):
// a flow starts from the source subnet, applies the source network's egress
// firewall rules, then its routes. No VM instances or other endpoints are
// modeled, so a destination inside the network aborts with UNKNOWN_IP rather
// than reporting a firewall verdict, and a private destination reached only
// through the default route drops with PRIVATE_TRAFFIC_TO_INTERNET. Anything
// the recording does not establish (custom-route next hops, external
// destinations, NAT) reports a coverage gap instead of a verdict. Flows that
// name VM instances follow a separate recording; see the instance analysis below.

// Connectivity Tests result values.
const (
	resultReachable    = "REACHABLE"
	resultUnreachable  = "UNREACHABLE"
	resultUndetermined = "UNDETERMINED"
)

// Reachability is the local analyzer's summary of one modeled flow.
type Reachability struct {
	Result string   `json:"result"`
	Reason string   `json:"reason"`
	Trace  []string `json:"trace"`
}

// reachabilityRequest is the flow specification the local analyzer accepts.
type reachabilityRequest struct {
	Project       string `json:"project"`
	SourceIP      string `json:"sourceIp"`
	SourceNetwork string `json:"sourceNetwork"`
	DestIP        string `json:"destIp"`
	Protocol      string `json:"protocol"`
	Port          int    `json:"port"`
}

// flow is one validated flow the analyzer evaluates. An endpoint is either an
// IP address or a zone/name instance key; the analyzer resolves an instance
// endpoint to its internal address.
type flow struct {
	project        string
	sourceIP       net.IP
	sourceNetwork  string // network name; empty resolves it from the source subnet
	sourceInstance string
	destIP         net.IP
	destInstance   string
	protocol       string // lower case, as firewall terms spell it
	port           int
}

// analysis is the analyzer outcome: a Connectivity Tests result with its
// steps, or a coverage gap when the flow needs topology that is not modeled.
type analysis struct {
	result   string
	steps    []map[string]any
	reason   string
	gap      string
	network  string // the source network
	sourceIP net.IP // the resolved endpoint addresses
	destIP   net.IP
}

// newFlow validates the addresses of one flow specification.
func newFlow(project, sourceIP, sourceNetwork, destIP, protocol string, port int) (flow, error) {
	source := net.ParseIP(strings.TrimSpace(sourceIP))
	destination := net.ParseIP(strings.TrimSpace(destIP))
	if source == nil || destination == nil {
		return flow{}, provider.Invalid("Connectivity test endpoints require valid IP addresses")
	}
	return flow{
		project: project, sourceIP: source, sourceNetwork: networkName(sourceNetwork),
		destIP: destination, protocol: strings.ToLower(strings.TrimSpace(protocol)), port: port,
	}, nil
}

// Analyzer returns the local reachability surface. It reads only the modeled
// topology and reports the same analysis as Connectivity Tests: unknown
// endpoints are "unknown", modeled drops are "dropped", and flows that need
// unmodeled topology are "unsupported", never a pass.
func (p *Provider) Analyzer() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var request reachabilityRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{keyError: "Invalid reachability request"})
			return
		}
		spec, err := newFlow(request.Project, request.SourceIP, request.SourceNetwork, request.DestIP, request.Protocol, request.Port)
		if err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{keyError: "Invalid reachability request addresses"})
			return
		}
		writeJSON(w, http.StatusOK, p.analyze(spec).summary())
	})
}

// writeJSON encodes one analyzer response.
func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// The payload is a fixed-shape analyzer response; encoding never fails on it.
	encoded, _ := json.Marshal(value) //nolint:errchkjson // plain analyzer structs
	_, _ = w.Write(encoded)
}

// summary renders the analysis in the local analyzer's verdict vocabulary.
func (a analysis) summary() Reachability {
	if a.gap != "" {
		return Reachability{Result: verdictUnsupported, Reason: a.gap, Trace: []string{}}
	}
	verdict := map[string]string{
		resultReachable: verdictReachable, resultUnreachable: verdictDropped, resultUndetermined: verdictUnknown,
	}[a.result]
	trace := make([]string, 0, len(a.steps))
	for _, step := range a.steps {
		trace = append(trace, stepSummary(step))
	}
	return Reachability{Result: verdict, Reason: a.reason, Trace: trace}
}

// stepSummary names one step and the rule or cause it reports.
func stepSummary(step map[string]any) string {
	var text strings.Builder
	fmt.Fprint(&text, step[keyState])
	for key, field := range map[string]string{"firewall": "displayName", "route": "displayName", "abort": keyCause, "drop": keyCause} {
		if detail, ok := step[key].(map[string]any); ok {
			fmt.Fprint(&text, " ", detail[field])
		}
	}
	return text.String()
}

// Local analyzer verdicts.
const (
	verdictReachable   = "reachable"
	verdictDropped     = "dropped"
	verdictUnknown     = "unknown"
	verdictUnsupported = "unsupported"
)

// analyze evaluates one flow against a consistent view of the topology.
func (p *Provider) analyze(spec flow) analysis {
	var outcome analysis
	_ = p.scopes.With(provider.ProjectScope(spec.project), func(s *scopeState) error {
		outcome = s.analyze(spec)
		return nil
	})
	return outcome
}

// analyze evaluates one flow: instance endpoints follow the VM recording and
// IP-address endpoints the earlier address-only recording.
func (s *scopeState) analyze(spec flow) analysis {
	if spec.sourceInstance != "" || spec.destInstance != "" {
		return s.analyzeInstances(spec)
	}
	if s.instanceWithIP(spec.sourceIP) != nil || s.instanceWithIP(spec.destIP) != nil {
		return analysis{gap: "An IP-address endpoint that belongs to a modeled instance is not recorded; name the instance"}
	}
	outcome := s.analyzeAddresses(spec)
	outcome.sourceIP, outcome.destIP = spec.sourceIP, spec.destIP
	return outcome
}

// analyzeAddresses walks the recorded step order for IP-address endpoints:
// source network, egress firewall, route, then the final state.
func (s *scopeState) analyzeAddresses(spec flow) analysis {
	source := s.sourceSubnet(spec)
	if source == nil {
		return analysis{gap: "The source address is not inside a modeled subnetwork of the source network"}
	}
	network := source.Network
	steps := []map[string]any{startStep(spec.project, source)}
	rule, unmodeled := s.decidingRule(network, directionEgress, spec, endpoints{})
	if unmodeled != "" {
		return analysis{gap: unmodeled}
	}
	steps = append(steps, firewallStep(spec.project, network, directionEgress, rule))
	if rule != nil && len(rule.Denied) > 0 {
		steps = append(steps, finalStep(spec, network, "DROP", "drop", "FIREWALL_RULE"))
		return analysis{result: resultUnreachable, steps: steps, network: network, reason: "Egress firewall rule " + rule.Name + " denies the flow"}
	}
	if subnet := s.subnetContaining(network, spec.destIP); subnet != nil {
		steps = append(steps, subnetRouteStep(spec.project, subnet),
			finalStep(spec, network, "ABORT", "abort", "UNKNOWN_IP"))
		return analysis{result: resultUndetermined, steps: steps, network: network, reason: "No modeled endpoint has the destination address"}
	}
	if route := s.customRoute(network, spec.destIP); route != "" {
		return analysis{gap: "Static route " + route + " matches the destination; route next hops are not modeled"}
	}
	if !spec.destIP.IsPrivate() {
		return analysis{gap: "External destinations (internet egress, NAT) are not modeled"}
	}
	steps = append(steps, defaultRouteStep(spec.project, network),
		finalStep(spec, network, "DROP", "drop", "PRIVATE_TRAFFIC_TO_INTERNET"))
	return analysis{result: resultUnreachable, steps: steps, network: network, reason: "The default route sends private traffic to the internet"}
}

// sourceSubnet finds the subnetwork that contains the source address, within
// the named source network when one is given.
func (s *scopeState) sourceSubnet(spec flow) *subnetworkRecord {
	for _, key := range sortedKeys(s.subnetworks) {
		subnet := s.subnetworks[key]
		if spec.sourceNetwork != "" && subnet.Network != spec.sourceNetwork {
			continue
		}
		if s.networks[subnet.Network] != nil && cidrContains(subnet.IPCIDRRange, spec.sourceIP) {
			return subnet
		}
	}
	return nil
}

// subnetContaining finds the network's subnetwork whose range contains ip.
func (s *scopeState) subnetContaining(network string, ip net.IP) *subnetworkRecord {
	for _, key := range sortedKeys(s.subnetworks) {
		if subnet := s.subnetworks[key]; subnet.Network == network && cidrContains(subnet.IPCIDRRange, ip) {
			return subnet
		}
	}
	return nil
}

// customRoute names an untagged static route of the network covering ip.
func (s *scopeState) customRoute(network string, ip net.IP) string {
	for _, name := range sortedKeys(s.routes) {
		if route := s.routes[name]; route.Network == network && len(route.Tags) == 0 && cidrContains(route.DestRange, ip) {
			return name
		}
	}
	return ""
}

// endpoints are the instances at the ends of a flow; nil is an IP address.
type endpoints struct {
	source, dest *instanceRecord
}

// Reasons a scoped firewall rule can't be evaluated.
const (
	unmodeledScopedTarget = "A firewall rule scoped to network tags or service accounts could decide this flow, " +
		"but scoped rules are modeled only for instance endpoints"
	unmodeledScopedSource = "A firewall rule with source tags or source service accounts could decide this flow " +
		"from another network; scoped sources across networks are not modeled"
)

// decidingRule returns the enabled rule of the network and direction that
// decides the flow: the highest priority matching rule, deny first at equal
// priority, or nil when only the implied rule applies. Following VPC firewall
// rules, a rule's targets are the destination instance of an INGRESS flow and
// the source instance of an EGRESS flow: every instance unless targetTags or
// targetServiceAccounts narrow them. A non-empty reason reports a scoped rule
// the model can't evaluate, so the caller answers unsupported, not a verdict.
func (s *scopeState) decidingRule(network, direction string, spec flow, ends endpoints) (*firewallRecord, string) {
	target := ends.source
	if direction == directionIngress {
		target = ends.dest
	}
	var best *firewallRecord
	for _, name := range sortedKeys(s.firewalls) {
		rule := s.firewalls[name]
		if rule.Network != network || rule.Direction != direction || rule.Disabled {
			continue
		}
		decides, reason := rule.covers(spec, target, ends.source)
		if reason != "" {
			return nil, reason
		}
		if decides && (best == nil || rule.precedes(best)) {
			best = rule
		}
	}
	return best, ""
}

// precedes reports whether the rule outranks other: a lower priority number,
// or a deny against an allow at equal priority.
func (rule *firewallRecord) precedes(other *firewallRecord) bool {
	if rule.Priority != other.Priority {
		return rule.Priority < other.Priority
	}
	return len(rule.Denied) > 0 && len(other.Denied) == 0
}

// covers reports whether an enabled rule of the flow's network and direction
// matches it: terms, destination ranges, targets, and for INGRESS sources. A
// non-empty reason reports a scoped rule the model can't evaluate.
func (rule *firewallRecord) covers(spec flow, target, source *instanceRecord) (bool, string) {
	if (!termsMatch(rule.Allowed, spec) && !termsMatch(rule.Denied, spec)) || !rangesCover(rule.DestinationRanges, spec.destIP) {
		return false, ""
	}
	if rule.scopedTarget() && target == nil {
		return false, unmodeledScopedTarget
	}
	if !rule.targets(target) {
		return false, ""
	}
	if rule.Direction == directionIngress {
		return rule.admitsSource(spec.sourceIP, source)
	}
	return true, ""
}

// scopedTarget reports whether the rule applies only to tagged or
// service-account-scoped instances.
func (rule *firewallRecord) scopedTarget() bool {
	return len(rule.TargetTags) > 0 || len(rule.TargetServiceAccounts) > 0
}

// targets reports whether the rule applies to the instance: every instance of
// the network, or those with one of its target tags, or those running as one
// of its target service accounts. A rule has at most one kind of target.
func (rule *firewallRecord) targets(instance *instanceRecord) bool {
	switch {
	case len(rule.TargetTags) > 0:
		return slices.ContainsFunc(instance.Tags, func(tag string) bool { return slices.Contains(rule.TargetTags, tag) })
	case len(rule.TargetServiceAccounts) > 0:
		return instance.ServiceAccount != "" && slices.Contains(rule.TargetServiceAccounts, instance.ServiceAccount)
	default:
		return true
	}
}

// admitsSource reports whether an INGRESS rule's sources cover the flow. With
// neither ranges, tags, nor service accounts the rule admits every source.
// Source ranges admit by address; source tags and source service accounts
// admit an instance of the rule's own network by its tags or service account,
// and either kind of source suffices. A scoped source from another network is
// reported as unmodeled.
func (rule *firewallRecord) admitsSource(ip net.IP, source *instanceRecord) (bool, string) {
	scoped := len(rule.SourceTags) > 0 || len(rule.SourceServiceAccounts) > 0
	if !scoped {
		return rangesCover(rule.SourceRanges, ip), ""
	}
	if len(rule.SourceRanges) > 0 && rangesCover(rule.SourceRanges, ip) {
		return true, ""
	}
	if source == nil || source.Network != rule.Network {
		return false, unmodeledScopedSource
	}
	if slices.ContainsFunc(source.Tags, func(tag string) bool { return slices.Contains(rule.SourceTags, tag) }) {
		return true, ""
	}
	return source.ServiceAccount != "" && slices.Contains(rule.SourceServiceAccounts, source.ServiceAccount), ""
}

// Firewall rule directions.
const (
	directionIngress = "INGRESS"
	directionEgress  = "EGRESS"
)

// termsMatch reports whether one allowed/denied term list covers the flow.
func termsMatch(terms []firewallTerm, spec flow) bool {
	for _, term := range terms {
		protocol := strings.ToLower(string(term.IPProtocol))
		if protocol != spec.protocol && protocol != "all" {
			continue
		}
		if portMatches(term.Ports, spec.port) {
			return true
		}
	}
	return false
}

// portMatches reports whether the port list covers the port. An empty list
// matches every port (terms without ports are unrestricted).
func portMatches(ports []string, port int) bool {
	if len(ports) == 0 {
		return true
	}
	for _, value := range ports {
		low, high, isRange := strings.Cut(value, "-")
		if !isRange {
			high = low
		}
		lowPort, lowErr := strconv.Atoi(low)
		highPort, highErr := strconv.Atoi(high)
		if lowErr == nil && highErr == nil && port >= lowPort && port <= highPort {
			return true
		}
	}
	return false
}

// cidrContains reports whether the CIDR range contains the address.
func cidrContains(cidr string, ip net.IP) bool {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}
	return network.Contains(ip)
}

// relativeNetwork renders the relative network URI Connectivity Tests uses.
func relativeNetwork(project, network string) string {
	return "projects/" + project + "/global/networks/" + network
}

// startStep is the START_FROM_PRIVATE_NETWORK step for the source subnet.
func startStep(project string, subnet *subnetworkRecord) map[string]any {
	return map[string]any{
		keyState: "START_FROM_PRIVATE_NETWORK", keyDescription: "Initial state: packet originating from a VPC network.",
		keyProjectID: project, keyCausesDrop: false,
		"network": map[string]any{
			keyDisplayName: subnet.Network, keyURI: relativeNetwork(project, subnet.Network),
			"matchedIpRange":   subnet.IPCIDRRange,
			"matchedSubnetUri": "projects/" + project + "/regions/" + subnet.Region + "/subnetworks/" + subnet.Name,
			keyRegion:          subnet.Region,
		},
	}
}

// firewallStep is the APPLY_EGRESS_FIREWALL_RULE or APPLY_INGRESS_FIREWALL_RULE
// step: the deciding rule, or the implied default-allow-egress or
// default-deny-ingress rule when no configured rule matches.
func firewallStep(project, network, direction string, rule *firewallRecord) map[string]any {
	info := map[string]any{
		keyDisplayName: "default-allow-egress", keyDirection: direction, "action": "ALLOW",
		keyPriority: 65535, keyNetworkURI: relativeNetwork(project, network),
		"firewallRuleType": "IMPLIED_VPC_FIREWALL_RULE",
	}
	if direction == directionIngress {
		info[keyDisplayName], info["action"] = "default-deny-ingress", "DENY"
	}
	if rule != nil {
		action := "ALLOW"
		if len(rule.Denied) > 0 {
			action = "DENY"
		}
		info = map[string]any{
			keyDisplayName: rule.Name, keyURI: "projects/" + project + "/global/firewalls/" + rule.Name,
			keyDirection: direction, "action": action, keyPriority: rule.Priority,
			keyNetworkURI: relativeNetwork(project, network), "firewallRuleType": "VPC_FIREWALL_RULE",
		}
		setList(info, "targetTags", rule.TargetTags)
		setList(info, "targetServiceAccounts", rule.TargetServiceAccounts)
	}
	return map[string]any{
		keyState: "APPLY_" + direction + "_FIREWALL_RULE", keyDescription: "Config checking state: verify " + direction + " firewall rule.",
		keyProjectID: project, keyCausesDrop: false, "firewall": info,
	}
}

// subnetRouteStep is the APPLY_ROUTE step for the destination subnet route.
func subnetRouteStep(project string, subnet *subnetworkRecord) map[string]any {
	return routeStep(project, map[string]any{
		keyDisplayName: subnet.Name, keyRouteType: "SUBNET", keyDestIPRange: subnet.IPCIDRRange,
		keyNextHopType: "NEXT_HOP_NETWORK", keyNextHop: relativeNetwork(project, subnet.Network),
		keyNetworkURI: relativeNetwork(project, subnet.Network), keyPriority: 0,
	})
}

// defaultRouteStep is the APPLY_ROUTE step for the network's default route.
func defaultRouteStep(project, network string) map[string]any {
	return routeStep(project, map[string]any{
		keyDisplayName: "default-route", keyRouteType: "STATIC", keyDestIPRange: "0.0.0.0/0",
		keyNextHopType: "NEXT_HOP_INTERNET_GATEWAY", keyNextHop: "internet gateway",
		keyNetworkURI: relativeNetwork(project, network), keyPriority: 1000,
	})
}

// routeStep wraps one route description in an APPLY_ROUTE step.
func routeStep(project string, route map[string]any) map[string]any {
	return map[string]any{
		keyState: "APPLY_ROUTE", keyDescription: "Config checking state: verify route.",
		keyProjectID: project, keyCausesDrop: false, "route": route,
	}
}

// finalStep is the terminal ABORT or DROP step with its cause.
func finalStep(spec flow, network, stepState, key, cause string) map[string]any {
	detail := map[string]any{keyCause: cause, "resourceUri": relativeNetwork(spec.project, network)}
	description := "Final state: packet could be dropped."
	if key == "abort" {
		detail[keyIPAddress] = spec.destIP.String()
		description = "Final state: analysis is aborted."
	} else {
		detail["sourceIp"], detail["destinationIp"] = spec.sourceIP.String(), spec.destIP.String()
	}
	return map[string]any{
		keyState: stepState, keyDescription: description, keyProjectID: spec.project, keyCausesDrop: true, key: detail,
	}
}

// Connectivity Tests between VM instances follow the steps recorded against
// GCP in tests/conformance/recordings/compute/vm-connectivity-nat-peering: a flow
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
	rule, unmodeled := s.decidingRule(source.Network, directionEgress, spec, endpoints{source, dest})
	if unmodeled != "" {
		return analysis{gap: unmodeled}
	}
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
	if subnet := s.subnetworks[regionalKey(source.Region, source.Subnetwork)]; subnet == nil || subnet.PrivateAccess {
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
	for _, key := range sortedKeys(s.routers) {
		router := s.routers[key]
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
	rule, unmodeled := s.decidingRule(dest.Network, directionIngress, spec, endpoints{source, dest})
	if unmodeled != "" {
		return analysis{gap: unmodeled}
	}
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
	subnet := s.subnetworks[regionalKey(dest.Region, dest.Subnetwork)]
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
