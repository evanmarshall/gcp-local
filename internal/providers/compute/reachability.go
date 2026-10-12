package compute

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"
)

// The analyzer follows the Connectivity Tests steps recorded against GCP for
// IP-address endpoints (tests/conformance/recordings/vpc-firewall-connectivity):
// a flow starts from the source subnet, applies the source network's egress
// firewall rules, then its routes. No VM instances or other endpoints are
// modeled, so a destination inside the network aborts with UNKNOWN_IP rather
// than reporting a firewall verdict, and a private destination reached only
// through the default route drops with PRIVATE_TRAFFIC_TO_INTERNET. Anything
// the recording does not establish (custom-route next hops, external
// destinations, NAT) reports a coverage gap instead of a verdict. Flows that
// name VM instances follow a separate recording; see reachability_instance.go.

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
		return flow{}, statusInvalid("Connectivity test endpoints require valid IP addresses")
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
	_ = p.scopes.With(scopeOf(spec.project, stateGlobal), func(s *scopeState) error {
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
	rule := s.egressRule(network, spec)
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
	for _, name := range sortedKeys(s.subnetworks) {
		subnet := s.subnetworks[name]
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
	for _, name := range sortedKeys(s.subnetworks) {
		if subnet := s.subnetworks[name]; subnet.Network == network && cidrContains(subnet.IPCIDRRange, ip) {
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

// egressRule returns the deciding enabled EGRESS rule of the network for the
// flow, or nil when the implied allow-egress rule applies. Rules with target
// tags never match: tagged endpoints are not modeled. At equal priority a deny
// rule takes precedence, as in VPC firewall evaluation.
func (s *scopeState) egressRule(network string, spec flow) *firewallRecord {
	return s.decidingRule(network, directionEgress, spec, egressMatches)
}

// decidingRule returns the highest-priority enabled untagged rule of one
// direction that matches the flow, deny first at equal priority, or nil.
func (s *scopeState) decidingRule(network, direction string, spec flow, matches func(*firewallRecord, flow) bool) *firewallRecord {
	var rules []*firewallRecord
	for _, name := range sortedKeys(s.firewalls) {
		if rule := s.firewalls[name]; rule.applies(network, direction, false) && matches(rule, spec) {
			rules = append(rules, rule)
		}
	}
	sort.SliceStable(rules, func(i, j int) bool {
		if rules[i].Priority != rules[j].Priority {
			return rules[i].Priority < rules[j].Priority
		}
		return len(rules[i].Denied) > 0 && len(rules[j].Denied) == 0
	})
	if len(rules) == 0 {
		return nil
	}
	return rules[0]
}

// applies reports whether an enabled rule of the network and direction is
// tagged (source or target tags) exactly when tagged is set.
func (rule *firewallRecord) applies(network, direction string, tagged bool) bool {
	hasTags := len(rule.TargetTags) > 0 || len(rule.SourceTags) > 0
	return rule.Network == network && rule.Direction == direction && !rule.Disabled && hasTags == tagged
}

// Firewall rule directions.
const (
	directionIngress = "INGRESS"
	directionEgress  = "EGRESS"
)

// egressMatches reports whether an EGRESS rule covers the flow's destination,
// protocol, and port. A rule without destination ranges covers every address.
func egressMatches(rule *firewallRecord, spec flow) bool {
	if !termsMatch(rule.Allowed, spec) && !termsMatch(rule.Denied, spec) {
		return false
	}
	return rangesCover(rule.DestinationRanges, spec.destIP)
}

// termsMatch reports whether one allowed/denied term list covers the flow.
func termsMatch(entries []map[string]any, spec flow) bool {
	for _, entry := range entries {
		protocol := strings.ToLower(fmt.Sprint(entry[keyIPProto]))
		if protocol != spec.protocol && protocol != "all" {
			continue
		}
		if portMatches(entry[keyPorts], spec.port) {
			return true
		}
	}
	return false
}

// portMatches reports whether the port list covers the port. An empty list
// matches every port (terms without ports are unrestricted).
func portMatches(ports any, port int) bool {
	values := stringListOf(ports)
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
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

// stateGlobal names the single compute location scope.
const stateGlobal = "global"

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
