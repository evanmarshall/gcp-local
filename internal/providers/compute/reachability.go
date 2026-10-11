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
// destinations, NAT) reports a coverage gap instead of a verdict.

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

// flow is one validated flow the analyzer evaluates.
type flow struct {
	project       string
	sourceIP      net.IP
	sourceNetwork string // network name; empty resolves it from the source subnet
	destIP        net.IP
	protocol      string // lower case, as firewall terms spell it
	port          int
}

// analysis is the analyzer outcome: a Connectivity Tests result with its
// steps, or a coverage gap when the flow needs topology that is not modeled.
type analysis struct {
	result string
	steps  []map[string]any
	reason string
	gap    string
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

// analyze walks the recorded step order: source network, egress firewall,
// route, then the final state.
func (s *scopeState) analyze(spec flow) analysis {
	source := s.sourceSubnet(spec)
	if source == nil {
		return analysis{gap: "The source address is not inside a modeled subnetwork of the source network"}
	}
	network := source.Network
	steps := []map[string]any{startStep(spec.project, source)}
	rule := s.egressRule(network, spec)
	steps = append(steps, firewallStep(spec.project, network, rule))
	if rule != nil && len(rule.Denied) > 0 {
		steps = append(steps, finalStep(spec, network, "DROP", "drop", "FIREWALL_RULE"))
		return analysis{result: resultUnreachable, steps: steps, reason: "Egress firewall rule " + rule.Name + " denies the flow"}
	}
	if subnet := s.subnetContaining(network, spec.destIP); subnet != nil {
		steps = append(steps, subnetRouteStep(spec.project, subnet),
			finalStep(spec, network, "ABORT", "abort", "UNKNOWN_IP"))
		return analysis{result: resultUndetermined, steps: steps, reason: "No modeled endpoint has the destination address"}
	}
	if route := s.customRoute(network, spec.destIP); route != "" {
		return analysis{gap: "Static route " + route + " matches the destination; route next hops are not modeled"}
	}
	if !spec.destIP.IsPrivate() {
		return analysis{gap: "External destinations (internet egress, NAT) are not modeled"}
	}
	steps = append(steps, defaultRouteStep(spec.project, network),
		finalStep(spec, network, "DROP", "drop", "PRIVATE_TRAFFIC_TO_INTERNET"))
	return analysis{result: resultUnreachable, steps: steps, reason: "The default route sends private traffic to the internet"}
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
	var rules []*firewallRecord
	for _, name := range sortedKeys(s.firewalls) {
		rule := s.firewalls[name]
		if rule.Network == network && rule.Direction == directionEgress && !rule.Disabled &&
			len(rule.TargetTags) == 0 && egressMatches(rule, spec) {
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
	if len(rule.DestinationRanges) == 0 {
		return true
	}
	for _, cidr := range rule.DestinationRanges {
		if cidrContains(cidr, spec.destIP) {
			return true
		}
	}
	return false
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
			keyDisplayName: subnet.Network, "uri": relativeNetwork(project, subnet.Network),
			"matchedIpRange":   subnet.IPCIDRRange,
			"matchedSubnetUri": "projects/" + project + "/regions/" + subnet.Region + "/subnetworks/" + subnet.Name,
			keyRegion:          subnet.Region,
		},
	}
}

// firewallStep is the APPLY_EGRESS_FIREWALL_RULE step: the deciding rule, or
// the implied default-allow-egress rule when no configured rule matches.
func firewallStep(project, network string, rule *firewallRecord) map[string]any {
	info := map[string]any{
		keyDisplayName: "default-allow-egress", keyDirection: directionEgress, "action": "ALLOW",
		keyPriority: 65535, keyNetworkURI: relativeNetwork(project, network),
		"firewallRuleType": "IMPLIED_VPC_FIREWALL_RULE",
	}
	if rule != nil {
		action := "ALLOW"
		if len(rule.Denied) > 0 {
			action = "DENY"
		}
		info = map[string]any{
			keyDisplayName: rule.Name, "uri": "projects/" + project + "/global/firewalls/" + rule.Name,
			keyDirection: directionEgress, "action": action, keyPriority: rule.Priority,
			keyNetworkURI: relativeNetwork(project, network), "firewallRuleType": "VPC_FIREWALL_RULE",
		}
	}
	return map[string]any{
		keyState: "APPLY_EGRESS_FIREWALL_RULE", keyDescription: "Config checking state: verify EGRESS firewall rule.",
		keyProjectID: project, keyCausesDrop: false, "firewall": info,
	}
}

// subnetRouteStep is the APPLY_ROUTE step for the destination subnet route.
func subnetRouteStep(project string, subnet *subnetworkRecord) map[string]any {
	return routeStep(project, map[string]any{
		keyDisplayName: subnet.Name, "routeType": "SUBNET", "destIpRange": subnet.IPCIDRRange,
		"nextHopType": "NEXT_HOP_NETWORK", "nextHop": relativeNetwork(project, subnet.Network),
		keyNetworkURI: relativeNetwork(project, subnet.Network), keyPriority: 0,
	})
}

// defaultRouteStep is the APPLY_ROUTE step for the network's default route.
func defaultRouteStep(project, network string) map[string]any {
	return routeStep(project, map[string]any{
		keyDisplayName: "default-route", "routeType": "STATIC", "destIpRange": "0.0.0.0/0",
		"nextHopType": "NEXT_HOP_INTERNET_GATEWAY", "nextHop": "internet gateway",
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
		detail["ipAddress"] = spec.destIP.String()
		description = "Final state: analysis is aborted."
	} else {
		detail["sourceIp"], detail["destinationIp"] = spec.sourceIP.String(), spec.destIP.String()
	}
	return map[string]any{
		keyState: stepState, keyDescription: description, keyProjectID: spec.project, keyCausesDrop: true, key: detail,
	}
}
