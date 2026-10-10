package compute

import (
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// Reachability is the analyzed verdict of one modeled flow.
type Reachability struct {
	Result  string   `json:"result"`
	Reason  string   `json:"reason"`
	Trace   []string `json:"trace"`
	Unknown []string `json:"unknown,omitempty"`
}

// reachabilityRequest is the posted flow specification.
type reachabilityRequest struct {
	Project    string   `json:"project"`
	SourceIP   string   `json:"sourceIp"`
	DestIP     string   `json:"destIp"`
	Protocol   string   `json:"protocol"`
	Port       int      `json:"port"`
	SourceTags []string `json:"sourceTags"`
	DestTags   []string `json:"destTags"`
}

// Analyzer returns the local reachability surface. It reads only the modeled
// topology: unresolved values and missing topology report "unknown", never a
// pass, and features without modeled resources report "unsupported".
func (p *Provider) Analyzer() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		var request reachabilityRequest
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil {
			writeJSON(w, http.StatusBadRequest, map[string]any{"error": "Invalid reachability request"})
			return
		}
		writeJSON(w, http.StatusOK, p.analyze(request))
	})
}

// writeJSON encodes one analyzer response.
// Values are plain structs and strings; encoding/json never fails on them.
func writeJSON(w http.ResponseWriter, code int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	// The payload is a fixed-shape analyzer response; encoding never fails on it.
	encoded, _ := json.Marshal(value) //nolint:errchkjson // plain analyzer structs
	_, _ = w.Write(encoded)
}

// analyze evaluates one flow against the modeled topology.
func (p *Provider) analyze(request reachabilityRequest) Reachability {
	scope := scopeOf(request.Project, stateGlobal)
	source, unknown := p.locate(scope, request.SourceIP, request.SourceTags, true)
	destination, destUnknown := p.locate(scope, request.DestIP, request.DestTags, false)
	unknown = append(unknown, destUnknown...)
	if len(unknown) > 0 {
		return Reachability{Result: "unknown", Reason: "The topology does not resolve every endpoint", Unknown: unknown}
	}
	if source.network == "" || destination.network == "" {
		return Reachability{Result: "unknown", Reason: "Both endpoints must resolve into a modeled network"}
	}
	trace := []string{}
	if source.network != destination.network {
		peering, found := p.peeringBetween(scope, source.network, destination.network)
		if !found {
			return unsupported("Cross-network flows require a modeled peering; NAT is not modeled")
		}
		trace = append(trace, "peering/"+peering.Name+" to "+peering.PeerNetwork)
	}
	return p.evaluateFirewalls(scope, request, source, destination, trace)
}

// endpoint is one resolved flow endpoint.
type endpoint struct {
	network string
	subnet  string
	tags    []string
}

// locate resolves one endpoint from the topology; misses report unknown reasons.
func (p *Provider) locate(scope state.Scope, ip string, tags []string, isSource bool) (endpoint, []string) {
	parsed := net.ParseIP(ip)
	if parsed == nil {
		return endpoint{}, []string{"The IP " + strconv.Quote(ip) + " is not a valid address"}
	}
	var result endpoint
	var misses []string
	_ = p.scopes.With(scope, func(s *scopeState) error {
		names := make([]string, 0, len(s.subnetworks))
		for name := range s.subnetworks {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			subnet := s.subnetworks[name]
			if !cidrContains(subnet.IPCIDRRange, parsed) {
				continue
			}
			result = endpoint{network: subnet.Network, subnet: name}
			if isSource {
				result.tags = tags
			}
			return nil
		}
		misses = append(misses, "No modeled subnetwork contains "+ip)
		return nil
	})
	return result, misses
}

func unsupported(reason string) Reachability {
	return Reachability{Result: "unsupported", Reason: reason}
}

// cidrContains reports whether the CIDR range contains the address.
func cidrContains(cidr string, ip net.IP) bool {
	_, network, err := net.ParseCIDR(cidr)
	if err != nil {
		return false
	}
	return network.Contains(ip)
}

// evaluateFirewalls applies the modeled firewall precedence to one flow.
func (p *Provider) evaluateFirewalls(scope state.Scope, request reachabilityRequest, source, destination endpoint, trace []string) Reachability {
	verdict, reason := p.applyFirewalls(scope, request, source, destination, &trace)
	if verdict == "" {
		// Compute's implied rules: default deny for ingress, allow for egress.
		verdict, reason = "dropped", "No ingress rule permits the flow; the implied deny applies"
	}
	return Reachability{Result: verdict, Reason: reason, Trace: trace}
}

// peeringBetween finds one directed peering from source to destination.
func (p *Provider) peeringBetween(scope state.Scope, source, destination string) (peeringRecord, bool) {
	var found peeringRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		network := s.networks[source]
		if network == nil {
			return nil
		}
		for _, candidate := range network.Peerings {
			if candidate.PeerNetwork == destination {
				found = candidate
				return nil
			}
		}
		return nil
	})
	return found, found.Name != ""
}

// applyFirewalls returns the modeled verdict and whether one was decided.
func (p *Provider) applyFirewalls(scope state.Scope, request reachabilityRequest, source, destination endpoint, trace *[]string) (string, string) {
	return p.matchRules(scope, request, source, destination, trace)
}

// matchRules walks the modeled rules in precedence order: ingress rules of
// the destination network, egress rules of the source network.
func (p *Provider) matchRules(scope state.Scope, request reachabilityRequest, source, destination endpoint, trace *[]string) (string, string) {
	rules := p.flowFirewalls(scope, source.network, destination.network)
	for _, rule := range rules {
		if rule.Disabled || !ruleMatches(rule, request) {
			continue
		}
		*trace = append(*trace, "firewall/"+rule.Name+" priority="+strconv.Itoa(rule.Priority))
		return ruleVerdict(rule)
	}
	return "", ""
}

// flowFirewalls collects the rules applicable to one flow: ingress rules of
// the destination network plus egress rules of the source network, merged in
// priority order.
func (p *Provider) flowFirewalls(scope state.Scope, sourceNetwork, destinationNetwork string) []*firewallRecord {
	if sourceNetwork == destinationNetwork {
		return p.orderedFirewalls(scope, sourceNetwork, "")
	}
	rules := p.orderedFirewalls(scope, destinationNetwork, directionIngress)
	rules = append(rules, p.orderedFirewalls(scope, sourceNetwork, directionEgress)...)
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].Priority < rules[j].Priority })
	return rules
}

// directionIngress and directionEgress name the firewall rule directions.
const (
	directionIngress = "INGRESS"
	directionEgress  = "EGRESS"
)

// orderedFirewalls returns the network's rules sorted by priority.
func (p *Provider) orderedFirewalls(scope state.Scope, network, direction string) []*firewallRecord {
	var rules []*firewallRecord
	_ = p.scopes.With(scope, func(s *scopeState) error {
		names := make([]string, 0, len(s.firewalls))
		for name := range s.firewalls {
			names = append(names, name)
		}
		sort.Strings(names)
		for _, name := range names {
			rule := s.firewalls[name]
			if rule.Network == network && (direction == "" || rule.Direction == direction) {
				rules = append(rules, rule)
			}
		}
		return nil
	})
	sort.SliceStable(rules, func(i, j int) bool { return rules[i].Priority < rules[j].Priority })
	return rules
}

// ruleVerdict renders the decided verdict of one matching rule.
func ruleVerdict(rule *firewallRecord) (string, string) {
	if len(rule.Denied) > 0 {
		return "dropped", "Firewall rule " + rule.Name + " denies the flow"
	}
	if len(rule.Allowed) > 0 {
		return "reachable", "Firewall rule " + rule.Name + " permits the flow"
	}
	return "", ""
}

// ruleMatches reports whether one rule's terms cover the flow.
func ruleMatches(rule *firewallRecord, request reachabilityRequest) bool {
	if rule.Direction == directionEgress {
		return egressMatches(rule, request)
	}
	for _, cidr := range rule.SourceRanges {
		if parsed := net.ParseIP(request.SourceIP); parsed != nil && cidrContains(cidr, parsed) {
			return ruleServes(rule, request)
		}
	}
	return false
}

// egressMatches evaluates one EGRESS rule against the destination address.
func egressMatches(rule *firewallRecord, request reachabilityRequest) bool {
	if request.DestIP == "" {
		return false
	}
	parsed := net.ParseIP(request.DestIP)
	if parsed == nil {
		return false
	}
	if len(rule.SourceRanges) == 0 {
		return ruleServes(rule, request)
	}
	for _, cidr := range rule.SourceRanges {
		if cidrContains(cidr, parsed) {
			return ruleServes(rule, request)
		}
	}
	return false
}

// ruleServes reports whether any of the rule's terms (allowed or denied)
// covers the flow's protocol and port.
func ruleServes(rule *firewallRecord, request reachabilityRequest) bool {
	return termsMatch(rule.Allowed, request) || termsMatch(rule.Denied, request)
}

// termsMatch reports whether one term list covers the flow.
func termsMatch(entries []map[string]any, request reachabilityRequest) bool {
	for _, entry := range entries {
		protocol := fmt.Sprintf("%v", entry["IPProtocol"])
		if protocol != request.Protocol && protocol != "all" {
			continue
		}
		if portMatches(entry["ports"], request.Port) {
			return true
		}
	}
	return false
}

// portMatches reports whether the port list covers the request port. An
// empty list matches every port (GCE terms without ports are unrestricted).
func portMatches(ports any, port int) bool {
	values := stringListOf(ports)
	if len(values) == 0 {
		return true
	}
	for _, value := range values {
		if strings.Contains(value, "-") {
			bounds := strings.SplitN(value, "-", 2)
			low, lowErr := strconv.Atoi(bounds[0])
			high, highErr := strconv.Atoi(bounds[1])
			if lowErr == nil && highErr == nil && port >= low && port <= high {
				return true
			}
			continue
		}
		if parsed, err := strconv.Atoi(value); err == nil && parsed == port {
			return true
		}
	}
	return false
}

// stateGlobal re-exports the global scope sentinel for readability.
const stateGlobal = "global"
