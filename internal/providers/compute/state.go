package compute

import "time"

// stateKind enumerates the topology resource kinds.
const (
	kindNetwork    = "network"
	kindSubnetwork = "subnetwork"
	kindFirewall   = "firewall"
	kindRoute      = "route"
	kindLocation   = "location"
)

// networkRecord is one stored VPC network.
type networkRecord struct {
	Name                  string
	AutoCreateSubnetworks bool
	MTU                   int
	Peerings              []peeringRecord
	Generation            int64
	CreatedAt             time.Time
}

// peeringRecord is one network peering of a network.
type peeringRecord struct {
	Name               string
	PeerNetwork        string
	ImportCustomRoutes bool
	ExportCustomRoutes bool
}

// subnetworkRecord is one stored subnetwork.
type subnetworkRecord struct {
	Name          string
	Network       string
	Region        string
	IPCIDRRange   string
	Gateway       string
	PrivateAccess bool
	Generation    int64
	CreatedAt     time.Time
}

// firewallRecord is one stored firewall rule.
type firewallRecord struct {
	Name         string
	Network      string
	Direction    string
	Allowed      []map[string]any
	Denied       []map[string]any
	SourceRanges []string
	TargetTags   []string
	Priority     int
	Disabled     bool
	Generation   int64
	CreatedAt    time.Time
}

// routeRecord is one stored route.
type routeRecord struct {
	Name           string
	Network        string
	DestRange      string
	NextHopGateway string
	Priority       int
	Tags           []string
	Generation     int64
	CreatedAt      time.Time
}

// scopeState is one project/location slice of provider state.
type scopeState struct {
	networks    map[string]*networkRecord
	subnetworks map[string]*subnetworkRecord
	firewalls   map[string]*firewallRecord
	routes      map[string]*routeRecord
	operations  map[string]*operationRecord
	generation  int64
}

func newScopeState() scopeState {
	return scopeState{
		networks: map[string]*networkRecord{}, subnetworks: map[string]*subnetworkRecord{},
		firewalls: map[string]*firewallRecord{}, routes: map[string]*routeRecord{},
		operations: map[string]*operationRecord{},
	}
}

func (s *scopeState) reset() {
	s.networks = map[string]*networkRecord{}
	s.subnetworks = map[string]*subnetworkRecord{}
	s.firewalls = map[string]*firewallRecord{}
	s.routes = map[string]*routeRecord{}
	s.operations = map[string]*operationRecord{}
}

func (s *scopeState) nextGeneration() int64 {
	s.generation++
	return s.generation
}

// operationRecord is one compute-style long-running operation.
type operationRecord struct {
	ID      string
	Name    string
	Kind    string // "global" or "region"
	Region  string
	Type    string
	Target  string
	Error   string
	Done    bool
	Created time.Time
}
