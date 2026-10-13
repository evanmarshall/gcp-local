package compute

import "time"

// stateKind enumerates the topology resource kinds.
const (
	kindNetwork    = "network"
	kindSubnetwork = "subnetwork"
	kindFirewall   = "firewall"
	kindRoute      = "route"
	kindRouter     = "router"
	kindInstance   = "instance"
)

// Collection names of the resources' relative names and IAM targets.
const (
	collectionNetworks    = "networks"
	collectionSubnetworks = "subnetworks"
	collectionFirewalls   = "firewalls"
	collectionRoutes      = "routes"
	collectionRouters     = "routers"
)

// networkRecord is one stored VPC network.
type networkRecord struct {
	Name                  string
	Description           string
	AutoCreateSubnetworks bool
	RoutingMode           string
	MTU                   int // zero when the caller left it unset
	Peerings              []peeringRecord
	Generation            int64
	CreatedAt             time.Time
}

// peeringRecord is one network peering of a network toward a same-project peer.
type peeringRecord struct {
	Name                 string
	PeerNetwork          string
	ExchangeSubnetRoutes bool
	ImportCustomRoutes   bool
	ExportCustomRoutes   bool
	ExportPublicIP       bool
	ImportPublicIP       bool
	CreatedAt            time.Time
}

type subnetworkRecord struct {
	Name          string
	Description   string
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
	Name              string
	Description       string
	Network           string
	Direction         string
	Allowed           []firewallTerm
	Denied            []firewallTerm
	SourceRanges      []string
	DestinationRanges []string
	TargetTags        []string
	SourceTags        []string
	// The service-account scopes; a rule never mixes them with tags.
	TargetServiceAccounts []string
	SourceServiceAccounts []string
	Priority              int
	Disabled              bool
	Generation            int64
	CreatedAt             time.Time
}

type routeRecord struct {
	Name           string
	Description    string
	Network        string
	DestRange      string
	NextHopGateway string
	NextHopIP      string
	Priority       int
	Tags           []string
	Generation     int64
	CreatedAt      time.Time
}

// natRecord is one Cloud NAT gateway configured on a router. Ranges is the
// sourceSubnetworkIpRangesToNat mode; Subnetworks lists the named
// subnetworks of a LIST_OF_SUBNETWORKS gateway.
type natRecord struct {
	Name        string
	Allocation  string
	Ranges      string
	Subnetworks []string
}

// instanceRecord is one zonal VM instance with a single network interface and
// no external address. Its primary internal IP comes from its subnetwork.
type instanceRecord struct {
	Name        string
	Zone        string
	Region      string
	Description string
	MachineType string
	Network     string
	Subnetwork  string
	IP          string
	Labels      map[string]string
	// The instance's network tags and the email of the service account it
	// runs as ("" for none); firewall rules can be scoped to either.
	Tags           []string
	ServiceAccount string
	Scopes         []string
	Generation     int64
	CreatedAt      time.Time
}

// routerRecord is one stored Cloud Router with its NAT gateways.
type routerRecord struct {
	Name        string
	Description string
	Region      string
	Network     string
	NATs        []natRecord
	Generation  int64
	CreatedAt   time.Time
}

// scopeState is one project/location slice of provider state.
type scopeState struct {
	connectivity map[string]*namedTest
	networks     map[string]*networkRecord
	subnetworks  map[string]*subnetworkRecord // keyed by region/name
	firewalls    map[string]*firewallRecord
	routes       map[string]*routeRecord
	routers      map[string]*routerRecord   // keyed by region/name
	instances    map[string]*instanceRecord // keyed by zone/name
	operations   map[string]*operationRecord
	generation   int64
}

func newScopeState() scopeState {
	return scopeState{
		connectivity: map[string]*namedTest{},
		networks:     map[string]*networkRecord{}, subnetworks: map[string]*subnetworkRecord{},
		firewalls: map[string]*firewallRecord{}, routes: map[string]*routeRecord{},
		routers: map[string]*routerRecord{}, instances: map[string]*instanceRecord{},
		operations: map[string]*operationRecord{},
	}
}

func (s *scopeState) reset() {
	s.connectivity = map[string]*namedTest{}
	s.networks = map[string]*networkRecord{}
	s.subnetworks = map[string]*subnetworkRecord{}
	s.firewalls = map[string]*firewallRecord{}
	s.routes = map[string]*routeRecord{}
	s.routers = map[string]*routerRecord{}
	s.instances = map[string]*instanceRecord{}
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
	Kind    string // "global", "region", or keyZone
	Region  string // the region or zone of a regional or zonal operation
	Type    string
	Target  string
	Error   *operationError
	Done    bool
	Created time.Time
}

// operationError is the error a finished compute operation reports. Compute
// validates some requests asynchronously: the insert or delete call succeeds
// and the operation finishes DONE with this error instead.
type operationError struct {
	Code    string
	Message string
}
