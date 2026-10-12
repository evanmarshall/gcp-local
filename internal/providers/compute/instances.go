package compute

import (
	"encoding/binary"
	"errors"
	"maps"
	"net"
	"net/url"
	"regexp"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Zonal VM instances exist so Connectivity Tests can name them as endpoints
// (tests/conformance/recordings/compute/vm-connectivity-nat-peering). An instance has
// exactly one network interface on a subnetwork and no external address; it
// gets the lowest free primary internal IP of that subnetwork. Boot disks are
// rendered on the instance but are not separate disk resources, and nothing
// runs: the instance is a topology endpoint, not a VM.

// zonePattern pins the zone shape (region plus one-letter suffix).
var zonePattern = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+-[a-z]$`)

// Instance request failures that map onto 400 responses.
var (
	errWrongRegion  = errors.New("the subnetwork is not in the instance's region")
	errAddressTaken = errors.New("the requested internal address is unavailable")
	errExhausted    = errors.New("the subnetwork has no free internal address")
	errIndex        = errors.New("the index refused the instance")
)

func (p *Provider) executeInstances(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodInstancesInsert:
		return p.createInstance(request, scope)
	case methodInstancesGet:
		return p.getInstance(request, scope)
	case methodInstancesList:
		return p.listInstances(request, scope)
	default:
		return p.deleteInstance(request, scope)
	}
}

func zoneRegion(zone string) (string, bool) {
	if !zonePattern.MatchString(zone) {
		return "", false
	}
	return zone[:strings.LastIndex(zone, "-")], true
}

// instanceKey is the scope-state key of one zonal instance.
func instanceKey(zone, name string) string {
	return zone + "/" + name
}

// zoneLink builds the self-link URL of one zone.
func zoneLink(project, zone string) string {
	return wireHost + "/projects/" + url.PathEscape(project) + "/zones/" + url.PathEscape(zone)
}

// instanceInterface is the requested network attachment of an instance.
type instanceInterface struct {
	network, subnetwork, address string
}

// parseInstance validates an instance insert body. A second interface or
// any external, IPv6, or alias address is an explicit gap.
func parseInstance(body map[string]any) (instanceInterface, string, error) {
	machineType := provider.Text(body["machineType"])
	if machineType == "" {
		return instanceInterface{}, "", provider.Invalid("An instance requires a machineType")
	}
	if !hasBootDisk(provider.MapList(body["disks"])) {
		return instanceInterface{}, "", provider.Invalid("An instance requires a boot disk")
	}
	nics := provider.MapList(body["networkInterfaces"])
	if len(nics) == 0 {
		return instanceInterface{}, "", provider.Invalid("An instance requires a network interface")
	}
	if len(nics) > 1 {
		return instanceInterface{}, "", gap("instances with more than one network interface are not modeled")
	}
	nic := nics[0]
	for _, field := range []string{"accessConfigs", "ipv6AccessConfigs", "aliasIpRanges"} {
		if len(provider.MapList(nic[field])) > 0 {
			return instanceInterface{}, "", gap("instance " + field + " (external, IPv6, or alias addresses) are not modeled")
		}
	}
	return instanceInterface{
		network: networkName(provider.Text(nic[keyNetwork])), subnetwork: subnetName(provider.Text(nic["subnetwork"])),
		address: provider.Text(nic["networkIP"]),
	}, machineType[strings.LastIndex(machineType, "/")+1:], nil
}

// hasBootDisk reports whether the attached disks include a boot disk.
func hasBootDisk(disks []map[string]any) bool {
	for _, disk := range disks {
		if boolOf(disk["boot"], false) {
			return true
		}
	}
	return false
}

// createInstance validates and stores one instance, assigning its internal
// address from the subnetwork, then reports the zonal operation.
func (p *Provider) createInstance(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	zone := request.Path["zone"]
	region, ok := zoneRegion(zone)
	if !ok {
		return nil, provider.Invalid("Invalid zone " + zone)
	}
	body, err := parseInsert(request)
	if err != nil {
		return nil, err
	}
	nic, machineType, err := parseInstance(body)
	if err != nil {
		return nil, err
	}
	record := &instanceRecord{
		Name: provider.Text(body[keyName]), Zone: zone, Region: region, Description: provider.Text(body[keyDescription]),
		MachineType: machineType, CreatedAt: p.clock(),
	}
	record.Labels, _ = body["labels"].(map[string]any)
	if err := p.scopes.With(scope, func(s *scopeState) error {
		return s.addInstance(record, nic, func() error {
			if err := p.index.Put(serviceName, state.Resource{
				Name: instanceName(scope.Project, zone, record.Name), Service: serviceName, Kind: kindInstance,
				Parent: provider.ProjectResource(scope.Project), Scope: scope,
			}); err != nil {
				return errIndex
			}
			return nil
		})
	}); err != nil {
		return nil, instanceError(err, record.Name)
	}
	return p.newOperation(scope, "zone", zone, "insert", instanceLink(scope.Project, zone, record.Name))
}

// instanceError maps an instance placement failure onto its status.
func instanceError(err error, name string) error {
	switch {
	case errors.Is(err, errAlreadyExists):
		return conflict(kindInstance, name)
	case errors.Is(err, errIndex):
		return gap("index the instance")
	case errors.Is(err, errMissing):
		return status.Error(codes.InvalidArgument, "The referenced network or subnetwork does not exist")
	default:
		return status.Error(codes.InvalidArgument, err.Error())
	}
}

// instanceName builds the canonical instance resource name.
func instanceName(project, zone, name string) string {
	return "//compute.googleapis.com/projects/" + url.PathEscape(project) + "/zones/" +
		url.PathEscape(zone) + "/instances/" + url.PathEscape(name)
}

// instanceLink builds the self-link URL of one instance.
func instanceLink(project, zone, name string) string {
	return zoneLink(project, zone) + "/instances/" + url.PathEscape(name)
}

// addInstance places one instance on its subnetwork under the scope lock.
// publish indexes it; the record is stored only when that succeeds.
func (s *scopeState) addInstance(record *instanceRecord, nic instanceInterface, publish func() error) error {
	key := instanceKey(record.Zone, record.Name)
	if s.instances[key] != nil {
		return errAlreadyExists
	}
	subnet := s.attachSubnet(nic, record.Region)
	if subnet == nil {
		return errMissing
	}
	if subnet.Region != record.Region || (nic.network != "" && nic.network != subnet.Network) {
		return errWrongRegion
	}
	address, err := s.allocateIP(subnet, nic.address)
	if err != nil {
		return err
	}
	if err := publish(); err != nil {
		return err
	}
	record.Network, record.Subnetwork, record.IP = subnet.Network, subnet.Name, address
	record.Generation = s.nextGeneration()
	s.instances[key] = record
	return nil
}

// attachSubnet resolves the interface's subnetwork: the named one, or the
// network's only subnetwork in the region when just a network is given.
func (s *scopeState) attachSubnet(nic instanceInterface, region string) *subnetworkRecord {
	if nic.subnetwork != "" {
		return s.subnetworks[nic.subnetwork]
	}
	for _, name := range sortedKeys(s.subnetworks) {
		if subnet := s.subnetworks[name]; subnet.Network == nic.network && subnet.Region == region {
			return subnet
		}
	}
	return nil
}

// allocateIP returns the requested address when it is a free usable host of
// the subnetwork, or else the lowest free one. Compute reserves the network
// and gateway addresses and the last two addresses of every primary range.
func (s *scopeState) allocateIP(subnet *subnetworkRecord, requested string) (string, error) {
	_, block, err := net.ParseCIDR(subnet.IPCIDRRange)
	if err != nil || block.IP.To4() == nil {
		return "", errExhausted
	}
	used := map[string]bool{}
	for _, record := range s.instances {
		used[record.IP] = true
	}
	if requested != "" {
		return claimIP(block, requested, used)
	}
	base := binary.BigEndian.Uint32(block.IP.To4())
	for offset := uint32(2); ; offset++ {
		candidate := make(net.IP, net.IPv4len)
		binary.BigEndian.PutUint32(candidate, base+offset)
		if !usableHost(block, candidate) {
			return "", errExhausted
		}
		if !used[candidate.String()] {
			return candidate.String(), nil
		}
	}
}

// claimIP validates a requested internal address against the range and the
// addresses in use.
func claimIP(block *net.IPNet, requested string, used map[string]bool) (string, error) {
	ip := net.ParseIP(requested).To4()
	if ip == nil || used[ip.String()] || !usableHost(block, ip) {
		return "", errAddressTaken
	}
	return ip.String(), nil
}

// usableHost reports whether ip is an assignable host of the IPv4 range:
// inside it, past the network and gateway addresses, and before the last two.
func usableHost(block *net.IPNet, ip net.IP) bool {
	if !block.Contains(ip) {
		return false
	}
	ones, bits := block.Mask.Size()
	offset := uint64(binary.BigEndian.Uint32(ip.To4()) - binary.BigEndian.Uint32(block.IP.To4()))
	return offset >= 2 && offset+2 < uint64(1)<<(bits-ones)
}

func (p *Provider) getInstance(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	zone, name := request.Path["zone"], request.Path["instance"]
	var document map[string]any
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if record := s.instances[instanceKey(zone, name)]; record != nil {
			document = instanceDocument(scope.Project, record)
		}
		return nil
	})
	if document == nil {
		return nil, provider.NotFound(kindInstance, name)
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// listInstances returns the zone's instances in name order.
func (p *Provider) listInstances(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	zone := request.Path["zone"]
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, key := range sortedKeys(s.instances) {
			if record := s.instances[key]; record.Zone == zone {
				items = append(items, instanceDocument(scope.Project, record))
			}
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{keyItems: items, keyKind: "compute#instanceList"}}, nil
}

// deleteInstance removes one instance, releasing its internal address.
func (p *Provider) deleteInstance(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	zone, name := request.Path["zone"], request.Path["instance"]
	found := false
	if err := p.scopes.With(scope, func(s *scopeState) error {
		if found = s.instances[instanceKey(zone, name)] != nil; !found {
			return nil
		}
		if err := p.unindex(scope, instanceName(scope.Project, zone, name)); err != nil {
			return err
		}
		delete(s.instances, instanceKey(zone, name))
		return nil
	}); err != nil {
		return nil, err //nolint:wrapcheck // unindex already wraps
	}
	if !found {
		return nil, provider.NotFound(kindInstance, name)
	}
	return p.newOperation(scope, "zone", zone, "delete", instanceLink(scope.Project, zone, name))
}

// instanceDocument renders the instance wire shape: one interface with its
// internal address and one autoDelete boot disk named after the instance.
func instanceDocument(project string, record *instanceRecord) map[string]any {
	id := idOf(record.Name, record.Generation)
	document := map[string]any{
		"id":               id,
		keyName:            record.Name,
		keyKind:            "compute#instance",
		keySelfLink:        instanceLink(project, record.Zone, record.Name),
		keyTimestamp:       stamp(record.CreatedAt),
		"zone":             zoneLink(project, record.Zone),
		"machineType":      zoneLink(project, record.Zone) + "/machineTypes/" + record.MachineType,
		"status":           "RUNNING",
		"canIpForward":     false,
		keyFingerprint:     fingerprint(record.Name, record.Generation),
		"labelFingerprint": fingerprint(record.Name+"/labels", record.Generation),
		"networkInterfaces": []any{map[string]any{
			keyKind: "compute#networkInterface", keyName: "nic0", keyNetwork: selfLink(project, "networks", record.Network),
			"subnetwork": regionalSelfLink(project, record.Region, "subnetworks", record.Subnetwork),
			"networkIP":  record.IP, keyStackType: stackIPv4Only, keyFingerprint: fingerprint(record.Name+"/nic0", record.Generation),
		}},
		"disks": []any{map[string]any{
			keyKind: "compute#attachedDisk", "type": "PERSISTENT", "mode": "READ_WRITE", "boot": true, "autoDelete": true,
			"deviceName": "persistent-disk-0", "index": 0, "interface": "SCSI",
			"source": zoneLink(project, record.Zone) + "/disks/" + record.Name,
		}},
	}
	setText(document, keyDescription, record.Description)
	if len(record.Labels) > 0 {
		document["labels"] = maps.Clone(record.Labels)
	}
	return document
}
