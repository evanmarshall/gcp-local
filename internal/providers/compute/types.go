// Package compute implements the first networking control-plane resources:
// networks, subnetworks, firewalls, routes, and their long-running operations.
// The surface follows the recorded v1 REST contracts; broader topology (routers,
// NAT, peering, PSA/PSC) stays an explicit gap rather than an approximation.
package compute

import (
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"reflect"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName is the provider and backend registry name.
const serviceName = "compute"

// uidPattern pins the GCE resource name shape.
var uidPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// cidrPattern pins the accepted IPv4 CIDR range shape.
var cidrPattern = regexp.MustCompile(`^([0-9]{1,3}\.){3}[0-9]{1,3}/[0-9]{1,2}$`)

// Wire document keys shared by the compute resource and response shapes.
const (
	keyItems            = "items"
	keyName             = "name"
	keySelfLink         = "selfLink"
	keyTimestamp        = "creationTimestamp"
	keyKind             = "kind"
	keyNetwork          = "network"
	keyIPProto          = "IPProtocol"
	keyPeerNetwork      = "peerNetwork"
	keyDestRange        = "destRange"
	keyError            = "error"
	keyMessage          = "message"
	keyDescription      = "description"
	keyCode             = "code"
	keyPorts            = "ports"
	keyAutoSubnets      = "autoCreateSubnetworks"
	keyDestIP           = "destIp"
	keyPort             = "port"
	keyIPCIDR           = "ipCidrRange"
	keyProject          = "project"
	keySourceIP         = "sourceIp"
	keySource           = "source"
	keyDestination      = "destination"
	keyProtocol         = "protocol"
	keyState            = "state"
	keyCause            = "cause"
	keyCausesDrop       = "causesDrop"
	keyRegion           = "region"
	keyDirection        = "direction"
	keyPriority         = "priority"
	keyNetworkURI       = "networkUri"
	stackIPv4Only       = "IPV4_ONLY"
	keyStackType        = "stackType"
	keyDisplayName      = "displayName"
	keyProjectID        = "projectId"
	keyFingerprint      = "fingerprint"
	keyURI              = "uri"
	keyDestIPRange      = "destIpRange"
	keyNextHop          = "nextHop"
	keyIPAddress        = "ipAddress"
	keyRouteType        = "routeType"
	keyNextHopType      = "nextHopType"
	keyAtType           = "@type"
	keyMetadata         = "metadata"
	keyZone             = "zone"
	keyStatus           = "status"
	keyErrors           = "errors"
	keyRoutingConfig    = "routingConfig"
	keyPrivateIPv6      = "privateIpv6GoogleAccess"
	keyLogConfig        = "logConfig"
	keyNATs             = "nats"
	keyParams           = "params"
	strategyIndependent = "INDEPENDENT"
	jsonNull            = "null"
)

// wireHost is the self-link host the recorded compute documents carry. Compute
// answers on compute.googleapis.com but links resources under www.googleapis.com.
const wireHost = "https://www.googleapis.com/compute/v1"

// globalName builds the canonical global resource name.
func globalName(project, kind, id string) string {
	return "//compute.googleapis.com/projects/" + url.PathEscape(project) + "/global/" + kind + "/" + url.PathEscape(id)
}

// regionalName builds the canonical regional resource name.
func regionalName(project, location, kind, id string) string {
	return "//compute.googleapis.com/projects/" + url.PathEscape(project) + "/regions/" +
		url.PathEscape(location) + "/" + kind + "/" + url.PathEscape(id)
}

// selfLink builds the recorded self-link URL of a global resource.
func selfLink(project, collection, id string) string {
	return wireHost + "/projects/" + url.PathEscape(project) + "/global/" + collection + "/" + url.PathEscape(id)
}

// regionalSelfLink builds the self-link URL of a regional resource.
func regionalSelfLink(project, location, collection, id string) string {
	return wireHost + "/projects/" + url.PathEscape(project) + "/regions/" +
		url.PathEscape(location) + "/" + collection + "/" + url.PathEscape(id)
}

// regionLink builds the self-link URL of one region.
func regionLink(project, region string) string {
	return wireHost + "/projects/" + url.PathEscape(project) + "/regions/" + url.PathEscape(region)
}

// gap reports an unimplemented slice boundary.
var gap = provider.CoverageGap("Compute")

// wireError renders Compute's native error body: an errors[] list whose
// entries carry the domain and reason, and no status field, unlike the shared
// Google API body (recorded: vm-firewall-service-accounts).
func wireError(code int, reason, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		keyError: map[string]any{
			keyCode: code, keyMessage: message,
			keyErrors: []any{map[string]any{"domain": "global", "reason": reason, keyMessage: message}},
		},
	}}
}

// computeError is an error a method answers with a ready Compute body.
type computeError struct {
	response *gateway.RESTResponse
	message  string
}

func (e *computeError) Error() string { return e.message }

// badRequest is a Compute validation refusal (reason badRequest).
func badRequest(message string) error {
	return &computeError{response: wireError(400, "badRequest", message), message: message}
}

// invalidValue is the request parser's refusal of a value of the wrong JSON
// type: it carries a status and a BadRequest detail and no domain (recorded).
func invalidValue(field, kind, raw string) error {
	message := fmt.Sprintf("Invalid value at '%s' (%s), %s", field, kind, raw)
	return &computeError{message: message, response: &gateway.RESTResponse{Status: 400, JSON: map[string]any{
		keyError: map[string]any{
			keyCode: 400, keyMessage: message, keyStatus: "INVALID_ARGUMENT",
			keyErrors: []any{map[string]any{"reason": "invalid", keyMessage: message}},
			"details": []any{map[string]any{
				keyAtType:         "type.googleapis.com/google.rpc.BadRequest",
				"fieldViolations": []any{map[string]any{"field": field, "description": message}},
			}},
		},
	}}}
}

// statusReasons maps the statuses Compute methods return onto Compute's
// native body; other statuses (coverage gaps) keep the shared body.
var statusReasons = map[codes.Code]struct {
	code   int
	reason string
}{
	codes.InvalidArgument: {400, "invalid"}, codes.NotFound: {404, "notFound"},
	codes.AlreadyExists: {409, "alreadyExists"},
}

// computeResponse renders a Compute method's error in Compute's body, or
// returns nil to leave it to the gateway.
func computeResponse(err error) *gateway.RESTResponse {
	if wire, isWire := errors.AsType[*computeError](err); isWire {
		return wire.response
	}
	refusal, ok := status.FromError(err)
	if !ok {
		return nil
	}
	if mapped, known := statusReasons[refusal.Code()]; known {
		return wireError(mapped.code, mapped.reason, refusal.Message())
	}
	return nil
}

// globalRef, regionalRef, and zonalRef are the relative resource paths
// Compute's messages name.
func globalRef(project, collection, name string) string {
	return "projects/" + project + "/global/" + collection + "/" + name
}

func regionalRef(project, region, collection, name string) string {
	return "projects/" + project + "/regions/" + region + "/" + collection + "/" + name
}

func zonalRef(project, zone, collection, name string) string {
	return "projects/" + project + "/zones/" + zone + "/" + collection + "/" + name
}

// notFound reports a missing resource in Compute's words (recorded).
func notFound(path string) error {
	return status.Error(codes.NotFound, "The resource '"+path+"' was not found")
}

// conflict reports a resource that already exists.
func conflict(path string) error {
	return status.Error(codes.AlreadyExists, "The resource '"+path+"' already exists")
}

// missingReference reports a resource that names a nonexistent dependency.
func missingReference() error {
	return status.Error(codes.InvalidArgument, "The referenced network does not exist")
}

// stamp renders Compute's RFC 3339 timestamps with a zone offset.
func stamp(moment time.Time) string {
	return moment.UTC().Format("2006-01-02T15:04:05.999999999Z07:00")
}

// idOf derives a numeric resource id from a name and a generation counter.
func idOf(name string, generation int64) string {
	return strconv.FormatInt(generation*1000+int64(fnv(name))%1000, 10)
}

// fnv derives a small stable hash for identifier shaping.
func fnv(name string) int {
	hash := 2166136261
	for _, char := range []byte(name) {
		hash ^= int(char)
		hash *= 16777619
	}
	if hash < 0 {
		hash = -hash
	}
	return hash % 997
}

// Request bodies. Each struct holds the fields of one Compute v1 resource
// (discovery revision 20261007) that the provider models, and decodeBody
// refuses a value of the wrong JSON type with Compute's 400. Every other input
// field of the resource is listed in its unmodeled table and refused as a
// coverage gap when set to anything but its default, so no field the provider
// can't honor is dropped in silence. Names outside the resource schema are
// ignored, as Compute ignores them (recorded: vm-firewall-service-accounts).

// bodySchema describes a request body beyond its Go struct. unmodeled maps a
// body path ("" is the resource, "disks" each attached disk) to the input
// fields there the provider doesn't model; output-only fields and update
// fingerprints are ignored, as on insert. types names, by field path, the
// protobuf type a type refusal reports where it isn't the scalar default.
type bodySchema struct {
	unmodeled map[string][]string
	types     map[string]string
}

// modeledBody is a request body that can name a field set to a value the
// provider can't honor.
type modeledBody interface {
	unmodeledValue() string
}

// decodeBody decodes a request body into its typed shape. A value of the
// wrong JSON type is refused as Compute's request parser refuses it, and an
// unmodeled field or value as a coverage gap; unknown names are ignored.
func decodeBody[T any, P interface {
	*T
	modeledBody
}](raw []byte, schema bodySchema) (*T, error) {
	if len(strings.TrimSpace(string(raw))) == 0 {
		raw = []byte("{}")
	}
	var generic any
	_ = json.Unmarshal(raw, &generic)
	if _, isObject := generic.(map[string]any); !isObject {
		return nil, badRequest("Invalid JSON payload received.")
	}
	body := P(new(T))
	if err := json.Unmarshal(raw, body); err != nil {
		if typeErr, isType := errors.AsType[*json.UnmarshalTypeError](err); isType {
			return nil, schema.typeError(typeErr, generic)
		}
		if _, isStatus := status.FromError(err); isStatus {
			return nil, err //nolint:wrapcheck // a field's coverage gap
		}
		return nil, badRequest("Invalid JSON payload received.")
	}
	if field := unmodeledIn(generic, schema.unmodeled); field != "" {
		return nil, gap("the field " + field + " is not modeled")
	}
	if field := body.unmodeledValue(); field != "" {
		return nil, gap("the value of " + field + " is not modeled")
	}
	return (*T)(body), nil
}

// typeError renders a type refusal: the field path under resource, its
// protobuf type, and the value as sent.
func (schema bodySchema) typeError(err *json.UnmarshalTypeError, body any) error {
	kind, known := schema.types[err.Field]
	if !known {
		switch err.Type.Kind() {
		case reflect.Bool:
			kind = "TYPE_BOOL"
		case reflect.Int32, reflect.Int64, reflect.Int:
			kind = "TYPE_INT64"
		default:
			kind = "TYPE_STRING"
		}
	}
	raw, _ := json.Marshal(valueAt(body, err.Field)) //nolint:errchkjson // a decoded JSON value
	return invalidValue("resource."+err.Field, kind, string(raw))
}

// valueAt finds the value at a dotted field path, reading the first list
// item that has it.
func valueAt(value any, path string) any {
	if list, ok := value.([]any); ok {
		for _, item := range list {
			if found := valueAt(item, path); found != nil {
				return found
			}
		}
		return nil
	}
	object, ok := value.(map[string]any)
	if !ok || path == "" {
		return value
	}
	head, rest, _ := strings.Cut(path, ".")
	if rest == "" {
		return object[head]
	}
	return valueAt(object[head], rest)
}

// networkBody is a networks.insert body.
type networkBody struct {
	Name                  string       `json:"name"`
	Description           string       `json:"description"`
	AutoCreateSubnetworks *flexBool    `json:"autoCreateSubnetworks"`
	MTU                   flexInt      `json:"mtu"`
	RoutingConfig         *routingBody `json:"routingConfig"`
	EnforcementOrder      string       `json:"networkFirewallPolicyEnforcementOrder"`
}

// routingBody is a network's routingConfig.
type routingBody struct {
	RoutingMode       string `json:"routingMode"`
	BestPathSelection string `json:"bgpBestPathSelectionMode"`
}

var networkSchema = bodySchema{
	unmodeled: map[string][]string{
		"":               {"IPv4Range", "enableUlaInternalIpv6", "internalIpv6Range", "networkProfile", keyParams},
		keyRoutingConfig: {"bgpAlwaysCompareMed", "bgpInterRegionCost"},
	},
	types: map[string]string{"mtu": "TYPE_INT32", keyRoutingConfig: "type.googleapis.com/compute.v1.NetworkRoutingConfig"},
}

// unmodeledValue names a modeled field set to a value the provider can't
// honor: only the defaults the network document renders are modeled.
func (b *networkBody) unmodeledValue() string {
	switch {
	case b.EnforcementOrder != "" && b.EnforcementOrder != "AFTER_CLASSIC_FIREWALL":
		return "networkFirewallPolicyEnforcementOrder"
	case b.RoutingConfig != nil && b.RoutingConfig.BestPathSelection != "" && b.RoutingConfig.BestPathSelection != "LEGACY":
		return "routingConfig.bgpBestPathSelectionMode"
	}
	return ""
}

// subnetworkBody is a subnetworks.insert body.
type subnetworkBody struct {
	Name          string         `json:"name"`
	Description   string         `json:"description"`
	Network       string         `json:"network"`
	Region        string         `json:"region"`
	IPCIDRRange   string         `json:"ipCidrRange"`
	PrivateAccess flexBool       `json:"privateIpGoogleAccess"`
	LogConfig     *logConfigBody `json:"logConfig"`
	EnableLogs    flexBool       `json:"enableFlowLogs"`
	Purpose       string         `json:"purpose"`
	StackType     string         `json:"stackType"`
	PrivateIPv6   string         `json:"privateIpv6GoogleAccess"`
	Overlap       flexBool       `json:"allowSubnetCidrRoutesOverlap"`
}

// logConfigBody is a firewall or subnetwork logConfig; only disabled logging
// is modeled.
type logConfigBody struct {
	Enable flexBool `json:"enable"`
}

var subnetworkSchema = bodySchema{
	unmodeled: map[string][]string{
		"": {"externalIpv6Prefix", "internalIpv6Prefix", "ipCollection", "ipv6AccessType", keyParams,
			"reservedInternalRange", "resolveSubnetMask", "role", "secondaryIpRanges"},
		keyLogConfig: {"aggregationInterval", "filterExpr", "flowSampling", keyMetadata, "metadataFields"},
	},
	types: map[string]string{keyLogConfig: "type.googleapis.com/compute.v1.SubnetworkLogConfig"},
}

// unmodeledValue names a modeled field set to a value the provider can't honor.
func (b *subnetworkBody) unmodeledValue() string {
	return firstTrue(
		named{"logConfig.enable", (b.LogConfig != nil && bool(b.LogConfig.Enable)) || bool(b.EnableLogs)},
		named{"purpose", b.Purpose != "" && b.Purpose != "PRIVATE"},
		named{"stackType", b.StackType != "" && b.StackType != stackIPv4Only},
		named{keyPrivateIPv6, b.PrivateIPv6 != "" && b.PrivateIPv6 != "DISABLE_GOOGLE_ACCESS"},
		named{"allowSubnetCidrRoutesOverlap", bool(b.Overlap)},
	)
}

// named is one condition with the field it names.
type named struct {
	field string
	holds bool
}

// firstTrue names the field of the first condition that holds, or "".
func firstTrue(conditions ...named) string {
	for _, condition := range conditions {
		if condition.holds {
			return condition.field
		}
	}
	return ""
}

// firewallBody is a firewalls.insert or firewalls.update body.
type firewallBody struct {
	Name                  string         `json:"name"`
	Description           string         `json:"description"`
	Network               string         `json:"network"`
	Direction             string         `json:"direction"`
	Priority              *flexInt       `json:"priority"`
	Disabled              flexBool       `json:"disabled"`
	Allowed               []firewallTerm `json:"allowed"`
	Denied                []firewallTerm `json:"denied"`
	SourceRanges          flexStrings    `json:"sourceRanges"`
	DestinationRanges     flexStrings    `json:"destinationRanges"`
	SourceTags            flexStrings    `json:"sourceTags"`
	TargetTags            flexStrings    `json:"targetTags"`
	SourceServiceAccounts flexStrings    `json:"sourceServiceAccounts"`
	TargetServiceAccounts flexStrings    `json:"targetServiceAccounts"`
	LogConfig             *logConfigBody `json:"logConfig"`
}

// firewallTerm is one allowed or denied protocol with optional ports.
type firewallTerm struct {
	IPProtocol protocolName `json:"IPProtocol"`
	Ports      flexStrings  `json:"ports,omitempty"`
}

var firewallSchema = bodySchema{
	unmodeled: map[string][]string{"": {keyParams}, keyLogConfig: {keyMetadata}},
	types: map[string]string{
		"priority": "TYPE_INT32", keyLogConfig: "type.googleapis.com/compute.v1.FirewallLogConfig",
		"allowed": "type.googleapis.com/compute.v1.Firewall.Allowed", "denied": "type.googleapis.com/compute.v1.Firewall.Denied",
	},
}

// unmodeledValue names a modeled field set to a value the provider can't honor.
func (b *firewallBody) unmodeledValue() string {
	if b.LogConfig != nil && bool(b.LogConfig.Enable) {
		return "logConfig.enable"
	}
	return ""
}

// routeBody is a routes.insert body.
type routeBody struct {
	Name           string      `json:"name"`
	Description    string      `json:"description"`
	Network        string      `json:"network"`
	DestRange      string      `json:"destRange"`
	NextHopGateway string      `json:"nextHopGateway"`
	NextHopIP      string      `json:"nextHopIp"`
	Priority       *flexInt    `json:"priority"`
	Tags           flexStrings `json:"tags"`
}

var routeSchema = bodySchema{
	unmodeled: map[string][]string{"": {"nextHopIlb", "nextHopInstance", "nextHopNetwork", "nextHopVpnTunnel", keyParams}},
	types:     map[string]string{"priority": "TYPE_UINT32"},
}

// unmodeledValue: every modeled route field value is modeled.
func (*routeBody) unmodeledValue() string { return "" }

// routerBody is a routers.insert body; region is output only and ignored.
type routerBody struct {
	Name        string    `json:"name"`
	Description string    `json:"description"`
	Network     string    `json:"network"`
	NATs        []natBody `json:"nats"`
}

// natBody is one Cloud NAT gateway of a router.
type natBody struct {
	Name        string          `json:"name"`
	Allocation  string          `json:"natIpAllocateOption"`
	Ranges      string          `json:"sourceSubnetworkIpRangesToNat"`
	Subnetworks []natSubnetBody `json:"subnetworks"`
}

// natSubnetBody names one subnetwork a LIST_OF_SUBNETWORKS gateway covers.
type natSubnetBody struct {
	Name   string      `json:"name"`
	Ranges flexStrings `json:"sourceIpRangesToNat"`
}

var routerSchema = bodySchema{
	unmodeled: map[string][]string{
		"": {"bgp", "bgpPeers", "encryptedInterconnectRouter", "interfaces", "md5AuthenticationKeys", "nccGateway", keyParams},
		keyNATs: {"autoNetworkTier", "drainNatIps", "enableDynamicPortAllocation", "enableEndpointIndependentMapping",
			"endpointTypes", "icmpIdleTimeoutSec", keyLogConfig, "maxPortsPerVm", "minPortsPerVm", "nat64Subnetworks",
			"natIps", "rules", "sourceSubnetworkIpRangesToNat64", "tcpEstablishedIdleTimeoutSec", "tcpTimeWaitTimeoutSec",
			"tcpTransitoryIdleTimeoutSec", "type", "udpIdleTimeoutSec"},
		"nats.subnetworks": {"secondaryIpRangeNames"},
	},
	types: map[string]string{keyNATs: "type.googleapis.com/compute.v1.RouterNat"},
}

// unmodeledValue names a modeled field set to a value the provider can't honor.
func (b *routerBody) unmodeledValue() string {
	for _, nat := range b.NATs {
		for _, subnet := range nat.Subnetworks {
			if len(subnet.Ranges) > 0 && !slices.Equal(subnet.Ranges, []string{"ALL_IP_RANGES"}) {
				return "nats.subnetworks.sourceIpRangesToNat"
			}
		}
	}
	return ""
}

// instanceBody is an instances.insert body.
type instanceBody struct {
	Name              string            `json:"name"`
	Description       string            `json:"description"`
	MachineType       string            `json:"machineType"`
	Disks             []diskBody        `json:"disks"`
	NetworkInterfaces []nicBody         `json:"networkInterfaces"`
	ServiceAccounts   []accountBody     `json:"serviceAccounts"`
	Labels            map[string]string `json:"labels"`
	Tags              *tagsBody         `json:"tags"`
	CanIPForward      flexBool          `json:"canIpForward"`
	Protected         flexBool          `json:"deletionProtection"`
}

// diskBody is one attached disk; only a new boot disk is modeled.
type diskBody struct {
	Boot       flexBool        `json:"boot"`
	AutoDelete *flexBool       `json:"autoDelete"`
	Type       string          `json:"type"`
	Mode       string          `json:"mode"`
	Interface  string          `json:"interface"`
	Initialize *initializeBody `json:"initializeParams"`
}

// initializeBody creates the boot disk; the disk itself isn't a resource.
type initializeBody struct {
	SourceImage string  `json:"sourceImage"`
	DiskSizeGB  flexInt `json:"diskSizeGb"`
	DiskType    string  `json:"diskType"`
}

// nicBody is one network interface; only a primary internal address is modeled.
type nicBody struct {
	Network    string `json:"network"`
	Subnetwork string `json:"subnetwork"`
	NetworkIP  string `json:"networkIP"`
	StackType  string `json:"stackType"`
}

// accountBody is the service account an instance runs as.
type accountBody struct {
	Email  string      `json:"email"`
	Scopes flexStrings `json:"scopes"`
}

// tagsBody holds an instance's network tags.
type tagsBody struct {
	Items flexStrings `json:"items"`
}

var instanceSchema = bodySchema{unmodeled: map[string][]string{
	"": {"advancedMachineFeatures", "confidentialInstanceConfig", "displayDevice", "guestAccelerators", "hostname",
		"instanceEncryptionKey", "keyRevocationActionType", "localSsdEncryptionMode", keyMetadata, "minCpuPlatform",
		"networkPerformanceConfig", keyParams, keyPrivateIPv6, "reservationAffinity", "resourcePolicies",
		"scheduling", "shieldedInstanceConfig", "shieldedInstanceIntegrityPolicy", "sourceMachineImage",
		"sourceMachineImageEncryptionKey", "workloadIdentityConfig"},
	"disks": {"deviceName", "diskEncryptionKey", "diskSizeGb", "forceAttach", "guestOsFeatures", "source"},
	"disks.initializeParams": {"architecture", "description", "diskName", "enableConfidentialCompute", "labels",
		"licenses", "onUpdateAction", "provisionedIops", "provisionedThroughput", "replicaZones",
		"resourceManagerTags", "resourcePolicies", "sourceImageEncryptionKey", "sourceSnapshot",
		"sourceSnapshotEncryptionKey", "storagePool"},
	"networkInterfaces": {"accessConfigs", "aliasIpRanges", "aliasIpv6Ranges", "enableVpcScopedDns", "igmpQuery",
		"internalIpv6PrefixLength", "ipv6AccessConfigs", "ipv6Address", "networkAttachment", "nicType",
		"parentNicName", "queueCount", "serviceClassId", "vlan"},
}, types: map[string]string{
	"labels": "type.googleapis.com/compute.v1.Instance.LabelsEntry", "tags": "type.googleapis.com/compute.v1.Tags",
	"disks.initializeParams.diskSizeGb": "TYPE_INT64",
}}

// unmodeledValue names a modeled field set to a value the provider can't honor.
func (b *instanceBody) unmodeledValue() string {
	if field := firstTrue(
		named{"canIpForward", bool(b.CanIPForward)},
		named{"deletionProtection", bool(b.Protected)},
		named{"disks (a second disk)", len(b.Disks) > 1},
		named{"networkInterfaces (a second interface)", len(b.NetworkInterfaces) > 1},
		named{"serviceAccounts (a second account)", len(b.ServiceAccounts) > 1},
	); field != "" {
		return field
	}
	for _, disk := range b.Disks {
		if disk.unmodeled() {
			return "disks (a disk other than an auto-deleted read-write persistent SCSI disk)"
		}
	}
	for _, nic := range b.NetworkInterfaces {
		if nic.StackType != "" && nic.StackType != stackIPv4Only {
			return "networkInterfaces.stackType"
		}
	}
	return ""
}

// unmodeled reports whether the disk is anything but the rendered boot disk.
func (disk diskBody) unmodeled() bool {
	return (disk.Type != "" && disk.Type != "PERSISTENT") || (disk.Mode != "" && disk.Mode != "READ_WRITE") ||
		(disk.Interface != "" && disk.Interface != "SCSI") || (disk.AutoDelete != nil && !bool(*disk.AutoDelete))
}

// peeringBody is a networks.addPeering body: the current networkPeering form
// or the legacy top-level name, peerNetwork, and autoCreateRoutes.
type peeringBody struct {
	Name             string           `json:"name"`
	PeerNetwork      string           `json:"peerNetwork"`
	AutoCreateRoutes *flexBool        `json:"autoCreateRoutes"`
	NetworkPeering   *networkPeerBody `json:"networkPeering"`
}

// networkPeerBody is the networkPeering of an addPeering body.
type networkPeerBody struct {
	Name                 string    `json:"name"`
	Network              string    `json:"network"`
	ExchangeSubnetRoutes *flexBool `json:"exchangeSubnetRoutes"`
	ImportCustomRoutes   flexBool  `json:"importCustomRoutes"`
	ExportCustomRoutes   flexBool  `json:"exportCustomRoutes"`
	ExportPublicIP       *flexBool `json:"exportSubnetRoutesWithPublicIp"`
	ImportPublicIP       flexBool  `json:"importSubnetRoutesWithPublicIp"`
	StackType            string    `json:"stackType"`
	UpdateStrategy       string    `json:"updateStrategy"`
}

var peeringSchema = bodySchema{types: map[string]string{
	"networkPeering": "type.googleapis.com/compute.v1.NetworkPeering",
}}

// unmodeledValue names a modeled field set to a value the provider can't honor.
func (b *peeringBody) unmodeledValue() string {
	if peer := b.NetworkPeering; peer != nil {
		return firstTrue(
			named{"networkPeering.stackType", peer.StackType != "" && peer.StackType != stackIPv4Only},
			named{"networkPeering.updateStrategy", peer.UpdateStrategy != "" && peer.UpdateStrategy != strategyIndependent},
		)
	}
	return ""
}

// removePeeringBody is a networks.removePeering body.
type removePeeringBody struct {
	Name string `json:"name"`
}

// unmodeledValue: the one field is modeled.
func (*removePeeringBody) unmodeledValue() string { return "" }

// set reports whether a decoded field carries a value other than JSON null or
// an empty string, list, or object, false, or zero: the defaults a client may
// echo without asking for unmodeled behavior.
func set(value any) bool {
	switch value := value.(type) {
	case nil:
		return false
	case string:
		return value != ""
	case bool:
		return value
	case float64:
		return value != 0
	case []any:
		return len(value) > 0
	case map[string]any:
		return len(value) > 0
	default:
		return true
	}
}

// unmodeledIn names the first unmodeled field set in a decoded body, walking
// each path of the table through objects and lists.
func unmodeledIn(body any, table map[string][]string) string {
	for _, path := range sortedKeys(table) {
		for _, object := range objectsAt(body, path) {
			for _, field := range table[path] {
				if set(object[field]) {
					return strings.TrimPrefix(path+"."+field, ".")
				}
			}
		}
	}
	return ""
}

// objectsAt collects the JSON objects at a dotted path, flattening lists.
func objectsAt(value any, path string) []map[string]any {
	if list, ok := value.([]any); ok {
		objects := make([]map[string]any, 0, len(list))
		for _, item := range list {
			objects = append(objects, objectsAt(item, path)...)
		}
		return objects
	}
	object, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	if path == "" {
		return []map[string]any{object}
	}
	head, rest, _ := strings.Cut(path, ".")
	return objectsAt(object[head], rest)
}

// flexInt is an integer field. Like Compute, it accepts a JSON number or a
// numeric string (int64 fields are strings on the wire).
type flexInt int64

// UnmarshalJSON accepts an integral number or a numeric string.
func (value *flexInt) UnmarshalJSON(raw []byte) error {
	if string(raw) == jsonNull {
		return nil
	}
	text := strings.Trim(string(raw), `"`)
	number, err := strconv.ParseInt(text, 10, 64)
	if err != nil {
		return &json.UnmarshalTypeError{Value: string(raw), Type: reflect.TypeFor[int64]()}
	}
	*value = flexInt(number)
	return nil
}

// flexBool is a boolean field. Compute reads some strings as booleans
// (recorded: "yes" disables a firewall, "no" and "false" don't); other
// non-boolean values are a gap, since what Compute makes of them is unknown.
type flexBool bool

// UnmarshalJSON accepts true, false, and the recorded string forms.
func (value *flexBool) UnmarshalJSON(raw []byte) error {
	switch string(raw) {
	case "true", `"true"`, `"yes"`:
		*value = true
	case "false", `"false"`, `"no"`, jsonNull:
		*value = false
	default:
		return gap("a boolean field given " + string(raw) + " is not modeled")
	}
	return nil
}

// flexStrings is a repeated string field. Compute reads a single string as a
// one-item list and ignores an object (recorded: targetServiceAccounts and
// sourceRanges); other values are a gap.
type flexStrings []string

// UnmarshalJSON accepts a list of strings, one string, or an ignored object.
func (value *flexStrings) UnmarshalJSON(raw []byte) error {
	var one string
	var list []string
	var object map[string]any
	switch {
	case string(raw) == jsonNull || json.Unmarshal(raw, &object) == nil:
		return nil
	case json.Unmarshal(raw, &one) == nil:
		*value = flexStrings{one}
	case json.Unmarshal(raw, &list) == nil:
		*value = list
	default:
		return gap("a repeated string field given " + string(raw) + " is not modeled")
	}
	return nil
}

// protocolName is a firewall term's IPProtocol. Compute names the protocol a
// number gives (recorded: 6 is stored as tcp); other numbers are a gap.
type protocolName string

// UnmarshalJSON accepts a protocol name or the recorded protocol number.
func (value *protocolName) UnmarshalJSON(raw []byte) error {
	var name string
	switch {
	case string(raw) == "6":
		*value = "tcp"
	case json.Unmarshal(raw, &name) == nil:
		*value = protocolName(name)
	default:
		return gap("an IPProtocol given " + string(raw) + " is not modeled")
	}
	return nil
}
