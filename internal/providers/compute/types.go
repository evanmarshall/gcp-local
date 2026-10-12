// Package compute implements the first networking control-plane resources:
// networks, subnetworks, firewalls, routes, and their long-running operations.
// The surface follows the recorded v1 REST contracts; broader topology (routers,
// NAT, peering, PSA/PSC) stays an explicit gap rather than an approximation.
package compute

import (
	"encoding/json"
	"net/url"
	"regexp"
	"strconv"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
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
	keyItems       = "items"
	keyName        = "name"
	keySelfLink    = "selfLink"
	keyTimestamp   = "creationTimestamp"
	keyKind        = "kind"
	keyNetwork     = "network"
	keyIPProto     = "IPProtocol"
	keyPeerNetwork = "peerNetwork"
	keyDestRange   = "destRange"
	keyError       = "error"
	keyMessage     = "message"
	keyDescription = "description"
	keyCode        = "code"
	keyPorts       = "ports"
	keyAutoSubnets = "autoCreateSubnetworks"
	keyDestIP      = "destIp"
	keyPort        = "port"
	keyIPCIDR      = "ipCidrRange"
	keyProject     = "project"
	keySourceIP    = "sourceIp"
	keySource      = "source"
	keyDestination = "destination"
	keyProtocol    = "protocol"
	keyState       = "state"
	keyCause       = "cause"
	keyCausesDrop  = "causesDrop"
	keyRegion      = "region"
	keyDirection   = "direction"
	keyPriority    = "priority"
	keyNetworkURI  = "networkUri"
	stackIPv4Only  = "IPV4_ONLY"
	keyStackType   = "stackType"
	keyDisplayName = "displayName"
	keyProjectID   = "projectId"
	keyFingerprint = "fingerprint"
	keyURI         = "uri"
	keyDestIPRange = "destIpRange"
	keyNextHop     = "nextHop"
	keyIPAddress   = "ipAddress"
	keyRouteType   = "routeType"
	keyNextHopType = "nextHopType"
)

// wireHost is the self-link host the recorded compute documents carry. Compute
// answers on compute.googleapis.com but links resources under www.googleapis.com.
const wireHost = "https://www.googleapis.com/compute/v1"

// scopeOf builds the location scope of one compute resource path.
func scopeOf(project, location string) state.Scope {
	return state.Scope{Project: project, Location: location}
}

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
func gap(message string) error {
	return status.Error(codes.Unimplemented, "Compute coverage gap: "+message)
}

// wireError builds the compute error body.
func wireError(code int, reason, message string) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: code, JSON: map[string]any{
		keyError: map[string]any{
			keyCode: code, keyMessage: message,
			"errors": []any{map[string]any{keyCode: code, "reason": reason, keyMessage: message}},
		},
	}}
}

// statusInvalid reports a 400 for one malformed request field.
func statusInvalid(message string) error {
	return status.Error(codes.InvalidArgument, message)
}

// notFound reports the compute not-found status for one resource type.
func notFound(kind, name string) error {
	return status.Errorf(codes.NotFound, "The resource '%s/%s' was not found", kind, name)
}

// conflict reports the already-exists status for one resource type.
func conflict(kind, name string) error {
	return status.Errorf(codes.AlreadyExists, "The resource '%s/%s' already exists", kind, name)
}

// missingReference reports a resource that names a nonexistent dependency.
func missingReference() error {
	return status.Error(codes.InvalidArgument, "The referenced network does not exist")
}

// stamp renders the compute timestamp shape.
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

// decodeBody decodes one request body into a generic map without unknown fields.
func decodeBody(body []byte) (map[string]any, error) {
	var value map[string]any
	if json.Unmarshal(body, &value) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid request body")
	}
	return value, nil
}
