// Package memorystore serves the Memorystore for Redis v1 control plane in
// front of one real Redis server per instance. The provider authenticates and
// authorizes every call with the shared IAM engine before any backend effect.
// It owns instance metadata, AUTH strings, and operations; each Redis process
// owns its keyspace. The data plane is the Redis protocol on a loopback port,
// not the HTTP gateway: like Memorystore, it enforces only the optional AUTH
// string per connection, never IAM.
package memorystore

import (
	"errors"
	"fmt"
	"net/http"
	"regexp"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName owns the backend registration and the index entries.
const serviceName = "redis"

// Method name constants; the generated route table uses the same identifiers.
const (
	methodCreate        = "redis.projects.locations.instances.create"
	methodGet           = "redis.projects.locations.instances.get"
	methodList          = "redis.projects.locations.instances.list"
	methodPatch         = "redis.projects.locations.instances.patch"
	methodDelete        = "redis.projects.locations.instances.delete"
	methodAuthString    = "redis.projects.locations.instances.getAuthString"
	methodOperationGet  = "redis.projects.locations.operations.get"
	methodOperationList = "redis.projects.locations.operations.list"
	methodOperationStop = "redis.projects.locations.operations.cancel"
	methodOperationDrop = "redis.projects.locations.operations.delete"
)

// permissions follows the documented Memorystore for Redis access-control rows.
// Memorystore has no instance-level IAM policies; every check is on the project.
var permissions = map[string]string{
	methodCreate:        "redis.instances.create",
	methodGet:           "redis.instances.get",
	methodList:          "redis.instances.list",
	methodPatch:         "redis.instances.update",
	methodDelete:        "redis.instances.delete",
	methodAuthString:    "redis.instances.getAuthString",
	methodOperationGet:  "redis.operations.get",
	methodOperationList: "redis.operations.list",
	methodOperationStop: "redis.operations.cancel",
	methodOperationDrop: "redis.operations.delete",
}

// Path variables bound by the generated routes.
const (
	pathProject   = "project"
	pathLocation  = "location"
	pathInstance  = "instance"
	pathOperation = "operation"
)

// Supported enum values and service defaults.
const (
	tierBasic          = "BASIC"
	tierStandard       = "STANDARD_HA"
	redisVersion72     = "REDIS_7_2"
	transitDisabled    = "DISABLED"
	transitServerAuth  = "SERVER_AUTHENTICATION"
	connectPeering     = "DIRECT_PEERING"
	connectPSA         = "PRIVATE_SERVICE_ACCESS"
	stateReady         = "READY"
	loopbackHost       = "127.0.0.1"
	maxBasicMemoryGB   = 300
	maxPageSize        = 1000
	operationType      = "type.googleapis.com/google.cloud.redis.v1.OperationMetadata"
	instanceType       = "type.googleapis.com/google.cloud.redis.v1.Instance"
	emptyType          = "type.googleapis.com/google.protobuf.Empty"
	verbCreate         = "create"
	verbUpdate         = "update"
	verbDelete         = "delete"
	defaultNetworkPath = "/global/networks/default"
)

// Name shapes from the Memorystore API reference.
var (
	instanceID   = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,38}[a-z0-9])?$`)
	regionName   = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)
	networkName  = regexp.MustCompile(`^projects/[a-z0-9-]+/global/networks/[a-z0-9-]+$`)
	labelPattern = regexp.MustCompile(`^[\p{Ll}\p{Lo}0-9_-]{0,63}$`)
)

// errProviderShape rejects an incomplete provider construction.
var errProviderShape = errors.New("memorystore requires registry, manager, index, and an authorizer")

// validScope accepts only region locations; Memorystore has no global scope.
func validScope(scope state.Scope) error {
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("memorystore scope: %w", err)
	}
	if !regionName.MatchString(scope.Location) {
		return errRegion
	}
	return nil
}

// errRegion reports a location that is not a region name.
var errRegion = errors.New("memorystore locations are regions")

func instanceName(scope state.Scope, instance string) string {
	return "projects/" + scope.Project + "/locations/" + scope.Location + "/instances/" + instance
}

// fullName prefixes a canonical Memorystore name for the shared index.
func fullName(name string) string {
	return "//redis.googleapis.com/" + name
}

// invalid reports a request the service would reject as malformed.
func invalid(format string, args ...any) error {
	return status.Errorf(codes.InvalidArgument, format, args...)
}

// notFound uses Memorystore's full-name text, not the shared kind/name form.
func notFound(name string) error {
	return status.Errorf(codes.NotFound, "Resource '%s' was not found", name)
}

// gap reports an unsupported request shape explicitly.
var gap = provider.CoverageGap("Memorystore")

// ok wraps a JSON response body.
func ok(body any) *gateway.RESTResponse {
	return &gateway.RESTResponse{Status: http.StatusOK, JSON: body}
}
