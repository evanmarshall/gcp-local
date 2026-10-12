package memorystore

import (
	"bytes"
	"encoding/json"
	"fmt"
	"maps"
	"regexp"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// Instance field names shared by rendering, decoding, and update masks.
const (
	fieldDisplayName = "displayName"
	fieldLabels      = "labels"
	fieldMemory      = "memorySizeGb"
	fieldVersion     = "redisVersion"
	fieldTier        = "tier"
)

// instance is the provider-owned metadata of one Redis instance. The Redis
// process owns the keyspace; this record owns everything the API reports.
type instance struct {
	ID, DisplayName, LocationID, RedisVersion, Tier string
	TransitEncryptionMode, ConnectMode, Network     string
	AuthString                                      string
	Labels                                          map[string]string
	MemorySizeGB                                    int64
	Port                                            int
	AuthEnabled                                     bool
	Created                                         time.Time
}

// clone copies the record so store callbacks never leak mutable maps.
func (i instance) clone() instance {
	i.Labels = maps.Clone(i.Labels)
	return i
}

// wire renders the record as a v1 Instance; unset fields are omitted as proto3 JSON does.
func (i instance) wire(scope state.Scope) map[string]any {
	body := map[string]any{
		"name":                  instanceName(scope, i.ID),
		"locationId":            i.LocationID,
		"currentLocationId":     i.LocationID,
		fieldVersion:            i.RedisVersion,
		fieldTier:               i.Tier,
		fieldMemory:             i.MemorySizeGB,
		"host":                  loopbackHost,
		"port":                  i.Port,
		"state":                 stateReady,
		"createTime":            i.Created.Format(time.RFC3339Nano),
		"authorizedNetwork":     i.Network,
		"connectMode":           i.ConnectMode,
		"transitEncryptionMode": i.TransitEncryptionMode,
		// Recorded (memorystore-redis-instance-admin): GCP always returns these
		// service-managed fields; the values are local stand-ins, only presence is compared.
		"maintenanceVersion":     "20260801_00_00",
		"nodes":                  []any{map[string]any{"id": "node-0", "zone": i.LocationID}},
		"persistenceConfig":      map[string]any{"persistenceMode": "DISABLED"},
		"persistenceIamIdentity": "serviceAccount:service-" + scope.Project + "@cloud-redis.iam.gserviceaccount.com",
		"readReplicasMode":       "READ_REPLICAS_DISABLED",
		"reservedIpRange":        "127.0.0.0/29",
	}
	if i.DisplayName != "" {
		body[fieldDisplayName] = i.DisplayName
	}
	if len(i.Labels) > 0 {
		body[fieldLabels] = maps.Clone(i.Labels)
	}
	if i.AuthEnabled {
		body["authEnabled"] = true
	}
	return body
}

// instanceBody is the writable subset of the v1 Instance the provider models.
type instanceBody struct {
	DisplayName           string            `json:"displayName"`
	Labels                map[string]string `json:"labels"`
	LocationID            string            `json:"locationId"`
	RedisVersion          string            `json:"redisVersion"`
	Tier                  string            `json:"tier"`
	MemorySizeGB          *int64            `json:"memorySizeGb"`
	AuthEnabled           bool              `json:"authEnabled"`
	TransitEncryptionMode string            `json:"transitEncryptionMode"`
	ConnectMode           string            `json:"connectMode"`
	AuthorizedNetwork     string            `json:"authorizedNetwork"`
}

// Field classes of the v1 Instance schema in the pinned discovery document.
var (
	modeledFields = set(fieldDisplayName, fieldLabels, "locationId", fieldVersion, fieldTier, fieldMemory,
		"authEnabled", "transitEncryptionMode", "connectMode", "authorizedNetwork")
	// Output-only fields are ignored on input, as the service does.
	outputFields = set("name", "host", "port", "state", "createTime", "currentLocationId", "statusMessage",
		"nodes", "readEndpoint", "readEndpointPort", "persistenceIamIdentity", "serverCaCerts",
		"maintenanceSchedule", "satisfiesPzs", "satisfiesPzi", "availableMaintenanceVersions")
	// Writable fields this slice does not model: an empty value is accepted, anything else is a gap.
	unmodeledFields = set("alternativeLocationId", "customerManagedKey", "maintenancePolicy", "maintenanceVersion",
		"persistenceConfig", "readReplicasMode", "redisConfigs", "replicaCount", "reservedIpRange",
		"secondaryIpRange", "suspensionReasons", "tags")
)

func set(values ...string) map[string]bool {
	result := make(map[string]bool, len(values))
	for _, value := range values {
		result[value] = true
	}
	return result
}

// emptyJSON reports a JSON value that leaves a field at its default.
func emptyJSON(raw json.RawMessage) bool {
	switch string(bytes.TrimSpace(raw)) {
	case "null", `""`, "{}", "[]", "0", "false":
		return true
	default:
		return false
	}
}

// decodeInstance checks every field class, then decodes the modeled subset.
func decodeInstance(raw json.RawMessage) (instanceBody, error) {
	var fields map[string]json.RawMessage
	if len(bytes.TrimSpace(raw)) == 0 {
		raw = json.RawMessage("{}")
	}
	if err := json.Unmarshal(raw, &fields); err != nil || fields == nil {
		return instanceBody{}, invalid("Invalid JSON payload received: the body must be an Instance object")
	}
	for name, value := range fields {
		switch {
		case modeledFields[name], outputFields[name]:
		case unmodeledFields[name]:
			if !emptyJSON(value) {
				return instanceBody{}, gap("Instance field %s is not modeled", name)
			}
		default:
			return instanceBody{}, invalid("Invalid JSON payload received. Unknown name %q: Cannot find field.", name)
		}
	}
	var body instanceBody
	if err := json.Unmarshal(raw, &body); err != nil {
		return instanceBody{}, invalid("Invalid JSON payload received: %v", err)
	}
	return body, nil
}

// newInstance validates a create request into a record (without port or time).
func newInstance(scope state.Scope, id string, body instanceBody) (instance, error) {
	if !instanceID.MatchString(id) {
		return instance{}, invalid("instanceId must be 1 to 40 lowercase letters, digits, or hyphens, start with a letter, and end with a letter or digit")
	}
	record := instance{ID: id, DisplayName: body.DisplayName, AuthEnabled: body.AuthEnabled}
	if err := capacity(&record, body); err != nil {
		return instance{}, err
	}
	if err := placement(&record, scope, body); err != nil {
		return instance{}, err
	}
	return record, nil
}

// capacity validates the service tier, size, version, and transport fields.
func capacity(record *instance, body instanceBody) error {
	var err error
	if record.Tier, err = tier(body.Tier); err != nil {
		return err
	}
	if record.MemorySizeGB, err = memory(body.MemorySizeGB); err != nil {
		return err
	}
	if record.RedisVersion, err = version(body.RedisVersion); err != nil {
		return err
	}
	if record.TransitEncryptionMode, err = choose("transitEncryptionMode", body.TransitEncryptionMode, transitDisabled, transitServerAuth); err != nil {
		return err
	}
	record.ConnectMode, err = choose("connectMode", body.ConnectMode, connectPeering, connectPSA)
	return err
}

// placement validates the zone, network, and labels.
func placement(record *instance, scope state.Scope, body instanceBody) error {
	var err error
	if record.LocationID, err = zone(scope, body.LocationID); err != nil {
		return err
	}
	if record.Network, err = network(scope, body.AuthorizedNetwork); err != nil {
		return err
	}
	record.Labels, err = labels(body.Labels)
	return err
}

// tier accepts BASIC; Standard Tier replication is a coverage gap.
func tier(value string) (string, error) {
	switch value {
	case tierBasic:
		return value, nil
	case tierStandard:
		return "", gap("STANDARD_HA replicas and failover are not modeled")
	case "", "TIER_UNSPECIFIED":
		return "", invalid("tier is required")
	default:
		return "", invalid("Invalid value for tier: %q", value)
	}
}

// memory validates the Basic Tier capacity range in GB.
func memory(value *int64) (int64, error) {
	if value == nil || *value < 1 || *value > maxBasicMemoryGB {
		return 0, invalid("memorySizeGb must be between 1 and %d for a BASIC instance", maxBasicMemoryGB)
	}
	return *value, nil
}

// knownVersions lists the documented redisVersion values other than the pinned one.
var knownVersions = set("REDIS_3_2", "REDIS_4_0", "REDIS_5_0", "REDIS_6_X", "REDIS_7_0")

// version accepts the pinned REDIS_7_2 backend version (also the local default).
func version(value string) (string, error) {
	switch {
	case value == "" || value == redisVersion72:
		return redisVersion72, nil
	case knownVersions[value]:
		return "", gap("redisVersion %s; the pinned backend is Redis %s (REDIS_7_2)", value, ServerVersion)
	default:
		return "", invalid("Invalid value for redisVersion: %q", value)
	}
}

// zoneName is one zone in a region.
var zoneName = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+-[a-z]$`)

// zone defaults the primary zone to the region's "-a" zone.
func zone(scope state.Scope, value string) (string, error) {
	if value == "" {
		return scope.Location + "-a", nil
	}
	if !zoneName.MatchString(value) || !strings.HasPrefix(value, scope.Location+"-") {
		return "", invalid("locationId %q is not a zone in %s", value, scope.Location)
	}
	return value, nil
}

// network records the authorized VPC name; reachability is not modeled.
func network(scope state.Scope, value string) (string, error) {
	if value == "" {
		return "projects/" + scope.Project + defaultNetworkPath, nil
	}
	if !networkName.MatchString(value) {
		return "", invalid("authorizedNetwork must be projects/{project}/global/networks/{network}")
	}
	return value, nil
}

// labelKey is the documented label key shape.
var labelKey = regexp.MustCompile(`^[\p{Ll}\p{Lo}][\p{Ll}\p{Lo}0-9_-]{0,62}$`)

// labels validates and copies user labels.
func labels(values map[string]string) (map[string]string, error) {
	if len(values) > 64 {
		return nil, invalid("An instance can have at most 64 labels")
	}
	for key, value := range values {
		if !labelKey.MatchString(key) || !labelPattern.MatchString(value) {
			return nil, invalid("Invalid label %q", key)
		}
	}
	return maps.Clone(values), nil
}

// choose accepts the modeled default (or unspecified) and reports the other documented value as a gap.
func choose(field, value, modeled, unmodeled string) (string, error) {
	switch {
	case value == "" || value == modeled || strings.HasSuffix(value, "_MODE_UNSPECIFIED"):
		return modeled, nil
	case value == unmodeled:
		return "", gap("%s %s is not modeled", field, unmodeled)
	default:
		return "", invalid("Invalid value for %s: %q", field, value)
	}
}

// updatable maps update-mask paths (camel or snake case) to modeled fields.
var updatable = map[string]string{
	fieldDisplayName: fieldDisplayName, "display_name": fieldDisplayName,
	fieldLabels: fieldLabels,
	fieldMemory: fieldMemory, "memory_size_gb": fieldMemory,
}

// unmodeledPaths are documented update-mask paths this slice does not model.
var unmodeledPaths = set("redisConfig", "redisConfigs", "redis_configs", "replicaCount", "replica_count",
	"readReplicasMode", "read_replicas_mode", "persistenceConfig", "persistence_config",
	"maintenancePolicy", "maintenance_policy", "secondaryIpRange", "secondary_ip_range")

// parseMask resolves a non-empty update mask.
func parseMask(mask string) ([]string, error) {
	if strings.TrimSpace(mask) == "" {
		return nil, invalid("updateMask is required")
	}
	var paths []string
	for path := range strings.SplitSeq(mask, ",") {
		path = strings.TrimSpace(path)
		switch {
		case updatable[path] != "":
			paths = append(paths, updatable[path])
		case unmodeledPaths[path]:
			return nil, gap("update of %s is not modeled", path)
		default:
			return nil, invalid("Unsupported update mask path %q", path)
		}
	}
	return paths, nil
}

// applyPatch returns the updated copy of record for the masked fields.
func applyPatch(record instance, body instanceBody, paths []string) (instance, error) {
	updated := record.clone()
	for _, path := range paths {
		switch path {
		case fieldDisplayName:
			updated.DisplayName = body.DisplayName
		case fieldLabels:
			values, err := labels(body.Labels)
			if err != nil {
				return instance{}, err
			}
			updated.Labels = values
		case fieldMemory:
			size, err := memory(body.MemorySizeGB)
			if err != nil {
				return instance{}, err
			}
			updated.MemorySizeGB = size
		default:
			return instance{}, fmt.Errorf("%w: %s", errMaskPath, path)
		}
	}
	return updated, nil
}

// errMaskPath reports an unresolved mask path (parseMask prevents it).
var errMaskPath = gap("unresolved update mask path")
