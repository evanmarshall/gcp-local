package cloudsql

import (
	"crypto/sha256"
	"encoding/hex"
	"maps"
	"strconv"
	"time"
)

// maxOperations bounds the retained operation history per project.
const maxOperations = 1000

// instanceRecord is the provider-owned instance metadata. The server itself,
// its databases, and its users are owned by the backend.
type instanceRecord struct {
	Name, Region, Version, Tier, Edition, PrivateNetwork string
	Labels                                               map[string]string
	DeletionProtection, IPv4                             bool
	SettingsVersion                                      int64
	Created                                              time.Time
}

// operationRecord is one completed sql#operation.
type operationRecord struct {
	Name, Type, Instance, User string
	Inserted, Ended            time.Time
}

// catalog is one project's provider state.
type catalog struct {
	instances  map[string]instanceRecord
	operations []operationRecord
}

func newCatalog() catalog { return catalog{instances: map[string]instanceRecord{}} }

// clone copies a record so callers never share the stored label map.
func (r instanceRecord) clone() instanceRecord {
	r.Labels = maps.Clone(r.Labels)
	return r
}

// record appends a completed operation, dropping the oldest beyond the bound.
func (c *catalog) record(operation operationRecord) {
	c.operations = append(c.operations, operation)
	if len(c.operations) > maxOperations {
		c.operations = append([]operationRecord(nil), c.operations[len(c.operations)-maxOperations:]...)
	}
}

// newOperation builds a completed operation. Every supported mutation runs to
// completion before its response, so operations are returned DONE.
func newOperation(kind, instance, user string, started, ended time.Time) operationRecord {
	return operationRecord{Name: operationID(), Type: kind, Instance: instance, User: user, Inserted: started, Ended: ended}
}

// wire renders the sql#operation resource.
func (o operationRecord) wire(project string) map[string]any {
	inserted, ended := o.Inserted.UTC().Format(time.RFC3339Nano), o.Ended.UTC().Format(time.RFC3339Nano)
	value := map[string]any{
		keyKind: "sql#operation", keyName: o.Name, "operationType": o.Type, "status": statusDone,
		"insertTime": inserted, "startTime": inserted, "endTime": ended,
		"targetId": o.Instance, "targetProject": project, "targetLink": instanceLink(project, o.Instance),
		keySelfLink: apiBase + "projects/" + project + "/operations/" + o.Name,
	}
	if o.User != "" {
		value["user"] = o.User
	}
	return value
}

// wire renders the sql#instance resource from provider metadata.
func (r instanceRecord) wire(project string, release string) map[string]any {
	ipType := "PRIMARY"
	if !r.IPv4 {
		ipType = "PRIVATE"
	}
	ipConfiguration := map[string]any{"ipv4Enabled": r.IPv4}
	if r.PrivateNetwork != "" {
		ipConfiguration["privateNetwork"] = r.PrivateNetwork
	}
	settings := map[string]any{
		keyKind: "sql#settings", "tier": r.Tier, keySettingsVersion: strconv.FormatInt(r.SettingsVersion, 10),
		keyDeletionProtection: r.DeletionProtection, "activationPolicy": "ALWAYS", "availabilityType": "ZONAL",
		"pricingPlan": "PER_USE", "ipConfiguration": ipConfiguration,
	}
	if len(r.Labels) > 0 {
		settings["userLabels"] = maps.Clone(r.Labels)
	}
	if r.Edition != "" {
		settings["edition"] = r.Edition
	}
	value := map[string]any{
		keyKind: "sql#instance", keyName: r.Name, pathProject: project, "region": r.Region, "databaseVersion": r.Version,
		"state": statusRunnable, "backendType": "SECOND_GEN", "instanceType": "CLOUD_SQL_INSTANCE",
		"connectionName": project + ":" + r.Region + ":" + r.Name, keySelfLink: instanceLink(project, r.Name),
		"createTime": r.Created.UTC().Format(time.RFC3339Nano), "etag": r.etag(),
		"ipAddresses": []any{map[string]any{"type": ipType, "ipAddress": loopbackIPv4},
			map[string]any{"type": "OUTGOING", "ipAddress": loopbackIPv4}}, keySettings: settings,
		// Recorded (cloudsql-postgres-instance-admin): GCP always returns these
		// service-managed fields; the values are local stand-ins, only presence is compared.
		"gceZone": r.Region + "-a", "geminiConfig": map[string]any{"entitled": false},
		"includeReplicasForMajorVersionUpgrade": false, "maintenanceVersion": r.Version + "_LOCAL",
		"satisfiesPzi": false, "serverCaCert": map[string]any{"kind": "sql#sslCert", pathInstance: r.Name},
		"serviceAccountEmailAddress": "p-local@gcp-sa-cloud-sql.iam.gserviceaccount.com",
		"sqlNetworkArchitecture":     "NEW_NETWORK_ARCHITECTURE", "upgradableDatabaseVersions": []any{},
	}
	if release != "" {
		value["databaseInstalledVersion"] = "POSTGRES_" + release
	}
	return value
}

// etag changes whenever the settings version changes.
func (r instanceRecord) etag() string {
	sum := sha256.Sum256([]byte(r.Name + "/" + strconv.FormatInt(r.SettingsVersion, 10) + "/" + r.Created.String()))
	return hex.EncodeToString(sum[:16])
}

// operationID renders a random RFC 4122 version-4 identifier, the shape of
// Cloud SQL operation names.
func operationID() string {
	raw := randomBytes(16)
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	text := hex.EncodeToString(raw)
	return text[:8] + "-" + text[8:12] + "-" + text[12:16] + "-" + text[16:20] + "-" + text[20:]
}
