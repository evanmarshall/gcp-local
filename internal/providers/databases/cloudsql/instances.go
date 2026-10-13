package cloudsql

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// createTimeout bounds initdb, server start, and bootstrap for one instance.
const createTimeout = 2 * time.Minute

// Request shape checks.
var (
	instanceNamePattern = regexp.MustCompile(`^[a-z]([-a-z0-9]{0,95}[a-z0-9])?$`)
	regionPattern       = regexp.MustCompile(`^[a-z]+-[a-z]+[0-9]+$`)
	tierPattern         = regexp.MustCompile(`^db-[a-z0-9-]+$`)
)

// settingsBody is the Settings subset the provider models. Pointers mark
// the fields a patch may name; the enum fields accept only the value the
// local server has (ZONAL, ALWAYS, PER_USE) and refuse the others as gaps.
type settingsBody struct {
	Kind                      string             `json:"kind"`
	Tier                      *string            `json:"tier"`
	Edition                   *string            `json:"edition"`
	UserLabels                *map[string]string `json:"userLabels"`
	DeletionProtectionEnabled *bool              `json:"deletionProtectionEnabled"`
	SettingsVersion           *string            `json:"settingsVersion"`
	AvailabilityType          *string            `json:"availabilityType"`
	ActivationPolicy          *string            `json:"activationPolicy"`
	PricingPlan               *string            `json:"pricingPlan"`
	BackupConfiguration       *backupBody        `json:"backupConfiguration"`
	IPConfiguration           *ipBody            `json:"ipConfiguration"`
}

// backupBody accepts a backup configuration that enables nothing; #91 models
// backups. Enabling one is a gap, never a stored claim.
type backupBody struct {
	Kind                       string `json:"kind"`
	Enabled                    bool   `json:"enabled"`
	PointInTimeRecoveryEnabled bool   `json:"pointInTimeRecoveryEnabled"`
	BinaryLogEnabled           bool   `json:"binaryLogEnabled"`
}

// ipBody is the IP configuration subset the provider models. requireSsl and
// authorizedNetworks are accepted only empty (Unmodeled); sslMode only as the
// default that requires no TLS.
type ipBody struct {
	IPv4Enabled    *bool  `json:"ipv4Enabled"`
	PrivateNetwork string `json:"privateNetwork"`
	SSLMode        string `json:"sslMode"`
}

// instanceBody is the DatabaseInstance subset the provider models.
type instanceBody struct {
	Kind            string        `json:"kind"`
	Name            string        `json:"name"`
	Project         string        `json:"project"`
	Region          *string       `json:"region"`
	DatabaseVersion *string       `json:"databaseVersion"`
	RootPassword    *string       `json:"rootPassword"`
	BackendType     *string       `json:"backendType"`
	InstanceType    *string       `json:"instanceType"`
	Settings        *settingsBody `json:"settings"`
}

// instanceFields lists the v1beta4 DatabaseInstance fields instanceBody
// lacks (api/discovery/sqladmin-v1beta4.json): unmodeled ones are a gap
// unless empty, output-only ones are dropped. Fields the discovery document
// doesn't mark output-only but the service reports (state, ipAddresses,
// selfLink) are dropped too.
var instanceFields = provider.Fields{
	Unmodeled: []string{"databaseCenterIntegrationEnabled", "diskEncryptionConfiguration", "failoverReplica",
		"includeReplicasForMajorVersionUpgrade", "maintenanceVersion", "masterInstanceName", "nodeCount",
		"onPremisesConfiguration", "replicaConfiguration", "replicaNames", "replicationCluster", "sqlNetworkArchitecture",
		"switchTransactionLogsToCloudStorageEnabled", "tags",
		"settings.acceleratedReplicaMode", "settings.activeDirectoryConfig", "settings.advancedMachineFeatures",
		"settings.authorizedGaeApplications", "settings.autoUpgradeEnabled", "settings.collation",
		"settings.connectionPoolConfig", "settings.connectorEnforcement", "settings.crashSafeReplicationEnabled",
		"settings.dataApiAccess", "settings.dataCacheConfig", "settings.dataDiskProvisionedIops",
		"settings.dataDiskProvisionedThroughput", "settings.dataDiskSizeGb", "settings.dataDiskType",
		"settings.databaseFlags", "settings.databaseReplicationEnabled", "settings.denyMaintenancePeriods",
		"settings.enableDataplexIntegration", "settings.enableGoogleMlIntegration", "settings.entraidConfig",
		"settings.finalBackupConfig", "settings.insightsConfig", "settings.locationPreference",
		"settings.maintenanceWindow", "settings.passwordValidationPolicy", "settings.performanceCaptureConfig",
		"settings.readPoolAutoScaleConfig", "settings.replicationLagMaxSeconds", "settings.replicationType",
		"settings.retainBackupsOnDelete", "settings.sqlServerAuditConfig", "settings.storageAutoResize",
		"settings.storageAutoResizeLimit", "settings.timeZone",
		"settings.backupConfiguration.backupRetentionSettings", "settings.backupConfiguration.location",
		"settings.backupConfiguration.replicationLogArchivingEnabled", "settings.backupConfiguration.startTime",
		"settings.backupConfiguration.transactionLogRetentionDays",
		"settings.ipConfiguration.allocatedIpRange", "settings.ipConfiguration.authorizedNetworks",
		"settings.ipConfiguration.customSubjectAlternativeNames",
		"settings.ipConfiguration.enablePrivatePathForGoogleCloudServices", "settings.ipConfiguration.pscConfig",
		"settings.ipConfiguration.requireSsl", "settings.ipConfiguration.serverCaMode",
		"settings.ipConfiguration.serverCaPool", "settings.ipConfiguration.serverCertificateRotationMode"},
	OutputOnly: []string{"availableMaintenanceVersions", "connectionName", "createTime", "currentDiskSize",
		"databaseInstalledVersion", "deploymentInfo", "diskEncryptionStatus", "dnsName", "dnsNames", keyETag, "gceZone",
		"geminiConfig", "ipAddresses", "ipv6Address", "maxDiskSize", "nodes", "outOfDiskReport", "primaryDnsName",
		"pscServiceAttachmentLink", "satisfiesPzi", "satisfiesPzs", "scheduledMaintenance", "secondaryGceZone",
		"selfLink", "serverCaCert", "serviceAccountEmailAddress", "state", "suspensionReason",
		"upgradableDatabaseVersions", "writeEndpoint",
		"settings.backupConfiguration.backupTier", "settings.backupConfiguration.transactionalLogStorageState"},
	IgnoreUnknown: true,
}

// decodeInstance strictly decodes a DatabaseInstance request body, bound to
// the request's "body" field. Names outside the schema are dropped, as GCP
// does (recorded).
func decodeInstance(raw []byte, project string) (instanceBody, error) {
	var body instanceBody
	if err := provider.DecodeJSON(raw, "body", &body, gap, instanceFields); err != nil {
		return instanceBody{}, err
	}
	if body.Project != "" && body.Project != project {
		return instanceBody{}, provider.Invalid("Invalid project.")
	}
	return body, nil
}

// settingsChoices are the modeled values of the settings enums: the local
// value and the unspecified spelling. Every other documented value is a gap.
var settingsChoices = []struct {
	field             string
	value             func(*settingsBody) *string
	modeled, fallback string
	others            []string
}{
	{"availabilityType", func(s *settingsBody) *string { return s.AvailabilityType }, "ZONAL", "SQL_AVAILABILITY_TYPE_UNSPECIFIED", []string{"REGIONAL"}},
	{"activationPolicy", func(s *settingsBody) *string { return s.ActivationPolicy }, "ALWAYS", "SQL_ACTIVATION_POLICY_UNSPECIFIED", []string{"NEVER", "ON_DEMAND"}},
	{"pricingPlan", func(s *settingsBody) *string { return s.PricingPlan }, "PER_USE", "SQL_PRICING_PLAN_UNSPECIFIED", []string{"PACKAGE"}},
}

// checkSettings refuses settings the local server can't honor: another
// availability type, activation policy, or pricing plan, backups, or TLS
// enforcement. Each refusal names its field.
func checkSettings(settings *settingsBody) error {
	if settings == nil {
		return nil
	}
	for _, choice := range settingsChoices {
		if err := choose(choice.field, choice.value(settings), choice.modeled, choice.fallback, choice.others); err != nil {
			return err
		}
	}
	if backup := settings.BackupConfiguration; backup != nil {
		for field, enabled := range map[string]bool{"enabled": backup.Enabled,
			"pointInTimeRecoveryEnabled": backup.PointInTimeRecoveryEnabled, "binaryLogEnabled": backup.BinaryLogEnabled} {
			if enabled {
				return gap("settings.backupConfiguration." + field + " (backups are not modeled)")
			}
		}
	}
	if ip := settings.IPConfiguration; ip != nil && !slices.Contains([]string{"", "SSL_MODE_UNSPECIFIED", "ALLOW_UNENCRYPTED_AND_ENCRYPTED"}, ip.SSLMode) {
		return gap("settings.ipConfiguration.sslMode " + ip.SSLMode + " (TLS is not served)")
	}
	return nil
}

// choose accepts an unset, modeled, or unspecified enum value, refuses
// another documented one as a gap, and any other as invalid.
func choose(field string, value *string, modeled, fallback string, others []string) error {
	switch {
	case value == nil || *value == "" || *value == modeled || *value == fallback:
		return nil
	case slices.Contains(others, *value):
		return gap(fmt.Sprintf("settings.%s %s", field, *value))
	default:
		return provider.Invalid(fmt.Sprintf("Invalid value for settings.%s: %q", field, *value))
	}
}

// check refuses an insert body the provider cannot honor. The tier is
// checked first (recorded).
func (p *Provider) check(body instanceBody) error {
	if body.Settings == nil || deref(body.Settings.Tier) == "" {
		return errMissingTier
	}
	version := deref(body.DatabaseVersion)
	switch {
	case !tierPattern.MatchString(*body.Settings.Tier):
		return provider.Invalid("Invalid tier.")
	case !instanceNamePattern.MatchString(body.Name):
		return provider.Invalid("Invalid instance name.")
	case p.releases[version] == "":
		return gap(fmt.Sprintf("databaseVersion %q (installed: %s)", version, strings.Join(sortedKeys(p.releases), ", ")))
	case body.Settings.SettingsVersion != nil:
		return provider.Invalid("settingsVersion applies to updates only.")
	}
	if err := checkTopology(body); err != nil {
		return err
	}
	if err := checkSettings(body.Settings); err != nil {
		return err
	}
	return checkFields(body)
}

// checkTopology refuses another backend or instance type.
func checkTopology(body instanceBody) error {
	if backend := deref(body.BackendType); backend != "" && backend != "SECOND_GEN" {
		return gap("backendType " + backend)
	}
	if kind := deref(body.InstanceType); kind != "" && kind != "CLOUD_SQL_INSTANCE" {
		return gap("instanceType " + kind)
	}
	return nil
}

// deref reads an optional string.
func deref(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}

// checkFields validates the free-form insert fields.
func checkFields(body instanceBody) error {
	if strings.ContainsRune(deref(body.RootPassword), 0) {
		return provider.Invalid("Invalid root password.")
	}
	if region := deref(body.Region); region != "" && !regionPattern.MatchString(region) {
		return provider.Invalid("Invalid region.")
	}
	return nil
}

// record validates an insert body and returns the instance metadata.
func (p *Provider) record(body instanceBody) (instanceRecord, error) {
	if err := p.check(body); err != nil {
		return instanceRecord{}, err
	}
	settings := body.Settings
	record := instanceRecord{Name: body.Name, Region: cmp.Or(deref(body.Region), defaultRegion), Version: deref(body.DatabaseVersion),
		Tier: deref(settings.Tier), Edition: deref(settings.Edition), IPv4: true, SettingsVersion: 1, Created: p.clock()}
	if settings.UserLabels != nil {
		record.Labels = maps.Clone(*settings.UserLabels)
	}
	if settings.DeletionProtectionEnabled != nil {
		record.DeletionProtection = *settings.DeletionProtectionEnabled
	}
	if ip := settings.IPConfiguration; ip != nil {
		if ip.IPv4Enabled != nil {
			record.IPv4 = *ip.IPv4Enabled
		}
		record.PrivateNetwork = ip.PrivateNetwork
	}
	if !record.IPv4 && record.PrivateNetwork == "" {
		return instanceRecord{}, provider.Invalid("At least one of public IP or private IP must be enabled.")
	}
	return record, nil
}

// insertInstance creates the server, then publishes the reference and metadata.
func (p *Provider) insertInstance(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	body, err := decodeInstance(request.JSON, project)
	if err != nil {
		return nil, err
	}
	record, err := p.record(body)
	if err != nil {
		return nil, err
	}
	started := p.clock()
	conflict := false
	ctx, cancel := context.WithTimeout(call.Context, createTimeout)
	defer cancel()
	err = p.use(ctx, project, func(ctx context.Context, fleet driver) error {
		if _, exists := p.lookup(project, record.Name); exists {
			conflict = true
			return nil
		}
		return p.start(ctx, fleet, project, record, deref(body.RootPassword))
	})
	if errors.Is(err, dataplane.ErrExhausted) {
		return apiError(http.StatusTooManyRequests, "RESOURCE_EXHAUSTED", "", "gcp-local: every published data-plane port is in use; delete an instance or widen the range"), nil
	}
	if err != nil {
		return nil, err
	}
	if conflict {
		return apiError(http.StatusConflict, "ALREADY_EXISTS", "instanceAlreadyExists", "The Cloud SQL instance already exists."), nil
	}
	return p.complete(call, project, "CREATE", record.Name, started)
}

// start creates the instance's server and published port, then indexes and
// stores the instance. A failure removes whatever was started.
func (p *Provider) start(ctx context.Context, fleet driver, project string, record instanceRecord, rootPassword string) error {
	if err := fleet.create(ctx, serverSpec{Instance: record.Name, Version: record.Version, RootPassword: rootPassword}); err != nil {
		return err
	}
	listener, err := p.openPublic(ctx, project, record)
	if err == nil && listener != nil {
		record.Port = listener.Port()
	}
	if err == nil {
		err = p.publish(project, record)
	}
	if err != nil {
		if listener != nil {
			listener.Close()
		}
		_ = fleet.remove(context.WithoutCancel(ctx), record.Name)
		return err
	}
	p.trackPublic(project, record.Name, listener)
	return nil
}

// publish indexes a created instance and stores its metadata under the
// project lock authorization holds.
func (p *Provider) publish(project string, record instanceRecord) error {
	return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error { //nolint:wrapcheck // the store returns scope validation only
		if err := p.index.Put(serviceName, state.Resource{Name: instanceResource(project, record.Name), Service: serviceName,
			Kind: kindInstance, Parent: provider.ProjectResource(project), Scope: provider.ProjectScope(project)}); err != nil {
			return fmt.Errorf("index the instance: %w", err)
		}
		c.instances[record.Name] = record.clone()
		return nil
	})
}

func (p *Provider) getInstance(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	record, ok := p.lookup(project, request.Path[pathInstance])
	if !ok {
		return notFound(), nil
	}
	return &gateway.RESTResponse{JSON: record.wire(project, p.releases[record.Version])}, nil
}

// instancePaging is instances.list's documented page sizes (500 by
// default, at most 1,000). A bad page token is refused as recorded
// (cloudsql-postgres-instance-admin).
var instancePaging = provider.Paging{Default: 500, Max: 1000, InvalidToken: errInvalidPageToken}

// listInstances renders one page of the project's instances, ordered by name.
func (p *Provider) listInstances(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	query, err := pageQuery(request.Query)
	if err != nil {
		return nil, err
	}
	var records []instanceRecord
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		for _, name := range sortedKeys(c.instances) {
			records = append(records, c.instances[name].clone())
		}
		return nil
	})
	page, next, err := provider.Page(records, query, instancePaging, gap)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(page))
	for _, record := range page {
		items = append(items, record.wire(project, p.releases[record.Version]))
	}
	// Recorded: instances.list has no kind.
	return &gateway.RESTResponse{JSON: listBody("", items, next)}, nil
}

// decodePatch strictly decodes a patch body and refuses a field that would
// change something other than tier, labels, and deletion protection. The
// settings enums and the backup and IP configurations may only restate what
// the instance already has.
func decodePatch(raw []byte, project, instance string) (instanceBody, error) {
	body, err := decodeInstance(raw, project)
	if err != nil {
		return instanceBody{}, err
	}
	if field := unpatchable(body); field != "" {
		return instanceBody{}, gap("instances.patch field " + field)
	}
	switch {
	case body.Name != "" && body.Name != instance:
		return instanceBody{}, provider.Invalid("Invalid instance name.")
	case body.Settings == nil:
		return body, nil
	case body.Settings.Tier != nil && !tierPattern.MatchString(*body.Settings.Tier):
		return instanceBody{}, provider.Invalid("Invalid tier.")
	default:
		return body, checkSettings(body.Settings)
	}
}

// unpatchable names the first field a patch may not change, or "".
func unpatchable(body instanceBody) string {
	present := map[string]bool{"region": body.Region != nil, "databaseVersion": body.DatabaseVersion != nil,
		"rootPassword": body.RootPassword != nil, "backendType": body.BackendType != nil, "instanceType": body.InstanceType != nil}
	if settings := body.Settings; settings != nil {
		present["settings.edition"] = settings.Edition != nil
		present["settings.ipConfiguration"] = settings.IPConfiguration != nil
	}
	for _, field := range sortedKeys(present) {
		if present[field] {
			return field
		}
	}
	return ""
}

// apply updates a record with a decoded patch's settings.
func apply(settings *settingsBody, record *instanceRecord) {
	if settings != nil {
		if settings.Tier != nil {
			record.Tier = *settings.Tier
		}
		if settings.UserLabels != nil {
			record.Labels = maps.Clone(*settings.UserLabels)
		}
		if settings.DeletionProtectionEnabled != nil {
			record.DeletionProtection = *settings.DeletionProtectionEnabled
		}
	}
	record.SettingsVersion++
}

// patchInstance updates provider-owned settings. A tier change is recorded
// without resizing the local server.
func (p *Provider) patchInstance(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name, started := request.Path[pathInstance], p.clock()
	patch, err := decodePatch(request.JSON, project, name)
	if err != nil {
		return nil, err
	}
	var version *string
	if patch.Settings != nil {
		version = patch.Settings.SettingsVersion
	}
	var failure *gateway.RESTResponse
	err = p.use(call.Context, project, func(context.Context, driver) error {
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			record, ok := c.instances[name]
			switch {
			case !ok:
				failure = notFound()
			case version != nil && *version != strconv.FormatInt(record.SettingsVersion, 10):
				failure = apiError(http.StatusPreconditionFailed, "FAILED_PRECONDITION", "", "Precondition check failed.")
			default:
				record = record.clone()
				apply(patch.Settings, &record)
				c.instances[name] = record
			}
			return nil
		})
	})
	if err != nil || failure != nil {
		return failure, err
	}
	return p.complete(call, project, "UPDATE", name, started)
}

// deleteInstance stops and removes the server, then the reference and metadata.
func (p *Provider) deleteInstance(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name, started := request.Path[pathInstance], p.clock()
	var failure *gateway.RESTResponse
	err := p.use(call.Context, project, func(ctx context.Context, fleet driver) error {
		record, ok := p.lookup(project, name)
		if !ok {
			failure = notFound()
			return nil
		}
		if record.DeletionProtection {
			failure = apiError(http.StatusBadRequest, "FAILED_PRECONDITION", "badRequest",
				"The instance is protected. Please disable the deletion protection and try again. "+
					"To disable deletion protection, update the instance settings with deletionProtectionEnabled set to false.")
			return nil
		}
		p.closePublic(project, name)
		if err := fleet.remove(ctx, name); err != nil {
			return err
		}
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			delete(c.instances, name)
			if err := p.index.Remove(serviceName, provider.ProjectScope(project), instanceResource(project, name)); err != nil {
				return fmt.Errorf("remove the instance reference: %w", err)
			}
			return nil
		})
	})
	if err != nil || failure != nil {
		return failure, err
	}
	return p.complete(call, project, "DELETE", name, started)
}
