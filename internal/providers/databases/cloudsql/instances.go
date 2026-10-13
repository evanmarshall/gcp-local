package cloudsql

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"net/netip"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server"
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
	startTimePattern    = regexp.MustCompile(`^([01][0-9]|2[0-3]):[0-5][0-9]$`)
)

// Availability types.
const (
	availabilityZonal    = "ZONAL"
	availabilityRegional = "REGIONAL"
)

// TLS modes of settings.ipConfiguration.sslMode.
const (
	sslAllow        = "ALLOW_UNENCRYPTED_AND_ENCRYPTED"
	sslEncrypted    = "ENCRYPTED_ONLY"
	sslTrustedCerts = "TRUSTED_CLIENT_CERTIFICATE_REQUIRED"
	sslUnspecified  = "SSL_MODE_UNSPECIFIED"
)

// settingsBody is the Settings subset the provider models. Pointers mark
// the fields a patch may name.
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
	DatabaseFlags             *[]flagBody        `json:"databaseFlags"`
}

// backupBody is settings.backupConfiguration. Patch merges the named fields.
type backupBody struct {
	Kind                        string  `json:"kind"`
	Enabled                     *bool   `json:"enabled"`
	StartTime                   *string `json:"startTime"`
	Location                    *string `json:"location"`
	PointInTimeRecoveryEnabled  *bool   `json:"pointInTimeRecoveryEnabled"`
	BinaryLogEnabled            *bool   `json:"binaryLogEnabled"`
	TransactionLogRetentionDays *int    `json:"transactionLogRetentionDays"`
	// ReplicationLogArchivingEnabled is reported with PostgreSQL point-in-time
	// recovery (recorded) and accepted only as that.
	ReplicationLogArchivingEnabled *bool          `json:"replicationLogArchivingEnabled"`
	BackupRetentionSettings        *retentionBody `json:"backupRetentionSettings"`
}

// retentionBody is backupConfiguration.backupRetentionSettings.
type retentionBody struct {
	RetainedBackups *int    `json:"retainedBackups"`
	RetentionUnit   *string `json:"retentionUnit"`
}

// ipBody is the IP configuration subset the provider models. Patch merges
// the named fields and replaces authorizedNetworks whole (recorded).
type ipBody struct {
	IPv4Enabled                   *bool       `json:"ipv4Enabled"`
	PrivateNetwork                *string     `json:"privateNetwork"`
	SSLMode                       *string     `json:"sslMode"`
	RequireSSL                    *bool       `json:"requireSsl"`
	AuthorizedNetworks            *[]aclEntry `json:"authorizedNetworks"`
	ServerCaMode                  *string     `json:"serverCaMode"`
	ServerCertificateRotationMode *string     `json:"serverCertificateRotationMode"`
}

// flagBody is one settings.databaseFlags entry.
type flagBody struct {
	Name  string `json:"name"`
	Value string `json:"value"`
}

// instanceBody is the DatabaseInstance subset the provider models.
type instanceBody struct {
	Kind            string  `json:"kind"`
	Name            string  `json:"name"`
	Project         string  `json:"project"`
	Region          *string `json:"region"`
	DatabaseVersion *string `json:"databaseVersion"`
	RootPassword    *string `json:"rootPassword"`
	BackendType     *string `json:"backendType"`
	InstanceType    *string `json:"instanceType"`
	// MaintenanceVersion is accepted only as the version the instance reports.
	MaintenanceVersion *string       `json:"maintenanceVersion"`
	Settings           *settingsBody `json:"settings"`
}

// instanceFields lists the v1beta4 DatabaseInstance fields instanceBody
// lacks (api/discovery/sqladmin-v1beta4.json): unmodeled ones are a gap
// unless empty, output-only ones are dropped. Fields the discovery document
// doesn't mark output-only but the service reports (state, ipAddresses,
// selfLink, and the settings stand-ins) are dropped too.
var instanceFields = provider.Fields{
	Unmodeled: []string{"databaseCenterIntegrationEnabled", "diskEncryptionConfiguration", "failoverReplica",
		"includeReplicasForMajorVersionUpgrade", "masterInstanceName", "nodeCount",
		"onPremisesConfiguration", "replicaConfiguration", "replicaNames", "replicationCluster", "sqlNetworkArchitecture",
		"switchTransactionLogsToCloudStorageEnabled", "tags",
		"settings.acceleratedReplicaMode", "settings.activeDirectoryConfig", "settings.advancedMachineFeatures",
		"settings.autoUpgradeEnabled", "settings.collation",
		"settings.connectionPoolConfig", "settings.crashSafeReplicationEnabled",
		"settings.dataApiAccess", "settings.dataCacheConfig", "settings.dataDiskProvisionedIops",
		"settings.dataDiskProvisionedThroughput",
		"settings.databaseReplicationEnabled", "settings.denyMaintenancePeriods",
		"settings.enableGoogleMlIntegration", "settings.entraidConfig",
		"settings.finalBackupConfig", "settings.insightsConfig",
		"settings.maintenanceWindow", "settings.passwordValidationPolicy", "settings.performanceCaptureConfig",
		"settings.readPoolAutoScaleConfig", "settings.sqlServerAuditConfig", "settings.timeZone",
		"settings.retainBackupsOnDelete",
		"settings.ipConfiguration.allocatedIpRange", "settings.ipConfiguration.customSubjectAlternativeNames",
		"settings.ipConfiguration.enablePrivatePathForGoogleCloudServices", "settings.ipConfiguration.pscConfig",
		"settings.ipConfiguration.serverCaPool"},
	OutputOnly: []string{"availableMaintenanceVersions", keyConnectionName, keyCreateTime, "currentDiskSize",
		"databaseInstalledVersion", "deploymentInfo", "diskEncryptionStatus", "dnsName", "dnsNames", keyETag, "gceZone",
		"geminiConfig", keyIPAddresses, "ipv6Address", "maxDiskSize", "nodes", "outOfDiskReport", "primaryDnsName",
		"pscServiceAttachmentLink", "satisfiesPzi", "satisfiesPzs", "scheduledMaintenance", "secondaryGceZone",
		"selfLink", keyServerCaCert, "serviceAccountEmailAddress", "state", "suspensionReason",
		"upgradableDatabaseVersions", "writeEndpoint",
		// Stand-ins the instance reports (settingsWire); clients send them back.
		"settings.authorizedGaeApplications", "settings.connectorEnforcement", "settings.dataDiskSizeGb",
		"settings.dataDiskType", "settings.enableDataplexIntegration", "settings.locationPreference",
		"settings.replicationLagMaxSeconds", "settings.replicationType", "settings.storageAutoResize",
		"settings.storageAutoResizeLimit",
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

// settingsEnums are the modeled values of the plain settings enums, with the
// transcoder's field path and enum type for an unknown value (recorded
// shape: cloudsql-postgres-connectivity). Every other documented value is a gap.
var settingsEnums = []struct {
	field, path, enum string
	value             func(*settingsBody) *string
	modeled, others   []string
}{
	{"activationPolicy", "activation_policy", "SqlActivationPolicy", func(s *settingsBody) *string { return s.ActivationPolicy },
		[]string{"ALWAYS", "SQL_ACTIVATION_POLICY_UNSPECIFIED"}, []string{"NEVER", "ON_DEMAND"}},
	{"pricingPlan", "pricing_plan", "SqlPricingPlan", func(s *settingsBody) *string { return s.PricingPlan },
		[]string{"PER_USE", "SQL_PRICING_PLAN_UNSPECIFIED"}, []string{"PACKAGE"}},
	{"availabilityType", "availability_type", "SqlAvailabilityType", func(s *settingsBody) *string { return s.AvailabilityType },
		[]string{availabilityZonal, availabilityRegional, "SQL_AVAILABILITY_TYPE_UNSPECIFIED"}, nil},
	{"edition", "edition", "Settings.Edition", func(s *settingsBody) *string { return s.Edition },
		[]string{"ENTERPRISE", "ENTERPRISE_PLUS", "EDITION_UNSPECIFIED"}, nil},
}

// checkEnums refuses an enum value the transcoder rejects and a documented
// value the local server can't honor.
func checkEnums(settings *settingsBody) error {
	for _, enum := range settingsEnums {
		value := deref(enum.value(settings))
		switch {
		case value == "" || slices.Contains(enum.modeled, value):
		case slices.Contains(enum.others, value):
			return gap(fmt.Sprintf("settings.%s %s", enum.field, value))
		default:
			return enumInvalid("body.settings."+enum.path, enum.enum, value)
		}
	}
	if settings.IPConfiguration == nil {
		return nil
	}
	if mode := deref(settings.IPConfiguration.SSLMode); mode != "" && !slices.Contains([]string{sslAllow, sslEncrypted, sslTrustedCerts, sslUnspecified}, mode) {
		return enumInvalid("body.settings.ip_configuration.ssl_mode", "IpConfiguration.SslMode", mode)
	}
	return nil
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
	case deref(body.MaintenanceVersion) != "":
		return gap("maintenanceVersion is not modeled")
	}
	if err := checkTopology(body); err != nil {
		return err
	}
	if err := checkEnums(body.Settings); err != nil {
		return err
	}
	return checkFields(body)
}

// checkTopology refuses another backend or instance type.
func checkTopology(body instanceBody) error {
	if backend := deref(body.BackendType); backend != "" && backend != secondGen {
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
	created := p.clock()
	record := instanceRecord{Name: body.Name, Region: cmp.Or(deref(body.Region), defaultRegion), Version: deref(body.DatabaseVersion),
		IPv4: true, SettingsVersion: 1, Created: created, Availability: availabilityZonal, SSLMode: sslAllow,
		Backup: defaultBackup(created)}
	if ip := body.Settings.IPConfiguration; ip != nil {
		if ip.IPv4Enabled != nil {
			record.IPv4 = *ip.IPv4Enabled
		}
		record.PrivateNetwork = deref(ip.PrivateNetwork)
	}
	if err := applySettings(&record, body.Settings); err != nil {
		return instanceRecord{}, err
	}
	if !record.IPv4 && record.PrivateNetwork == "" {
		return instanceRecord{}, provider.Invalid("At least one of public IP or private IP must be enabled.")
	}
	return record, nil
}

// applySettings validates settings against record and applies them, as an
// insert does over the defaults and a patch over the stored instance.
// Named fields replace stored ones; databaseFlags and authorizedNetworks
// replace the whole list (recorded).
func applySettings(record *instanceRecord, settings *settingsBody) error {
	if settings == nil {
		return nil
	}
	applyScalars(record, settings)
	if err := checkEdition(*record); err != nil {
		return err
	}
	if err := applyIP(record, settings.IPConfiguration); err != nil {
		return err
	}
	if err := applyBackup(record, settings.BackupConfiguration); err != nil {
		return err
	}
	if settings.DatabaseFlags != nil {
		flags, err := checkFlags(record.Version, *settings.DatabaseFlags)
		if err != nil {
			return err
		}
		record.Flags = flags
	}
	return nil
}

// applyScalars applies the settings a patch names that need no validation beyond decoding.
func applyScalars(record *instanceRecord, settings *settingsBody) {
	if settings.Tier != nil {
		record.Tier = *settings.Tier
	}
	if settings.Edition != nil && *settings.Edition != "EDITION_UNSPECIFIED" {
		record.Edition = *settings.Edition
	}
	if settings.UserLabels != nil {
		record.Labels = maps.Clone(*settings.UserLabels)
	}
	if settings.DeletionProtectionEnabled != nil {
		record.DeletionProtection = *settings.DeletionProtectionEnabled
	}
	if value := deref(settings.AvailabilityType); value == availabilityZonal || value == availabilityRegional {
		record.Availability = value
	}
}

// checkEdition refuses an Enterprise Plus instance without a performance-optimized tier (recorded).
func checkEdition(record instanceRecord) error {
	if record.Edition != "ENTERPRISE_PLUS" || strings.HasPrefix(record.Tier, "db-perf-optimized-N-") {
		return nil
	}
	engine := "postgres"
	if isMySQL(record.Version) {
		engine = "mysql"
	}
	return invalid(fmt.Sprintf("Invalid request: Invalid Tier (%s) for (ENTERPRISE_PLUS) Edition. Use a predefined Tier like "+
		"db-perf-optimized-N-* instead. Learn more at https://cloud.google.com/sql/docs/%s/create-instance#machine-types.", record.Tier, engine))
}

// applyIP applies settings.ipConfiguration: the TLS mode, the legacy
// requireSsl, and authorized networks. The CA mode and certificate rotation
// accept only the served values.
func applyIP(record *instanceRecord, ip *ipBody) error {
	if ip == nil {
		return nil
	}
	if err := checkCAModes(ip); err != nil {
		return err
	}
	if err := checkSSLPair(ip, record.Version); err != nil {
		return err
	}
	if ip.AuthorizedNetworks != nil {
		networks, err := checkNetworks(*ip.AuthorizedNetworks, record.Networks)
		if err != nil {
			return err
		}
		record.Networks = networks
	}
	applySSL(record, ip)
	return nil
}

// checkCAModes accepts only the served server CA mode and certificate rotation.
func checkCAModes(ip *ipBody) error {
	if mode := deref(ip.ServerCaMode); mode != "" && mode != serverCaMode && mode != "CA_MODE_UNSPECIFIED" {
		return gap("settings.ipConfiguration.serverCaMode " + mode)
	}
	if mode := deref(ip.ServerCertificateRotationMode); mode != "" && mode != rotationUnspecified {
		return gap("settings.ipConfiguration.serverCertificateRotationMode " + mode)
	}
	return nil
}

// checkSSLPair refuses an sslMode and requireSsl named together that
// disagree (recorded: cloudsql-postgres-connectivity).
func checkSSLPair(ip *ipBody, version string) error {
	mode := deref(ip.SSLMode)
	if ip.RequireSSL == nil || mode == "" || mode == sslUnspecified || *ip.RequireSSL == (mode == sslTrustedCerts) {
		return nil
	}
	engine := "Postgres"
	if isMySQL(version) {
		engine = "MySQL"
	}
	return invalid(fmt.Sprintf("Invalid request: For a %s instance, sslMode value %s and requireSsl value %t are conflicting. "+
		"When sslMode=ENCRYPTED_ONLY, requireSsl must be false. When requireSsl=true, sslMode must be TRUSTED_CLIENT_CERTIFICATE_REQUIRED. "+
		"It's recommended that you only set sslMode.", engine, mode, *ip.RequireSSL))
}

// applySSL applies sslMode and the legacy requireSsl; sslMode wins (Cloud
// SQL Admin API reference), and requireSsl alone sets or clears the client
// certificate requirement.
func applySSL(record *instanceRecord, ip *ipBody) {
	if ip.RequireSSL != nil {
		record.RequireSSL = *ip.RequireSSL
	}
	switch mode := deref(ip.SSLMode); {
	case mode != "" && mode != sslUnspecified:
		record.SSLMode = mode
	case ip.RequireSSL == nil:
	case *ip.RequireSSL:
		record.SSLMode = sslTrustedCerts
	case record.SSLMode == sslTrustedCerts:
		record.SSLMode = sslAllow
	}
}

// aclEntry is one authorized network, a CIDR range or a single address.
type aclEntry struct {
	Kind           string `json:"kind"`
	Name           string `json:"name"`
	Value          string `json:"value"`
	ExpirationTime string `json:"expirationTime"`
	prefix         netip.Prefix
	expires        time.Time
}

// wire renders an sql#aclEntry.
func (a aclEntry) wire() map[string]any {
	value := map[string]any{keyKind: "sql#aclEntry", "value": a.Value}
	if a.Name != "" {
		value[keyName] = a.Name
	}
	if a.ExpirationTime != "" {
		value["expirationTime"] = a.ExpirationTime
	}
	return value
}

// admits reports whether the entry admits an address at now; an expired
// entry admits nothing.
func (a aclEntry) admits(address netip.Addr, now time.Time) bool {
	return a.prefix.Contains(address.Unmap()) && (a.expires.IsZero() || now.Before(a.expires))
}

// checkNetworks parses authorized networks: an IPv4 CIDR with no host bits
// set, or one address. Anything else is GCP's refusal (recorded).
func checkNetworks(entries []aclEntry, current []aclEntry) ([]aclEntry, error) {
	parsed := make([]aclEntry, 0, len(entries))
	for _, entry := range entries {
		refused := invalid("Invalid request: Network (" + entry.Value + ")..")
		prefix, err := netip.ParsePrefix(entry.Value)
		if err != nil {
			address, addressErr := netip.ParseAddr(entry.Value)
			if addressErr != nil {
				return nil, refused
			}
			prefix = netip.PrefixFrom(address, address.BitLen())
		}
		if !prefix.Addr().Is4() || prefix.Masked() != prefix {
			return nil, refused
		}
		entry.prefix = prefix
		if entry.ExpirationTime != "" {
			expires, err := time.Parse(time.RFC3339Nano, entry.ExpirationTime)
			if err != nil {
				return nil, invalid("Invalid request: Invalid expiration time (" + entry.ExpirationTime + ").")
			}
			entry.expires = expires
		}
		entry.Kind = ""
		parsed = append(parsed, entry)
	}
	return reorder(parsed, current), nil
}

// reorder puts entries the instance didn't have first, in request order,
// then the ones it kept, in their stored order (recorded:
// cloudsql-postgres-connectivity).
func reorder(entries, current []aclEntry) []aclEntry {
	kept := func(entry aclEntry) bool {
		return slices.ContainsFunc(current, func(c aclEntry) bool { return c.Value == entry.Value })
	}
	ordered := slices.DeleteFunc(slices.Clone(entries), kept)
	for _, previous := range current {
		if index := slices.IndexFunc(entries, func(e aclEntry) bool { return e.Value == previous.Value }); index >= 0 {
			ordered = append(ordered, entries[index])
		}
	}
	return ordered
}

// errFlagName is GCP's refusal of a flag it doesn't support (recorded:
// cloudsql-mysql-backups-flags), a 404.
var errFlagName = failure{code: http.StatusNotFound, reason: "invalidFlagName", domain: domainGlobal,
	message: "The requested flag is either misspelled or unsupported by Cloud SQL."}

// checkFlags validates database flags for the instance's engine. A flag
// Cloud SQL documents but the local server doesn't model is a gap.
func checkFlags(version string, flags []flagBody) ([]server.Flag, error) {
	checked := make([]server.Flag, 0, len(flags))
	for _, flag := range flags {
		candidate := server.Flag{Name: flag.Name, Value: flag.Value}
		if slices.ContainsFunc(checked, func(f server.Flag) bool { return f.Name == flag.Name }) {
			return nil, invalid("Invalid request: Duplicate flags.")
		}
		check, reason := server.CheckFlag(version, candidate)
		switch check {
		case server.FlagUnmodeled:
			return nil, gap("database flag " + flag.Name)
		case server.FlagUnknown:
			return nil, errFlagName
		case server.FlagInvalidValue:
			return nil, failure{code: http.StatusBadRequest, reason: "invalidFlagValue", domain: domainGlobal,
				message: "Value requested is not valid. Failed to set " + flag.Name + ": " + reason}
		default:
			checked = append(checked, candidate)
		}
	}
	return checked, nil
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
	record.TLS, err = newAuthority(project, record.Name, record.Created)
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
	if err := fleet.Create(ctx, server.Spec{Instance: record.Name, Version: record.Version, RootPassword: rootPassword, Flags: record.Flags}); err != nil {
		return err //nolint:wrapcheck // the fleet's errors are descriptive
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
		_ = fleet.Remove(context.WithoutCancel(ctx), record.Name)
		return err
	}
	p.trackPublic(project, record.Name, listener)
	p.scheduleBackups(ctx, project, record.Name, record.Backup.Enabled)
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

// decodePatch strictly decodes a patch or update body.
func decodePatch(raw []byte, project, instance string) (instanceBody, error) {
	body, err := decodeInstance(raw, project)
	if err != nil {
		return instanceBody{}, err
	}
	switch {
	case body.Name != "" && body.Name != instance:
		return instanceBody{}, provider.Invalid("Invalid instance name.")
	case body.RootPassword != nil:
		return instanceBody{}, gap("instances.patch field rootPassword")
	case body.Settings == nil:
		return body, nil
	case body.Settings.Tier != nil && !tierPattern.MatchString(*body.Settings.Tier):
		return instanceBody{}, provider.Invalid("Invalid tier.")
	default:
		return body, checkEnums(body.Settings)
	}
}

// unpatchable names the first field a patch would change that the local
// server can't, or "". Clients that send the whole instance, as Terraform
// does, restate these unchanged.
func unpatchable(body instanceBody, record instanceRecord) string {
	changed := func(value *string, current ...string) bool {
		return value != nil && *value != "" && !slices.Contains(current, *value)
	}
	present := map[string]bool{keyRegion: changed(body.Region, record.Region), "databaseVersion": changed(body.DatabaseVersion, record.Version),
		"backendType": changed(body.BackendType, secondGen), "instanceType": changed(body.InstanceType, "CLOUD_SQL_INSTANCE"),
		"maintenanceVersion": changed(body.MaintenanceVersion, record.Version+"_LOCAL")}
	if settings := body.Settings; settings != nil {
		present["settings.edition"] = changed(settings.Edition, record.Edition, "EDITION_UNSPECIFIED")
		if ip := settings.IPConfiguration; ip != nil {
			present["settings.ipConfiguration.ipv4Enabled"] = ip.IPv4Enabled != nil && *ip.IPv4Enabled != record.IPv4
			present["settings.ipConfiguration.privateNetwork"] = changed(ip.PrivateNetwork, record.PrivateNetwork)
		}
	}
	for _, field := range sortedKeys(present) {
		if present[field] {
			return field
		}
	}
	return ""
}

// patched applies a patch to a copy of record; the public and private IP
// settings may only restate what the instance has. An update (PUT) also
// clears the labels, flags, and authorized networks it omits, as replacing
// the settings does (Cloud SQL Admin API reference; not recorded).
func (p *Provider) patched(record instanceRecord, body instanceBody, replace bool) (instanceRecord, error) {
	if field := unpatchable(body, record); field != "" {
		return instanceRecord{}, gap("instances.patch field " + field)
	}
	settings := body.Settings
	updated := record.clone()
	if replace {
		updated.Labels, updated.Flags = nil, nil
		if settings == nil || settings.IPConfiguration == nil || settings.IPConfiguration.AuthorizedNetworks == nil {
			updated.Networks = nil
		}
	}
	if err := applySettings(&updated, settings); err != nil {
		return instanceRecord{}, err
	}
	updated.SettingsVersion++
	return updated, nil
}

// patchInstance serves instances.patch and instances.update: it updates
// provider-owned settings and restarts the server when its flags change. A
// tier change is recorded without resizing it.
func (p *Provider) patchInstance(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name, started := request.Path[pathInstance], p.clock()
	patch, err := decodePatch(request.JSON, project, name)
	if err != nil {
		return nil, err
	}
	err = p.use(call.Context, project, func(ctx context.Context, fleet driver) error {
		return p.applyPatch(ctx, fleet, project, name, patch, call.Method == methodInstancesUpdate)
	})
	if err != nil {
		return nil, err
	}
	return p.complete(call, project, "UPDATE", name, started)
}

// applyPatch validates a patch against the stored instance, restarts the
// server when the flags change, and stores the result.
func (p *Provider) applyPatch(ctx context.Context, fleet driver, project, name string, patch instanceBody, replace bool) error {
	record, ok := p.lookup(project, name)
	if !ok {
		return errInstanceMissing
	}
	if patch.Settings != nil && patch.Settings.SettingsVersion != nil && *patch.Settings.SettingsVersion != strconv.FormatInt(record.SettingsVersion, 10) {
		return failure{code: http.StatusPreconditionFailed, message: "Precondition check failed."}
	}
	updated, err := p.patched(record, patch, replace)
	if err != nil {
		return err
	}
	if !slices.Equal(updated.Flags, record.Flags) {
		if err := fleet.Configure(ctx, name, updated.Flags); err != nil {
			return fmt.Errorf("apply the database flags: %w", err)
		}
	}
	if updated.Backup != record.Backup {
		defer p.scheduleBackups(ctx, project, name, updated.Backup.Enabled)
	}
	return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error { //nolint:wrapcheck // the store returns scope validation only
		c.instances[name] = updated
		return nil
	})
}

// failover is an explicit gap: a REGIONAL instance is one local server with
// no standby, so there is nothing to fail over to.
func failover(gateway.Call, *gateway.RESTRequest, string) (*gateway.RESTResponse, error) {
	return nil, gap("instances.failover (REGIONAL availability is stored, but one local server serves the instance and there is no standby)")
}

// deleteInstance stops and removes the server, then the reference, metadata, and backup runs.
func (p *Provider) deleteInstance(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name, started := request.Path[pathInstance], p.clock()
	if final, _ := strconv.ParseBool(request.Query.Get("enableFinalBackup")); final {
		return nil, gap("instances.delete enableFinalBackup (final backups are not modeled)")
	}
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
		p.scheduleBackups(ctx, project, name, false)
		if err := fleet.Remove(ctx, name); err != nil {
			return err //nolint:wrapcheck // the fleet's errors are descriptive
		}
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			delete(c.instances, name)
			delete(c.backups, name)
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
