package cloudsql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Provider is the authorization gate in front of the PostgreSQL and MySQL fleet. It
// owns instance metadata, operations, and index references; the servers own
// databases, roles, and data.
type Provider struct {
	manager *backend.Manager
	index   *state.Index
	engine  *iam.Engine
	gate    provider.Gate
	catalog *state.Store[catalog]
	// latency is the minimum time from a request to its operation reporting
	// DONE: local work finishes before the response, and the latency makes
	// clients poll, as they do against GCP.
	latency  time.Duration
	releases map[string]string
	pool     *dataplane.Pool
	publicMu sync.Mutex
	public   map[string]*dataplane.Listener
	clock    func() time.Time
	// after is the timer of automated backups; tests substitute it.
	after     func(time.Duration) <-chan time.Time
	workersMu sync.Mutex
	// workers stop each instance's automated backups, by project/instance.
	workers map[string]context.CancelFunc
}

// handler serves one authorized method for one project.
type handler func(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error)

// New registers the Cloud SQL provider backed by the pinned PostgreSQL and MySQL binaries.
func New(_ context.Context, registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, config Config) (*Provider, error) {
	versions, err := server.Verify(config.Root, config.MySQLRoot, config.RunAs)
	if err != nil {
		return nil, fmt.Errorf("verify the Cloud SQL backends: %w", err)
	}
	releases := map[string]string{}
	for version, dist := range versions {
		releases[version] = dist.Release()
	}
	provider, err := newProvider(registry, manager, index, engine, releases, func(state.Scope) (backend.Backend, error) {
		return server.NewFleet(versions, config.RunAs), nil
	})
	if err != nil {
		return nil, err
	}
	provider.pool, provider.latency = config.Publish, operationLatency
	return provider, nil
}

// newProvider wires the provider with an arbitrary backend factory (tests use a fake fleet).
func newProvider(registry *gateway.Registry, manager *backend.Manager, index *state.Index, engine *iam.Engine, releases map[string]string, factory func(state.Scope) (backend.Backend, error)) (*Provider, error) {
	if registry == nil || manager == nil || index == nil || engine == nil {
		return nil, errProviderShape
	}
	if err := registerPlans(engine); err != nil {
		return nil, err
	}
	p := &Provider{manager: manager, index: index, engine: engine, releases: releases, clock: time.Now, after: time.After,
		workers: map[string]context.CancelFunc{},
		gate: provider.Gate{Engine: engine, Index: index, Message: provider.LacksPermission,
			Attributes: func(target, _ string) iam.ResourceAttributes { return attributes(target) }},
		catalog: state.NewStore(newCatalog, state.GlobalScope), public: map[string]*dataplane.Listener{}}
	if err := manager.Register(serviceName, backend.Definition{
		Validate: state.GlobalScope,
		New:      factory,
		// The manager stops every server of the project (discarding its data
		// directories) before this hook closes its published ports and clears
		// metadata and references.
		// References go first, under the project lock authorization holds.
		AfterReset: func(scope state.Scope) error {
			p.closeProjectPublic(scope.Project)
			p.stopProjectBackups(scope.Project)
			unindex := p.catalog.With(scope, func(*catalog) error {
				index.RemoveScope(serviceName, scope)
				return nil
			})
			if err := errors.Join(unindex, p.catalog.Reset(scope)); err != nil {
				return fmt.Errorf("clear the Cloud SQL scope: %w", err)
			}
			return nil
		},
	}); err != nil {
		return nil, fmt.Errorf("register the Cloud SQL backend: %w", err)
	}
	return p, p.registerRoutes(registry)
}

// registerPlans registers every permission plan after confirming on a
// scratch engine that the pinned roles grant them all.
func registerPlans(engine *iam.Engine) error {
	if err := rolesRecorded(); err != nil {
		return err
	}
	for method, permission := range permissions {
		if err := engine.Register(method, iam.Rule{Baseline: []string{permission}}); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// registerRoutes binds every served method to the provider.
func (p *Provider) registerRoutes(registry *gateway.Registry) error {
	for method := range p.handlers() {
		if err := registry.Register(method, p.handle); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// rolesRecorded checks on a scratch engine that the pinned predefined roles
// grant every planned permission, so a missing snapshot refuses registration
// before any plan reaches the shared engine.
func rolesRecorded() error {
	scratch, err := iam.New(state.NewIndex())
	if err != nil {
		return fmt.Errorf("load the pinned roles: %w", err)
	}
	for method, permission := range permissions {
		if scratch.Register(method, iam.Rule{Baseline: []string{permission}}) != nil {
			return fmt.Errorf("%w: no pinned role grants %s", ErrRolesUnrecorded, permission)
		}
	}
	return nil
}

// handlers maps each served method to its implementation.
func (p *Provider) handlers() map[string]handler {
	return map[string]handler{
		methodInstancesInsert: p.insertInstance, methodInstancesGet: p.getInstance, methodInstancesList: p.listInstances,
		methodInstancesPatch: p.patchInstance, methodInstancesUpdate: p.patchInstance, methodInstancesDelete: p.deleteInstance,
		methodDatabasesInsert: p.insertDatabase, methodDatabasesGet: p.getDatabase, methodDatabasesList: p.listDatabases,
		methodDatabasesDelete: p.deleteDatabase,
		methodUsersInsert:     p.insertUser, methodUsersList: p.listUsers, methodUsersDelete: p.deleteUser,
		methodOperationsGet: p.getOperation, methodOperationsList: p.listOperations, methodOperationsCancel: p.cancelOperation,
		methodRestoreBackup: p.restoreBackup, methodFailover: failover,
		methodBackupRunsInsert: p.insertBackupRun, methodBackupRunsGet: p.getBackupRun, methodBackupRunsList: p.listBackupRuns,
		methodBackupRunsDelete: p.deleteBackupRun,
		methodSSLCertsInsert:   p.insertSSLCert, methodSSLCertsGet: p.getSSLCert, methodSSLCertsList: p.listSSLCerts,
		methodSSLCertsDelete: p.deleteSSLCert, methodConnectSettings: p.connectSettings,
	}
}

// handle is the gateway entry point. Authorization completes before any
// backend is started or called.
func (p *Provider) handle(call gateway.Call, raw any) (any, error) {
	request, ok := raw.(*gateway.RESTRequest)
	if !ok {
		return nil, status.Error(codes.Internal, "Invalid Cloud SQL request")
	}
	project := request.Path[pathProject]
	if provider.ProjectScope(project).Validate() != nil {
		return nil, provider.Invalid("Invalid project.")
	}
	decision, target, err := provider.Nearest(p.gate, p.catalog, provider.ProjectScope(project), call, "",
		p.candidates(project, p.addressed(call.Method, request, project))...)
	if err != nil {
		return refusal(call.Method, err)
	}
	if request.Query.Get("location") != "" {
		return nil, gap("the location query parameter")
	}
	call.Resource = target.Name
	response, err := p.handlers()[call.Method](call, request, project)
	if err != nil {
		return wireFailure(err)
	}
	return provider.WithDecision(response, decision), nil
}

// refusal renders a denied decision in the Cloud SQL shape (recorded,
// cloudsql-postgres-instance-admin); coverage gaps and other failures stay
// errors.
func refusal(method string, err error) (any, error) {
	switch status.Code(err) {
	case codes.PermissionDenied:
		return apiError(http.StatusForbidden, "PERMISSION_DENIED", "notAuthorized", "The client is not authorized to make this request."), nil
	case codes.Unauthenticated:
		return apiError(http.StatusUnauthorized, "UNAUTHENTICATED", "required", "Login Required."), nil
	default:
		return nil, fmt.Errorf("authorize %s: %w", method, err)
	}
}

// addressed names the instance a request acts on, or "" for project-level calls.
func (p *Provider) addressed(method string, request *gateway.RESTRequest, project string) string {
	switch method {
	case methodInstancesInsert, methodInstancesList:
		return ""
	case methodOperationsList:
		return request.Query.Get("instance")
	case methodOperationsGet, methodOperationsCancel:
		var instance string
		_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			if operation := c.operation(request.Path[pathOperation]); operation != nil {
				instance = operation.Instance
			}
			return nil
		})
		return instance
	default:
		return request.Path[pathInstance]
	}
}

// candidates lists the IAM targets of a request, most specific first: the
// instance while it is indexed, then its project, so a missing instance is
// authorized at the project and then reported as missing. Cloud SQL has no
// instance-level IAM policies; the instance node carries its project
// ancestry and, through attributes, the condition attributes.
func (p *Provider) candidates(project, instance string) []string {
	if instance == "" {
		return []string{provider.ProjectResource(project)}
	}
	return []string{instanceResource(project, instance), provider.ProjectResource(project)}
}

// attributes are an instance's IAM Conditions attributes (recorded,
// cloudsql-postgres-instance-admin); a project carries none.
func attributes(target string) iam.ResourceAttributes {
	name, isInstance := strings.CutPrefix(target, "//cloudsql.googleapis.com/")
	if !isInstance {
		return iam.ResourceAttributes{}
	}
	return iam.ResourceAttributes{Name: name, Type: "sqladmin.googleapis.com/Instance", Service: "sqladmin.googleapis.com"}
}

// use runs one backend operation for a project after authorization.
func (p *Provider) use(ctx context.Context, project string, operation func(context.Context, driver) error) error {
	err := p.manager.Use(ctx, backend.Key{Service: serviceName, Scope: provider.ProjectScope(project)}, func(ctx context.Context, active backend.Backend) error {
		fleet, err := driverOf(active)
		if err != nil {
			return err
		}
		return operation(ctx, fleet)
	})
	if err != nil {
		return fmt.Errorf("cloud SQL backend: %w", err)
	}
	return nil
}

// lookup returns a copy of one instance's metadata.
func (p *Provider) lookup(project, instance string) (instanceRecord, bool) {
	var record instanceRecord
	var found bool
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		var stored instanceRecord
		stored, found = c.instances[instance]
		record = stored.clone()
		return nil
	})
	return record, found
}

// complete records a finished operation and returns its wire form.
func (p *Provider) complete(call gateway.Call, project, kind, instance string, started time.Time) (*gateway.RESTResponse, error) {
	return p.completeBackup(call, project, kind, instance, "", started)
}

// completeBackup records a finished operation about one backup run.
func (p *Provider) completeBackup(call gateway.Call, project, kind, instance, backupID string, started time.Time) (*gateway.RESTResponse, error) {
	ended := p.clock()
	operation := newOperation(kind, instance, subject(call.Principal), started, ended, p.latency)
	operation.BackupID = backupID
	if err := p.catalog.With(provider.ProjectScope(project), func(c *catalog) error { c.record(operation); return nil }); err != nil {
		return nil, fmt.Errorf("record the operation: %w", err)
	}
	return &gateway.RESTResponse{JSON: operation.wire(project, ended)}, nil
}

// errInstanceMissing is notFound as a handler error.
var errInstanceMissing = failure{code: http.StatusNotFound, reason: "instanceDoesNotExist", domain: domainGlobal,
	message: "The Cloud SQL instance does not exist."}

// mutate changes one instance's metadata inside the project's backend
// session, under the store lock: change validates and edits a copy, which is
// stored only when it returns nil.
func (p *Provider) mutate(ctx context.Context, project, instance string, change func(*instanceRecord) error) error {
	return p.use(ctx, project, func(context.Context, driver) error {
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			stored, ok := c.instances[instance]
			if !ok {
				return errInstanceMissing
			}
			record := stored.clone()
			if err := change(&record); err != nil {
				return err
			}
			c.instances[instance] = record
			return nil
		})
	})
}

// subject renders the operation user as GCP does: the caller's email.
func subject(principal gateway.Principal) string {
	if principal.State != gateway.Authenticated {
		return ""
	}
	for _, prefix := range []string{"serviceAccount:", "user:"} {
		if len(principal.Subject) > len(prefix) && principal.Subject[:len(prefix)] == prefix {
			return principal.Subject[len(prefix):]
		}
	}
	return principal.Subject
}

// backendFailure maps PostgreSQL and MySQL errors onto API errors; other
// failures stay internal.
func backendFailure(err error) (*gateway.RESTResponse, error) {
	if my, ok := errors.AsType[*mysqlwire.Error](err); ok {
		return mysqlFailure(my), nil
	}
	var pg *pgwire.Error
	if !errors.As(err, &pg) {
		return nil, err
	}
	switch pg.Code {
	case "42P04", "42710": // duplicate_database, duplicate_object
		return apiError(http.StatusConflict, "ALREADY_EXISTS", "", pg.Message), nil
	case "3D000", "42704": // invalid_catalog_name, undefined_object
		return apiError(http.StatusNotFound, "NOT_FOUND", "", pg.Message), nil
	default:
		return apiError(http.StatusBadRequest, "FAILED_PRECONDITION", "", pg.Message), nil
	}
}

// mysqlFailure maps a MySQL server error onto an API error. Recorded
// (cloudsql-mysql-instance-admin): a duplicate database (1007) is a 400, not
// a 409, so it takes the default branch.
func mysqlFailure(my *mysqlwire.Error) *gateway.RESTResponse {
	switch {
	case my.Code == 1396 && strings.Contains(my.Message, "CREATE USER"): // account exists
		return apiError(http.StatusConflict, "ALREADY_EXISTS", "", my.Message)
	case my.Code == 1008, my.Code == 1049, my.Code == 1396: // database or account missing
		return apiError(http.StatusNotFound, "NOT_FOUND", "", my.Message)
	default:
		return apiError(http.StatusBadRequest, "FAILED_PRECONDITION", "", my.Message)
	}
}

// maxOperations bounds the retained operation history per project.
const maxOperations = 1000

// operationLatency is the production minimum from request to DONE.
const operationLatency = 500 * time.Millisecond

// instanceRecord is the provider-owned instance metadata. The server itself,
// its databases, its users, and its backups' files are owned by the backend.
type instanceRecord struct {
	Name, Region, Version, Tier, Edition, PrivateNetwork string
	Labels                                               map[string]string
	DeletionProtection, IPv4                             bool
	SettingsVersion                                      int64
	Created                                              time.Time
	// Port is the published data-plane port of a public-IP instance, or 0.
	Port int
	// Availability is ZONAL or REGIONAL. REGIONAL is metadata: one server
	// serves the instance and there is no standby to fail over to.
	Availability string
	Backup       backupConfig
	// SSLMode is the listener's TLS requirement; RequireSSL is its legacy
	// spelling, stored as requested.
	SSLMode    string
	RequireSSL bool
	Networks   []aclEntry
	Flags      []server.Flag
	// TLS is created with the instance and never changes.
	TLS   *authority
	Certs []clientCert
}

// clone copies a record so callers never share its maps or slices.
func (r instanceRecord) clone() instanceRecord {
	r.Labels = maps.Clone(r.Labels)
	r.Networks, r.Flags, r.Certs = slices.Clone(r.Networks), slices.Clone(r.Flags), slices.Clone(r.Certs)
	return r
}

// operationRecord is one sql#operation. Its work finished before it was
// recorded; it reports PENDING until Done, as GCP's do while they run.
type operationRecord struct {
	Name, Type, Instance, User string
	// BackupID names the backup run of a BACKUP_VOLUME or DELETE_BACKUP operation.
	BackupID       string
	Inserted, Done time.Time
}

// catalog is one project's provider state.
type catalog struct {
	instances  map[string]instanceRecord
	operations []operationRecord
	// backups are each instance's backup runs, oldest first.
	backups map[string][]backupRun
}

func newCatalog() catalog {
	return catalog{instances: map[string]instanceRecord{}, backups: map[string][]backupRun{}}
}

// record appends an operation, dropping the oldest beyond the bound.
func (c *catalog) record(operation operationRecord) {
	c.operations = append(c.operations, operation)
	if len(c.operations) > maxOperations {
		c.operations = append([]operationRecord(nil), c.operations[len(c.operations)-maxOperations:]...)
	}
}

// operation finds one operation by name, or nil.
func (c *catalog) operation(name string) *operationRecord {
	for i := range c.operations {
		if c.operations[i].Name == name {
			return &c.operations[i]
		}
	}
	return nil
}

// newOperation builds the operation of finished work: DONE once latency has
// passed since the work ended, so the response always reports PENDING, as
// GCP's insert does (recorded), however long a slow machine took to start
// the server.
func newOperation(kind, instance, user string, started, ended time.Time, latency time.Duration) operationRecord {
	return operationRecord{Name: operationID(), Type: kind, Instance: instance, User: user, Inserted: started, Done: ended.Add(latency)}
}

// wire renders the sql#operation resource at now: PENDING without endTime
// until the operation is done (recorded: an insert first reports PENDING).
func (o operationRecord) wire(project string, now time.Time) map[string]any {
	inserted := rfc3339(o.Inserted)
	value := map[string]any{
		keyKind: "sql#operation", keyName: o.Name, "operationType": o.Type, keyStatus: statusPending,
		"insertTime": inserted, keyStartTime: inserted,
		"targetId": o.Instance, "targetProject": project, "targetLink": instanceLink(project, o.Instance),
		keySelfLink: apiBase + "projects/" + project + "/operations/" + o.Name,
	}
	if !now.Before(o.Done) {
		value[keyStatus], value["endTime"] = statusDone, rfc3339(o.Done)
	}
	if o.User != "" {
		value["user"] = o.User
	}
	if o.BackupID != "" {
		value["backupContext"] = map[string]any{keyKind: "sql#backupContext", "backupId": o.BackupID,
			keyName: "projects/" + project + "/backups/" + o.BackupID}
	}
	return value
}

// serverCaMode is the only CA mode served: a per-instance CA (GCP's default, observed live).
const serverCaMode = "GOOGLE_MANAGED_INTERNAL_CA"

// rotationUnspecified is the observed default serverCertificateRotationMode.
const rotationUnspecified = "SERVER_CERTIFICATE_ROTATION_MODE_UNSPECIFIED"

// ipWire renders settings.ipConfiguration.
func (r instanceRecord) ipWire() map[string]any {
	value := map[string]any{"ipv4Enabled": r.IPv4, "requireSsl": r.RequireSSL, "sslMode": r.SSLMode,
		"serverCaMode": serverCaMode, "serverCertificateRotationMode": rotationUnspecified}
	if r.PrivateNetwork != "" {
		value["privateNetwork"] = r.PrivateNetwork
	}
	// Observed live: authorizedNetworks is present, empty, when nothing is authorized.
	entries := make([]any, 0, len(r.Networks))
	for _, entry := range r.Networks {
		entries = append(entries, entry.wire())
	}
	value["authorizedNetworks"] = entries
	return value
}

// addresses renders ipAddresses: one loopback address, PRIMARY or PRIVATE,
// with the published port, and PostgreSQL's OUTGOING address.
func (r instanceRecord) addresses() []any {
	ipType := "PRIMARY"
	if !r.IPv4 {
		ipType = "PRIVATE"
	}
	address := map[string]any{keyType: ipType, "ipAddress": loopbackIPv4}
	if r.Port > 0 {
		// gcp-local extension: GCP serves PostgreSQL on 5432 and MySQL on 3306
		// and has no port field; locally each instance has its own published port.
		address["port"] = r.Port
	}
	addresses := []any{address}
	if !isMySQL(r.Version) {
		// Recorded: PostgreSQL instances list an OUTGOING address; MySQL instances do not.
		addresses = append(addresses, map[string]any{keyType: "OUTGOING", "ipAddress": loopbackIPv4})
	}
	return addresses
}

// settingsWire renders sql#settings. The disk, location, and replication
// fields are stand-ins GCP always returns (observed live: their presence only).
func (r instanceRecord) settingsWire() map[string]any {
	settings := map[string]any{
		keyKind: "sql#settings", "tier": r.Tier, keySettingsVersion: strconv.FormatInt(r.SettingsVersion, 10),
		keyDeletionProtection: r.DeletionProtection, "activationPolicy": "ALWAYS", "availabilityType": r.Availability,
		"pricingPlan": "PER_USE", "ipConfiguration": r.ipWire(), "backupConfiguration": r.Backup.wire(isMySQL(r.Version)),
		"authorizedGaeApplications": []any{}, "connectorEnforcement": "NOT_REQUIRED", "dataDiskSizeGb": "10",
		"dataDiskType": "PD_SSD", "enableDataplexIntegration": false, "replicationLagMaxSeconds": 31536000,
		"replicationType": "SYNCHRONOUS", "storageAutoResize": true, "storageAutoResizeLimit": "0",
		"locationPreference": map[string]any{keyKind: "sql#locationPreference", "zone": r.Region + "-a"},
	}
	if len(r.Labels) > 0 {
		settings["userLabels"] = maps.Clone(r.Labels)
	}
	if r.Edition != "" {
		settings["edition"] = r.Edition
	}
	if len(r.Flags) > 0 {
		flags := make([]any, 0, len(r.Flags))
		for _, flag := range r.Flags {
			flags = append(flags, map[string]any{keyName: flag.Name, "value": flag.Value})
		}
		settings["databaseFlags"] = flags
	}
	return settings
}

// wire renders the sql#instance resource from provider metadata.
func (r instanceRecord) wire(project string, release string) map[string]any {
	value := map[string]any{
		keyKind: "sql#instance", keyName: r.Name, pathProject: project, keyRegion: r.Region, keyDatabaseVersion: r.Version,
		"state": statusRunnable, keyBackendType: secondGen, "instanceType": "CLOUD_SQL_INSTANCE",
		keyConnectionName: project + ":" + r.Region + ":" + r.Name, keySelfLink: instanceLink(project, r.Name),
		keyCreateTime: rfc3339(r.Created), keyETag: r.etag(), keyIPAddresses: r.addresses(), keySettings: r.settingsWire(),
		// Recorded (cloudsql-postgres-instance-admin): GCP always returns these
		// service-managed fields; the values are local stand-ins, only presence is compared.
		"gceZone": r.Region + "-a", "geminiConfig": map[string]any{"entitled": false},
		"includeReplicasForMajorVersionUpgrade": false, "maintenanceVersion": r.Version + "_LOCAL",
		"satisfiesPzi": false, "serviceAccountEmailAddress": "p-local@gcp-sa-cloud-sql.iam.gserviceaccount.com",
		"sqlNetworkArchitecture": "NEW_NETWORK_ARCHITECTURE", "upgradableDatabaseVersions": []any{},
	}
	if r.TLS != nil {
		value[keyServerCaCert] = r.TLS.caWire(project, r.Name, false)
	}
	if r.Availability == availabilityRegional {
		// Observed live (cloudsql-postgres-connectivity): a REGIONAL instance names a
		// secondary zone and a failover replica; locally neither exists.
		value["secondaryGceZone"] = r.Region + "-b"
		value["failoverReplica"] = map[string]any{"available": true}
	}
	if release != "" {
		engine, _, _ := strings.Cut(r.Version, "_")
		value["databaseInstalledVersion"] = engine + "_" + release
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
