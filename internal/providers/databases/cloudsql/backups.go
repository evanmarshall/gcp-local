package cloudsql

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// backupConfig is settings.backupConfiguration as stored. Enabled starts the
// instance's daily automated backup at StartTime (UTC); Retained bounds the
// automated runs kept.
type backupConfig struct {
	Enabled, PITR, BinaryLog bool
	StartTime, Location      string
	LogDays, Retained        int
}

// Backup configuration defaults (recorded: an instance created without one).
const (
	defaultLogDays  = 7
	defaultRetained = 7
	retentionCount  = "COUNT"
)

// defaultBackup is the configuration of an instance created without one;
// GCP assigns the start time.
func defaultBackup(created time.Time) backupConfig {
	return backupConfig{StartTime: fmt.Sprintf("%02d:00", created.UTC().Hour()), LogDays: defaultLogDays, Retained: defaultRetained}
}

// wire renders sql#backupConfiguration.
func (b backupConfig) wire(mysql bool) map[string]any {
	value := map[string]any{keyKind: "sql#backupConfiguration", "enabled": b.Enabled, keyStartTime: b.StartTime,
		"transactionLogRetentionDays": b.LogDays, "backupTier": "STANDARD",
		"transactionalLogStorageState": "TRANSACTIONAL_LOG_STORAGE_STATE_UNSPECIFIED",
		"backupRetentionSettings":      map[string]any{"retainedBackups": b.Retained, "retentionUnit": retentionCount}}
	if b.Location != "" {
		value["location"] = b.Location
	}
	if b.PITR {
		value["pointInTimeRecoveryEnabled"] = true
	}
	if b.PITR && !mysql {
		// Recorded (cloudsql-postgres-backups): PostgreSQL point-in-time
		// recovery archives its logs to Cloud Storage.
		value["replicationLogArchivingEnabled"], value["transactionalLogStorageState"] = true, "CLOUD_STORAGE"
	}
	if b.BinaryLog {
		// Recorded: MySQL binary logs are stored in Cloud Storage.
		value["binaryLogEnabled"], value["transactionalLogStorageState"] = true, "CLOUD_STORAGE"
	}
	return value
}

// applyBackup merges a backup configuration into record and validates the result.
func applyBackup(record *instanceRecord, body *backupBody) error {
	if body == nil {
		return nil
	}
	config := mergeBackup(record.Backup, body)
	if err := applyRetention(&config, body.BackupRetentionSettings); err != nil {
		return err
	}
	if err := checkArchiving(body.ReplicationLogArchivingEnabled, config, isMySQL(record.Version)); err != nil {
		return err
	}
	if err := config.check(isMySQL(record.Version)); err != nil {
		return err
	}
	record.Backup = config
	return nil
}

// mergeBackup applies the plain fields a backup configuration names.
func mergeBackup(config backupConfig, body *backupBody) backupConfig {
	for target, value := range map[*bool]*bool{&config.Enabled: body.Enabled, &config.PITR: body.PointInTimeRecoveryEnabled,
		&config.BinaryLog: body.BinaryLogEnabled} {
		if value != nil {
			*target = *value
		}
	}
	for target, value := range map[*string]*string{&config.StartTime: body.StartTime, &config.Location: body.Location} {
		if value != nil {
			*target = *value
		}
	}
	if days := body.TransactionLogRetentionDays; days != nil {
		config.LogDays = *days
	}
	return config
}

// checkArchiving accepts replicationLogArchivingEnabled only as PostgreSQL
// point-in-time recovery reports it.
func checkArchiving(archiving *bool, config backupConfig, mysql bool) error {
	if archiving != nil && *archiving && (!config.PITR || mysql) {
		return gap("settings.backupConfiguration.replicationLogArchivingEnabled without PostgreSQL point-in-time recovery")
	}
	return nil
}

// applyRetention applies backupRetentionSettings; COUNT is the only unit.
func applyRetention(b *backupConfig, body *retentionBody) error {
	if body == nil {
		return nil
	}
	if unit := deref(body.RetentionUnit); unit != "" && unit != retentionCount && unit != "RETENTION_UNIT_UNSPECIFIED" {
		return enumInvalid("body.settings.backup_configuration.backup_retention_settings.retention_unit",
			"BackupRetentionSettings.RetentionUnit", unit)
	}
	// Recorded (cloudsql-postgres-backups): GCP accepts 0, the unset value;
	// it leaves the count as it was.
	if body.RetainedBackups != nil && *body.RetainedBackups != 0 {
		b.Retained = *body.RetainedBackups
	}
	return nil
}

// check validates a backup configuration.
func (b backupConfig) check(mysql bool) error {
	// Recorded (cloudsql-postgres-backups): the start time and binary logging
	// are checked before the instance exists.
	switch {
	case !startTimePattern.MatchString(b.StartTime):
		return invalid("Invalid request: Backup specification (every day " + b.StartTime + ").")
	case b.BinaryLog && !mysql:
		return invalid("Invalid request: Binary log can only be enabled for MySQL instances.")
	}
	return b.checkRetention()
}

// checkRetention validates the retention and the logs backups depend on.
func (b backupConfig) checkRetention() error {
	switch {
	case b.LogDays < 1 || b.LogDays > 7:
		return invalid("Invalid request: The number of transaction log retention days must be between 1 and 7.")
	case b.Retained > 365:
		return invalid("Invalid request: Invalid retained backups (" + strconv.Itoa(b.Retained) + ").")
	case b.BinaryLog && !b.Enabled:
		// Recorded (cloudsql-mysql-backups-flags), before the instance exists.
		return invalid("Invalid request: Binary log must be disabled when backup is disabled or the instance must be a replica instance with a MySQL 5.7 or above version.")
	case b.PITR && !b.Enabled:
		return invalid("Invalid request: Point in time recovery must be disabled when backup is disabled.")
	}
	return nil
}

// backupRun is one sql#backupRun: a physical backup the backend holds,
// ON_DEMAND or AUTOMATED (with the window it ran in).
type backupRun struct {
	ID, Type, Description, Location, Version string
	Enqueued, Ended, Window                  time.Time
	// Deleted marks a run whose backup is gone; get still reads it.
	Deleted bool
}

// wire renders the backup run.
func (b backupRun) wire(project, instance string) map[string]any {
	value := map[string]any{keyKind: "sql#backupRun", "id": b.ID, pathInstance: instance, keyStatus: "SUCCESSFUL",
		keyType: b.Type, "backupKind": "SNAPSHOT", keyDatabaseVersion: b.Version, "enqueuedTime": rfc3339(b.Enqueued),
		"windowStartTime": rfc3339(cmp.Or(b.Window, b.Enqueued)), keySelfLink: instanceLink(project, instance) + "/backupRuns/" + b.ID}
	if b.Deleted {
		// Recorded: a deleted run stays readable without its run times.
		value[keyStatus] = "DELETED"
	} else {
		value[keyStartTime], value["endTime"] = rfc3339(b.Enqueued), rfc3339(b.Ended)
	}
	if b.Description != "" {
		value["description"] = b.Description
	}
	if b.Location != "" {
		value["location"] = b.Location
	}
	return value
}

// backupRunBody is the BackupRun subset an on-demand insert models
// (api/discovery/sqladmin-v1beta4.json). type is ignored, as documented.
type backupRunBody struct {
	Description string `json:"description"`
	Location    string `json:"location"`
	BackupKind  string `json:"backupKind"`
}

// backupRunFields refuses encryption and drops the output-only fields.
var backupRunFields = provider.Fields{
	Unmodeled: []string{"diskEncryptionConfiguration", "timeZone"},
	OutputOnly: []string{keyDatabaseVersion, "diskEncryptionStatus", "endTime", "enqueuedTime", "error", "id", "instance", "kind",
		"maxChargeableBytes", "selfLink", keyStartTime, keyStatus, "type", "windowStartTime"},
	IgnoreUnknown: true,
}

// Recorded refusals (cloudsql-postgres-backups).
var (
	errBackupRunMissing = failure{code: http.StatusNotFound, reason: "backupRunDoesNotExist", domain: domainGlobal,
		message: "The backup run does not exist."}
	errRestoreContext = invalid("Invalid request: Missing parameter: Backup name , BackupDr Backup name and RestoreBackupContext, all cannot be empty..")
	errRestoreRun     = invalid("Invalid request: Backup run does not exist..")
)

// backupRunPaging is backupRuns.list's page sizes: 500 by default, at most
// 1,000, as instances.list (not recorded).
var backupRunPaging = provider.Paging{Default: 500, Max: 1000, InvalidToken: errInvalidPageToken}

// insertBackupRun takes an on-demand physical backup of the instance's server.
func (p *Provider) insertBackupRun(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body backupRunBody
	if err := provider.DecodeJSON(request.JSON, "body", &body, gap, backupRunFields); err != nil {
		return nil, err
	}
	if body.BackupKind != "" && body.BackupKind != "SNAPSHOT" && body.BackupKind != "SQL_BACKUP_KIND_UNSPECIFIED" {
		return nil, gap("backupRuns.insert backupKind " + body.BackupKind)
	}
	name, started := request.Path[pathInstance], p.clock()
	var id string
	err := p.use(call.Context, project, func(ctx context.Context, fleet driver) error {
		record, ok := p.lookup(project, name)
		if !ok {
			return errInstanceMissing
		}
		id = p.backupID(project, name, started)
		if err := fleet.Backup(ctx, name, id); err != nil {
			return fmt.Errorf("back up the instance: %w", err)
		}
		run := backupRun{ID: id, Type: "ON_DEMAND", Description: body.Description, Location: cmp.Or(body.Location, record.Backup.Location, multiRegion(record.Region)),
			Version: record.Version, Enqueued: started, Ended: p.clock()}
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			c.backups[name] = append(c.backups[name], run)
			return nil
		})
	})
	if err != nil {
		return p.finish(call, project, "BACKUP_VOLUME", name, started, err)
	}
	return p.completeBackup(call, project, "BACKUP_VOLUME", name, id, started)
}

// backupID returns a new backup run ID: the enqueue time in milliseconds,
// the shape of GCP's IDs, unique within the instance.
func (p *Provider) backupID(project, instance string, enqueued time.Time) string {
	id := enqueued.UnixMilli()
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		for slices.ContainsFunc(c.backups[instance], func(b backupRun) bool { return b.ID == strconv.FormatInt(id, 10) }) {
			id++
		}
		return nil
	})
	return strconv.FormatInt(id, 10)
}

// runs returns a copy of one instance's backup runs, or false when the instance is missing.
func (p *Provider) runs(project, instance string) ([]backupRun, bool) {
	var runs []backupRun
	var found bool
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		_, found = c.instances[instance]
		runs = slices.Clone(c.backups[instance])
		return nil
	})
	return runs, found
}

// listBackupRuns lists an instance's backup runs, newest first.
func (p *Provider) listBackupRuns(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	query, err := pageQuery(request.Query)
	if err != nil {
		return nil, err
	}
	name := request.Path[pathInstance]
	runs, ok := p.runs(project, name)
	if !ok {
		return notFound(), nil
	}
	runs = slices.DeleteFunc(runs, func(b backupRun) bool { return b.Deleted })
	slices.Reverse(runs)
	page, next, err := provider.Page(runs, query, backupRunPaging, gap)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(page))
	for _, run := range page {
		items = append(items, run.wire(project, name))
	}
	return &gateway.RESTResponse{JSON: listBody("sql#backupRunsList", items, next)}, nil
}

// getBackupRun reads one backup run.
func (p *Provider) getBackupRun(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name := request.Path[pathInstance]
	runs, ok := p.runs(project, name)
	if !ok {
		return notFound(), nil
	}
	for _, run := range runs {
		if run.ID == request.Path["id"] {
			value := run.wire(project, name)
			if !run.Deleted {
				// Recorded: get, not list, reports the chargeable size; a stand-in.
				value["maxChargeableBytes"] = "0"
			}
			return &gateway.RESTResponse{JSON: value}, nil
		}
	}
	return nil, errBackupRunMissing
}

// deleteBackupRun deletes one backup run and its files.
func (p *Provider) deleteBackupRun(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name, id, started := request.Path[pathInstance], request.Path["id"], p.clock()
	err := p.use(call.Context, project, func(ctx context.Context, fleet driver) error {
		runs, ok := p.runs(project, name)
		switch {
		case !ok:
			return errInstanceMissing
		case !slices.ContainsFunc(runs, func(b backupRun) bool { return b.ID == id && !b.Deleted }):
			return errBackupRunMissing
		}
		if err := fleet.DeleteBackup(ctx, name, id); err != nil {
			return fmt.Errorf("delete the backup: %w", err)
		}
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			for i := range c.backups[name] {
				if c.backups[name][i].ID == id {
					c.backups[name][i].Deleted = true
				}
			}
			return nil
		})
	})
	return p.finish(call, project, "DELETE_BACKUP", name, started, err) // recorded: no backupContext
}

// restoreRun restores one backup run of source over target.
func (p *Provider) restoreRun(ctx context.Context, fleet driver, project, target, source, id string) error {
	if _, ok := p.lookup(project, target); !ok {
		return errInstanceMissing
	}
	runs, ok := p.runs(project, source)
	if !ok || !slices.ContainsFunc(runs, func(b backupRun) bool { return b.ID == id && !b.Deleted }) {
		return errRestoreRun
	}
	err := fleet.Restore(ctx, target, source, id)
	if errors.Is(err, server.ErrBackupVersion) {
		return invalid("Invalid request: The backup's database version doesn't match the target instance.")
	}
	if err != nil {
		return fmt.Errorf("restore the backup: %w", err)
	}
	return nil
}

// restoreBody is InstancesRestoreBackupRequest; only a backup run context is modeled.
type restoreBody struct {
	RestoreBackupContext *struct {
		Kind        string `json:"kind"`
		BackupRunID string `json:"backupRunId"`
		InstanceID  string `json:"instanceId"`
		Project     string `json:"project"`
	} `json:"restoreBackupContext"`
	IgnoreMaintenanceVersion bool `json:"ignoreMaintenanceVersion"`
}

// restoreFields refuses the backup resource and settings overrides.
var restoreFields = provider.Fields{
	Unmodeled:     []string{"backup", "backupdrBackup", "restoreInstanceSettings", "restoreInstanceClearOverridesFieldNames"},
	IgnoreUnknown: true,
}

// restoreBackup replaces the instance's data with a backup run of it or of
// another instance in the project, as GCP restores over the whole instance.
func (p *Provider) restoreBackup(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body restoreBody
	if err := provider.DecodeJSON(request.JSON, "body", &body, gap, restoreFields); err != nil {
		return nil, err
	}
	if body.RestoreBackupContext == nil {
		return nil, errRestoreContext
	}
	restore := *body.RestoreBackupContext
	if restore.Project != "" && restore.Project != project {
		return nil, gap("restoreBackup from another project")
	}
	target, started := request.Path[pathInstance], p.clock()
	source := cmp.Or(restore.InstanceID, target)
	err := p.use(call.Context, project, func(ctx context.Context, fleet driver) error {
		return p.restoreRun(ctx, fleet, project, target, source, restore.BackupRunID)
	})
	return p.finish(call, project, "RESTORE_VOLUME", target, started, err)
}

// scheduleBackups starts, restarts, or stops an instance's automated backup
// worker to match its configuration. The worker is the service's own: it
// takes backups without a caller and calls no other service.
func (p *Provider) scheduleBackups(ctx context.Context, project, instance string, enabled bool) {
	key := publicKey(project, instance)
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	if stop := p.workers[key]; stop != nil {
		stop()
		delete(p.workers, key)
	}
	if !enabled {
		return
	}
	// The worker outlives the request that configured it.
	lifetime, cancel := context.WithCancel(context.WithoutCancel(ctx))
	p.workers[key] = cancel
	go p.automate(lifetime, project, instance)
}

// stopProjectBackups stops every automated backup worker of one project.
func (p *Provider) stopProjectBackups(project string) {
	p.workersMu.Lock()
	defer p.workersMu.Unlock()
	for key, stop := range p.workers {
		if strings.HasPrefix(key, project+"/") {
			stop()
			delete(p.workers, key)
		}
	}
}

// automate takes an automated backup at each daily window start until the
// worker stops.
func (p *Provider) automate(ctx context.Context, project, instance string) {
	for {
		record, ok := p.lookup(project, instance)
		if !ok || !record.Backup.Enabled {
			return
		}
		window := nextWindow(p.clock(), record.Backup.StartTime)
		select {
		case <-ctx.Done():
			return
		case <-p.after(window.Sub(p.clock())):
		}
		p.automatedBackup(ctx, project, instance, window)
	}
}

// nextWindow is the first daily start at HH:MM UTC after now.
func nextWindow(now time.Time, start string) time.Time {
	parsed, _ := time.Parse("15:04", start) // validated by backupConfig.check
	now = now.UTC()
	window := time.Date(now.Year(), now.Month(), now.Day(), parsed.Hour(), parsed.Minute(), 0, 0, time.UTC)
	if !window.After(now) {
		window = window.AddDate(0, 0, 1)
	}
	return window
}

// automatedBackup takes one AUTOMATED backup run and deletes the oldest
// automated runs beyond retainedBackups. A failure skips the window.
func (p *Provider) automatedBackup(ctx context.Context, project, instance string, window time.Time) {
	_ = p.use(ctx, project, func(ctx context.Context, fleet driver) error {
		record, ok := p.lookup(project, instance)
		if !ok || !record.Backup.Enabled {
			return nil
		}
		started := p.clock()
		id := p.backupID(project, instance, started)
		if err := fleet.Backup(ctx, instance, id); err != nil {
			return fmt.Errorf("take the automated backup: %w", err)
		}
		run := backupRun{ID: id, Type: "AUTOMATED", Location: cmp.Or(record.Backup.Location, multiRegion(record.Region)), Version: record.Version,
			Enqueued: started, Ended: p.clock(), Window: window}
		var expired []string
		_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			c.backups[instance], expired = retain(append(c.backups[instance], run), record.Backup.Retained)
			return nil
		})
		for _, old := range expired {
			_ = fleet.DeleteBackup(ctx, instance, old)
		}
		return nil
	})
}

// retain keeps the newest retained AUTOMATED runs and every ON_DEMAND run,
// returning the kept runs and the IDs of the expired ones.
func retain(runs []backupRun, retained int) ([]backupRun, []string) {
	var expired []string
	automated := 0
	for _, run := range slices.Backward(runs) {
		if run.Type == "AUTOMATED" && !run.Deleted {
			if automated++; automated > retained {
				expired = append(expired, run.ID)
			}
		}
	}
	return slices.DeleteFunc(runs, func(b backupRun) bool { return slices.Contains(expired, b.ID) }), expired
}

// multiRegion is the default backup location of a region: its multi-region
// (recorded: "us" for us-central1 without a configured location).
func multiRegion(region string) string {
	prefix, _, _ := strings.Cut(region, "-")
	switch prefix {
	case "europe":
		return "eu"
	case "asia", "australia":
		return "asia"
	default:
		return prefix
	}
}
