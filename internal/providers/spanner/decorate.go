package spanner

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/protobuf/proto"
)

// Service-managed values the emulator does not model. The provider supplies
// them as documented local defaults (docs/spanner.md); the recording compares
// only their presence.
const (
	defaultInstanceType     = "PROVISIONED"
	defaultEdition          = "STANDARD"
	defaultBackupSchedule   = "AUTOMATIC"
	defaultVersionRetention = "1h"
	defaultEncryptionType   = "GOOGLE_DEFAULT_ENCRYPTION"
	typeInstance            = "type.googleapis.com/google.spanner.admin.instance.v1.Instance"
	typeDatabase            = "type.googleapis.com/google.spanner.admin.database.v1.Database"
	keyType                 = "@type"
	keyResponse             = "response"
)

// createdDatabases records when the provider observed each database's
// successful creation; the emulator does not report database createTime.
type createdDatabases map[string]time.Time

// supported refuses instance settings the emulator would silently drop.
func supported(message proto.Message) error {
	var instance *instancepb.Instance
	switch request := message.(type) {
	case *instancepb.CreateInstanceRequest:
		instance = request.GetInstance()
	case *instancepb.UpdateInstanceRequest:
		instance = request.GetInstance()
	default:
		return nil
	}
	switch {
	case instance.GetEdition() > instancepb.Instance_STANDARD:
		return gap("instance edition " + instance.GetEdition().String())
	case instance.GetInstanceType() > instancepb.Instance_PROVISIONED:
		return gap("instance type " + instance.GetInstanceType().String())
	case instance.GetDefaultBackupScheduleType() != instancepb.Instance_DEFAULT_BACKUP_SCHEDULE_TYPE_UNSPECIFIED &&
		instance.GetDefaultBackupScheduleType() != instancepb.Instance_AUTOMATIC:
		return gap("default backup schedule type " + instance.GetDefaultBackupScheduleType().String())
	case instance.GetAutoscalingConfig() != nil:
		return gap("instance autoscaling")
	}
	return nil
}

// decorator fills service-managed fields in one rendered response, reading
// the emulator for facts it holds but does not return in that document.
type decorator struct {
	b       driver
	scope   state.Scope
	created *state.Store[createdDatabases]
	now     time.Time
}

// decorate dispatches by method over the rendered JSON document.
func (d decorator) decorate(ctx context.Context, method string, document map[string]any) error {
	switch method {
	case methodInstancesGet:
		instanceDefaults(document)
	case methodInstancesList:
		for _, item := range items(document, "instances") {
			instanceDefaults(item)
		}
	case methodInstancesCreate, methodInstancesUpdate, methodInstanceOperationGet:
		instanceOperation(document)
	default:
		return d.decorateDatabases(ctx, method, document)
	}
	return nil
}

// decorateDatabases handles the database reads and operations.
func (d decorator) decorateDatabases(ctx context.Context, method string, document map[string]any) error {
	switch method {
	case methodDatabasesGet:
		return d.database(ctx, document, false)
	case methodDatabasesList:
		for _, item := range items(document, "databases") {
			if err := d.database(ctx, item, false); err != nil {
				return err
			}
		}
	case methodDatabasesCreate, methodDatabaseOperationGet:
		if response, ok := document[keyResponse].(map[string]any); ok && response[keyType] == typeDatabase {
			return d.database(ctx, response, true)
		}
	}
	return nil
}

// instanceOperation decorates the instance in an operation's metadata and response.
func instanceOperation(document map[string]any) {
	if metadata, ok := document["metadata"].(map[string]any); ok {
		if instance, ok := metadata["instance"].(map[string]any); ok {
			instanceDefaults(instance)
		}
	}
	if response, ok := document[keyResponse].(map[string]any); ok && response[keyType] == typeInstance {
		instanceDefaults(response)
	}
}

// items returns the objects of one repeated JSON field.
func items(document map[string]any, field string) []map[string]any {
	values, _ := document[field].([]any)
	result := make([]map[string]any, 0, len(values))
	for _, value := range values {
		if item, ok := value.(map[string]any); ok {
			result = append(result, item)
		}
	}
	return result
}

// setDefault stores value only when the emulator left the field unset.
func setDefault(document map[string]any, field string, value any) {
	if _, ok := document[field]; !ok {
		document[field] = value
	}
}

// configLocation maps an instance config to its location: regional configs
// name their region; other config ids are reported unchanged.
func configLocation(config string) string {
	return strings.TrimPrefix(config[strings.LastIndexByte(config, '/')+1:], "regional-")
}

// instanceDefaults adds the instance fields the emulator does not model.
func instanceDefaults(instance map[string]any) {
	config, _ := instance["config"].(string)
	location := configLocation(config)
	setDefault(instance, "instanceType", defaultInstanceType)
	setDefault(instance, "edition", defaultEdition)
	setDefault(instance, "defaultBackupScheduleType", defaultBackupSchedule)
	setDefault(instance, "resourceLocation", location)
	setDefault(instance, "replicaComputeCapacity", []any{map[string]any{
		"replicaSelection": map[string]any{"location": location},
		"processingUnits":  instance["processingUnits"],
	}})
}

// database adds createTime, the version-retention window, and either the
// create response's resourceLocationId or the read's encryptionInfo, matching
// the recorded GCP field sets.
func (d decorator) database(ctx context.Context, document map[string]any, creation bool) error {
	name, _ := document["name"].(string)
	retention, err := d.retention(ctx, name)
	if err != nil {
		return err
	}
	setDefault(document, "versionRetentionPeriod", retention)
	var created time.Time
	_ = d.created.With(d.scope, func(times *createdDatabases) error { created = (*times)[name]; return nil })
	if !created.IsZero() {
		setDefault(document, "createTime", provider.Stamp(created))
		earliest := d.now.Add(-retentionDuration(retention))
		if earliest.Before(created) {
			earliest = created
		}
		setDefault(document, "earliestVersionTime", provider.Stamp(earliest))
	}
	if !creation {
		setDefault(document, "encryptionInfo", []any{map[string]any{"encryptionType": defaultEncryptionType}})
		return nil
	}
	instance := &instancepb.Instance{}
	if err := d.b.client().Invoke(ctx, instanceAdmin+"GetInstance",
		&instancepb.GetInstanceRequest{Name: name[:strings.LastIndex(name, "/databases/")]}, instance); err != nil {
		return fmt.Errorf("read the database instance: %w", err)
	}
	setDefault(document, "resourceLocationId", configLocation(instance.GetConfig()))
	return nil
}

// retentionOption finds a version_retention_period set through DDL.
var retentionOption = regexp.MustCompile(`(?i)version_retention_period\s*=\s*'([0-9]+[smhd])'`)

// retention reads the database's retention option from its emulator schema.
func (d decorator) retention(ctx context.Context, name string) (string, error) {
	response := &databasepb.GetDatabaseDdlResponse{}
	if err := d.b.client().Invoke(ctx, databaseAdmin+"GetDatabaseDdl", &databasepb.GetDatabaseDdlRequest{Database: name}, response); err != nil {
		return "", fmt.Errorf("read the database schema: %w", err)
	}
	retention := defaultVersionRetention
	for _, statement := range response.GetStatements() {
		if match := retentionOption.FindStringSubmatch(statement); match != nil {
			retention = match[1]
		}
	}
	return retention, nil
}

// retentionDuration parses Spanner's retention units (s, m, h, d).
func retentionDuration(value string) time.Duration {
	number, err := strconv.Atoi(value[:len(value)-1])
	if err != nil {
		return 0
	}
	unit := map[byte]time.Duration{'s': time.Second, 'm': time.Minute, 'h': time.Hour, 'd': 24 * time.Hour}[value[len(value)-1]]
	return time.Duration(number) * unit
}
