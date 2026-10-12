package cloudsql

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"maps"
	"net/http"
	"regexp"
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

// settingsBody is the settings subset the provider models.
type settingsBody struct {
	Tier                      string            `json:"tier"`
	Edition                   string            `json:"edition"`
	UserLabels                map[string]string `json:"userLabels"`
	DeletionProtectionEnabled bool              `json:"deletionProtectionEnabled"`
	DatabaseFlags             []json.RawMessage `json:"databaseFlags"`
	IPConfiguration           *struct {
		IPv4Enabled    *bool  `json:"ipv4Enabled"`
		PrivateNetwork string `json:"privateNetwork"`
	} `json:"ipConfiguration"`
}

// instanceBody is the instances.insert subset the provider models. Fields
// that would change server topology are refused rather than ignored.
type instanceBody struct {
	Name                 string          `json:"name"`
	Region               string          `json:"region"`
	DatabaseVersion      string          `json:"databaseVersion"`
	RootPassword         string          `json:"rootPassword"`
	MasterInstanceName   string          `json:"masterInstanceName"`
	ReplicaConfiguration json.RawMessage `json:"replicaConfiguration"`
	FailoverReplica      json.RawMessage `json:"failoverReplica"`
	Settings             *settingsBody   `json:"settings"`
}

// check refuses an insert body the provider cannot honor.
func (p *Provider) check(body instanceBody) error {
	switch {
	case !instanceNamePattern.MatchString(body.Name):
		return provider.Invalid("Invalid instance name.")
	case p.releases[body.DatabaseVersion] == "":
		return gap(fmt.Sprintf("databaseVersion %q (installed: %s)", body.DatabaseVersion, strings.Join(sortedKeys(p.releases), ", ")))
	case body.MasterInstanceName != "" || len(body.ReplicaConfiguration) > 0 || len(body.FailoverReplica) > 0:
		return gap("replicas")
	case body.Settings == nil || !tierPattern.MatchString(body.Settings.Tier):
		return provider.Invalid("Invalid request: settings.tier is required.")
	case len(body.Settings.DatabaseFlags) > 0:
		return gap("settings.databaseFlags")
	default:
		return checkFields(body)
	}
}

// checkFields validates the free-form insert fields.
func checkFields(body instanceBody) error {
	if strings.ContainsRune(body.RootPassword, 0) {
		return provider.Invalid("Invalid root password.")
	}
	if body.Region != "" && !regionPattern.MatchString(body.Region) {
		return provider.Invalid("Invalid region.")
	}
	return nil
}

// record validates an insert body and returns the instance metadata.
func (p *Provider) record(body instanceBody) (instanceRecord, error) {
	if err := p.check(body); err != nil {
		return instanceRecord{}, err
	}
	record := instanceRecord{Name: body.Name, Region: body.Region, Version: body.DatabaseVersion, Tier: body.Settings.Tier,
		Edition: body.Settings.Edition, Labels: maps.Clone(body.Settings.UserLabels), IPv4: true,
		DeletionProtection: body.Settings.DeletionProtectionEnabled, SettingsVersion: 1, Created: p.clock()}
	if record.Region == "" {
		record.Region = defaultRegion
	}
	if ip := body.Settings.IPConfiguration; ip != nil {
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
	var body instanceBody
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, provider.Invalid("Invalid JSON payload.")
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
		return p.start(ctx, fleet, project, record, body.RootPassword)
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

// publish indexes a created instance and stores its metadata.
func (p *Provider) publish(project string, record instanceRecord) error {
	err := p.index.Put(serviceName, state.Resource{Name: instanceResource(project, record.Name), Service: serviceName,
		Kind: kindInstance, Parent: provider.ProjectResource(project), Scope: provider.ProjectScope(project)})
	if err != nil {
		return fmt.Errorf("index the instance: %w", err)
	}
	return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error { //nolint:wrapcheck // the store returns scope validation only
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

// listInstances renders every instance of the project in one page.
func (p *Provider) listInstances(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	if request.Query.Get("filter") != "" || request.Query.Get("pageToken") != "" {
		return nil, gap("instances.list filter and pageToken")
	}
	var items []any
	_ = p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
		for _, name := range sortedKeys(c.instances) {
			record := c.instances[name]
			items = append(items, record.wire(project, p.releases[record.Version]))
		}
		return nil
	})
	response := map[string]any{keyKind: "sql#instancesList"}
	if len(items) > 0 {
		response["items"] = items
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// Patchable fields; any other field present in a patch is refused.
var (
	patchTopLevel = map[string]bool{keySettings: true, keyName: true, pathProject: true, keyKind: true}
	patchSettings = map[string]bool{"tier": true, "userLabels": true, keyDeletionProtection: true, keySettingsVersion: true, keyKind: true}
)

// instancePatch is a decoded patch body.
type instancePatch struct {
	Tier               *string            `json:"tier"`
	Labels             *map[string]string `json:"userLabels"`
	DeletionProtection *bool              `json:"deletionProtectionEnabled"`
	Version            *string            `json:"settingsVersion"`
}

// decodePatch reads the supported settings fields of a patch body and
// refuses any other field.
func decodePatch(raw json.RawMessage) (instancePatch, error) {
	var top, settings map[string]json.RawMessage
	if json.Unmarshal(raw, &top) != nil {
		return instancePatch{}, provider.Invalid("Invalid JSON payload.")
	}
	if err := supportedKeys(top, patchTopLevel, ""); err != nil || top[keySettings] == nil {
		return instancePatch{}, err
	}
	var patch instancePatch
	if json.Unmarshal(top[keySettings], &settings) != nil || json.Unmarshal(top[keySettings], &patch) != nil {
		return instancePatch{}, provider.Invalid("Invalid settings.")
	}
	if err := supportedKeys(settings, patchSettings, "settings."); err != nil {
		return instancePatch{}, err
	}
	if patch.Tier != nil && !tierPattern.MatchString(*patch.Tier) {
		return instancePatch{}, provider.Invalid("Invalid tier.")
	}
	return patch, nil
}

// supportedKeys refuses a field outside the modeled set.
func supportedKeys(values map[string]json.RawMessage, allowed map[string]bool, prefix string) error {
	for _, key := range sortedKeys(values) {
		if !allowed[key] {
			return gap(fmt.Sprintf("instances.patch field %s%s", prefix, key))
		}
	}
	return nil
}

// apply updates a record with a decoded patch.
func (patch instancePatch) apply(record *instanceRecord) {
	if patch.Tier != nil {
		record.Tier = *patch.Tier
	}
	if patch.Labels != nil {
		record.Labels = maps.Clone(*patch.Labels)
	}
	if patch.DeletionProtection != nil {
		record.DeletionProtection = *patch.DeletionProtection
	}
	record.SettingsVersion++
}

// patchInstance updates provider-owned settings. A tier change is recorded
// without resizing the local server.
func (p *Provider) patchInstance(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	patch, err := decodePatch(request.JSON)
	if err != nil {
		return nil, err
	}
	name, started := request.Path[pathInstance], p.clock()
	var failure *gateway.RESTResponse
	err = p.use(call.Context, project, func(context.Context, driver) error {
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			record, ok := c.instances[name]
			switch {
			case !ok:
				failure = notFound()
			case patch.Version != nil && *patch.Version != strconv.FormatInt(record.SettingsVersion, 10):
				failure = apiError(http.StatusPreconditionFailed, "FAILED_PRECONDITION", "", "Precondition check failed.")
			default:
				record = record.clone()
				patch.apply(&record)
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
			failure = apiError(http.StatusBadRequest, "FAILED_PRECONDITION", "",
				"The instance is protected. Please disable the deletion protection and try again.")
			return nil
		}
		p.closePublic(project, name)
		if err := fleet.remove(ctx, name); err != nil {
			return err
		}
		if err := p.index.Remove(serviceName, provider.ProjectScope(project), instanceResource(project, name)); err != nil {
			return fmt.Errorf("remove the instance reference: %w", err)
		}
		return p.catalog.With(provider.ProjectScope(project), func(c *catalog) error {
			delete(c.instances, name)
			return nil
		})
	})
	if err != nil || failure != nil {
		return failure, err
	}
	return p.complete(call, project, "DELETE", name, started)
}
