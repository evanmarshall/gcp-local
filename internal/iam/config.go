package iam

import (
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// Configuration sentinels; Load reports the first violated bootstrap rule.
var (
	errConfigMode      = errors.New("iam configuration must be a regular owner-only file")
	errConfigOpen      = errors.New("unable to open iam configuration")
	errConfigInvalid   = errors.New("invalid iam configuration")
	errConfigTrailing  = errors.New("iam configuration contains trailing data")
	errConfigHierarchy = errors.New("bootstrap only distinct resource-manager hierarchy nodes")
	errConfigProject   = errors.New("project name and scope disagree")
	errConfigRoot      = errors.New("organization must be a canonical root")
	errConfigFolder    = errors.New("folder must have a canonical name and parent")
	errConfigKind      = errors.New("unsupported hierarchy node kind")
	errConfigDuplicate = errors.New("duplicate local service account name or unique id")
)

type Config struct {
	Resources       []state.Resource    `json:"resources"`
	Policies        map[string]Policy   `json:"policies"`
	Unsupported     map[string][]string `json:"unsupported,omitempty"`
	ServiceAccounts []ServiceAccount    `json:"service_accounts,omitempty"`
}

// Load creates a fresh index/engine so a rejected bootstrap cannot publish a
// partial hierarchy to a running provider. Hierarchy roots and explicitly
// declared local service accounts are bootstrapped. Other service resources are
// published by their owning providers after backend success.
func Load(path string) (*Engine, *state.Index, error) {
	info, err := os.Lstat(path)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return nil, nil, errConfigMode
	}
	file, err := os.Open(path) //nolint:gosec // the operator selects the config path explicitly
	if err != nil {
		return nil, nil, errConfigOpen
	}
	defer file.Close()
	config, err := decodeConfig(file)
	if err != nil {
		return nil, nil, err
	}
	index := state.NewIndex()
	engine, err := New(index)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	if err := publishHierarchy(index, engine, config.Resources, seen); err != nil {
		return nil, nil, err
	}
	if err := publishAccounts(index, engine, config.ServiceAccounts, seen); err != nil {
		return nil, nil, err
	}
	for resource, policy := range config.Policies {
		if _, err := engine.SetPolicy(resource, policy); err != nil {
			return nil, nil, err
		}
	}
	for resource, features := range config.Unsupported {
		if err := engine.MarkUnsupported(resource, features); err != nil {
			return nil, nil, err
		}
	}
	return engine, index, nil
}

// decodeConfig reads one JSON object, rejecting unknown fields and trailing data.
func decodeConfig(file *os.File) (Config, error) {
	var config Config
	decoder := json.NewDecoder(io.LimitReader(file, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return Config{}, errConfigInvalid
	}
	var trailing map[string]any
	if decoder.Decode(&trailing) != io.EOF {
		return Config{}, errConfigTrailing
	}
	return config, nil
}

// publishHierarchy publishes the declared resourcemanager resources and rejects
// duplicates, non-global locations, and shapes the hierarchy cannot anchor.
func publishHierarchy(index *state.Index, engine *Engine, resources []state.Resource, seen map[string]bool) error {
	for _, resource := range resources {
		if resource.Service != "resourcemanager" || resource.Scope.Location != state.Global || seen[resource.Name] {
			return errConfigHierarchy
		}
		if err := validateHierarchyResource(resource); err != nil {
			return err
		}
		seen[resource.Name] = true
		if err := index.Put("resourcemanager", resource); err != nil {
			return fmt.Errorf("publish hierarchy resource %s: %w", resource.Name, err)
		}
	}
	for _, resource := range resources {
		if _, err := engine.ancestry(Target{Name: resource.Name}); err != nil {
			return err
		}
	}
	return nil
}

// validateHierarchyResource checks the resource name against its declared kind.
func validateHierarchyResource(resource state.Resource) error {
	prefix := "//cloudresourcemanager.googleapis.com/"
	switch resource.Kind {
	case "project":
		if resource.Name != prefix+"projects/"+resource.Scope.Project {
			return errConfigProject
		}
	case "organization":
		if !strings.HasPrefix(resource.Name, prefix+"organizations/") || resource.Parent != "" {
			return errConfigRoot
		}
	case "folder":
		if !strings.HasPrefix(resource.Name, prefix+"folders/") || resource.Parent == "" {
			return errConfigFolder
		}
	default:
		return errConfigKind
	}
	return nil
}

// publishAccounts publishes the declared service accounts and verifies each
// resource is reachable through the hierarchy.
func publishAccounts(index *state.Index, engine *Engine, accounts []ServiceAccount, seen map[string]bool) error {
	accountIDs := map[string]bool{}
	for _, account := range accounts {
		resource, err := account.resource()
		if err != nil {
			return err
		}
		if seen[resource.Name] || accountIDs[account.UniqueID] {
			return errConfigDuplicate
		}
		seen[resource.Name], accountIDs[account.UniqueID] = true, true
		if err := index.Put("iam", resource); err != nil {
			return fmt.Errorf("publish service account %s: %w", resource.Name, err)
		}
		if _, err := engine.ancestry(Target{Name: resource.Name}); err != nil {
			return err
		}
	}
	return nil
}
