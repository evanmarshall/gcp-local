package iam

import (
	"encoding/json"
	"errors"
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
	file, err := os.Open(path)
	if err != nil {
		return nil, nil, errConfigOpen
	}
	defer file.Close()
	var config Config
	decoder := json.NewDecoder(io.LimitReader(file, 2<<20))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&config); err != nil {
		return nil, nil, errConfigInvalid
	}
	var trailing any
	if decoder.Decode(&trailing) != io.EOF {
		return nil, nil, errConfigTrailing
	}
	index := state.NewIndex()
	engine, err := New(index)
	if err != nil {
		return nil, nil, err
	}
	seen := map[string]bool{}
	for _, resource := range config.Resources {
		if resource.Service != "resourcemanager" || resource.Scope.Location != state.Global || seen[resource.Name] {
			return nil, nil, errConfigHierarchy
		}
		seen[resource.Name] = true
		prefix := "//cloudresourcemanager.googleapis.com/"
		switch resource.Kind {
		case "project":
			if resource.Name != prefix+"projects/"+resource.Scope.Project {
				return nil, nil, errConfigProject
			}
		case "organization":
			if !strings.HasPrefix(resource.Name, prefix+"organizations/") || resource.Parent != "" {
				return nil, nil, errConfigRoot
			}
		case "folder":
			if !strings.HasPrefix(resource.Name, prefix+"folders/") || resource.Parent == "" {
				return nil, nil, errConfigFolder
			}
		default:
			return nil, nil, errConfigKind
		}
		if err := index.Put("resourcemanager", resource); err != nil {
			return nil, nil, err
		}
	}
	for _, resource := range config.Resources {
		if _, err := engine.ancestry(Target{Name: resource.Name}); err != nil {
			return nil, nil, err
		}
	}
	accountIDs := map[string]bool{}
	for _, account := range config.ServiceAccounts {
		resource, err := account.resource()
		if err != nil {
			return nil, nil, err
		}
		if seen[resource.Name] || accountIDs[account.UniqueID] {
			return nil, nil, errConfigDuplicate
		}
		seen[resource.Name], accountIDs[account.UniqueID] = true, true
		if err := index.Put("iam", resource); err != nil {
			return nil, nil, err
		}
		if _, err := engine.ancestry(Target{Name: resource.Name}); err != nil {
			return nil, nil, err
		}
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
