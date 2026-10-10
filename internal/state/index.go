package state

import (
	"errors"
	"strings"
	"sync"
)

// Index sentinels.
var (
	errGlobalHierarchy  = errors.New("organization/folder references have global scope without a project")
	errResourceOwner    = errors.New("invalid resource owner or canonical name")
	errResourceConflict = errors.New("resource already belongs to another owner or hierarchy")
	errResourceScope    = errors.New("resource belongs to another owner or scope")
)

// Resource is a cross-service reference, not a second copy of backend data/IAM.
type Resource struct {
	Name, Service, Kind, Parent string
	Number                      string `json:"number,omitempty"`
	Scope                       Scope
}
type Index struct {
	mu        sync.RWMutex
	resources map[string]Resource
}

func NewIndex() *Index { return &Index{resources: map[string]Resource{}} }
func (i *Index) Put(owner string, resource Resource) error {
	if resource.Service == "resourcemanager" && (resource.Kind == "organization" || resource.Kind == "folder") {
		if resource.Scope.Project != "" || resource.Scope.Location != Global {
			return errGlobalHierarchy
		}
	} else if err := resource.Scope.Validate(); err != nil {
		return err
	}
	if owner == "" || resource.Service != owner || resource.Kind == "" || !strings.HasPrefix(resource.Name, "//") {
		return errResourceOwner
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if existing, ok := i.resources[resource.Name]; ok && (existing.Service != owner || existing.Scope != resource.Scope || existing.Kind != resource.Kind || existing.Parent != resource.Parent || existing.Number != resource.Number) {
		return errResourceConflict
	}
	i.resources[resource.Name] = resource
	return nil
}
func (i *Index) Get(name string) (Resource, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	value, ok := i.resources[name]
	return value, ok
}
func (i *Index) Remove(owner string, scope Scope, name string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	if existing, ok := i.resources[name]; ok && (existing.Service != owner || existing.Scope != scope) {
		return errResourceScope
	}
	delete(i.resources, name)
	return nil
}
func (i *Index) RemoveScope(owner string, scope Scope) {
	i.mu.Lock()
	defer i.mu.Unlock()
	for name, resource := range i.resources {
		if resource.Service == owner && resource.Scope == scope {
			delete(i.resources, name)
		}
	}
}
