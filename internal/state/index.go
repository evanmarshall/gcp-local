package state

import (
	"errors"
	"slices"
	"strings"
	"sync"
)

// Index sentinels.
var (
	errGlobalHierarchy  = errors.New("organization/folder references have global scope without a project")
	errResourceOwner    = errors.New("invalid resource owner or canonical name")
	errResourceConflict = errors.New("resource already belongs to another owner or hierarchy")
	errResourceScope    = errors.New("resource belongs to another owner or scope")
	errResourceMissing  = errors.New("resource is not indexed")
)

// Resource is a cross-service reference, not a second copy of backend data/IAM.
type Resource struct {
	Name    string `json:"name"`
	Service string `json:"service"`
	Kind    string `json:"kind"`
	Parent  string `json:"parent"`
	Number  string `json:"number,omitempty"`
	Scope   Scope  `json:"scope"`
}
type Index struct {
	mu        sync.RWMutex
	resources map[string]Resource
	// removed runs after each name leaves the index, outside the index lock.
	// The IAM engine registers here so a deleted resource never keeps its
	// policy: GCP gives a recreated resource of the same name an empty one.
	removed []func(name string)
}

func NewIndex() *Index { return &Index{resources: map[string]Resource{}} }
func (i *Index) Put(owner string, resource Resource) error {
	if err := validatePut(owner, resource); err != nil {
		return err
	}
	i.mu.Lock()
	defer i.mu.Unlock()
	if existing, ok := i.resources[resource.Name]; ok && (existing.Service != owner || existing.Scope != resource.Scope || existing.Kind != resource.Kind || existing.Parent != resource.Parent || existing.Number != resource.Number) {
		return errResourceConflict
	}
	i.resources[resource.Name] = resource
	return nil
}

// validatePut applies the shape rules that do not depend on index state.
// Global hierarchy nodes are the only entries allowed outside a project scope.
func validatePut(owner string, resource Resource) error {
	global := resource.Service == "resourcemanager" && (resource.Kind == "organization" || resource.Kind == "folder")
	if global {
		if resource.Scope.Project != "" || resource.Scope.Location != Global {
			return errGlobalHierarchy
		}
		return nil
	}
	if err := resource.Scope.Validate(); err != nil {
		return err
	}
	return validateOwner(owner, resource)
}

// validateOwner checks that the publishing service owns the resource name shape.
func validateOwner(owner string, resource Resource) error {
	if owner == "" || resource.Service != owner || resource.Kind == "" || !strings.HasPrefix(resource.Name, "//") {
		return errResourceOwner
	}
	return nil
}

func (i *Index) Get(name string) (Resource, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	value, ok := i.resources[name]
	return value, ok
}

// ProjectByNumber finds the indexed project with a numeric project ID, the
// form GCP returns in many resource names.
func (i *Index) ProjectByNumber(number string) (Resource, bool) {
	i.mu.RLock()
	defer i.mu.RUnlock()
	for _, resource := range i.resources {
		if resource.Service == "resourcemanager" && resource.Kind == "project" && resource.Number == number {
			return resource, true
		}
	}
	return Resource{}, false
}

// Owned returns every resource service publishes, ordered by name.
func (i *Index) Owned(service string) []Resource {
	i.mu.RLock()
	defer i.mu.RUnlock()
	var owned []Resource
	for _, resource := range i.resources {
		if resource.Service == service {
			owned = append(owned, resource)
		}
	}
	slices.SortFunc(owned, func(a, b Resource) int { return strings.Compare(a.Name, b.Name) })
	return owned
}

// Reparent moves one of owner's resources under parent. The resource keeps
// its name, so its policy stays attached and no removal hook runs: a moved
// project or folder keeps its own bindings and inherits from its new
// ancestors at once. The caller validates the new hierarchy.
func (i *Index) Reparent(owner, name, parent string) error {
	i.mu.Lock()
	defer i.mu.Unlock()
	existing, ok := i.resources[name]
	switch {
	case !ok:
		return errResourceMissing
	case existing.Service != owner:
		return errResourceScope
	}
	existing.Parent = parent
	i.resources[name] = existing
	return nil
}

// OnRemove registers a hook that runs for every name Remove or RemoveScope
// deletes. Hooks run after the index lock is released, so they may take
// locks of their own that are also held around index reads.
func (i *Index) OnRemove(hook func(name string)) {
	i.mu.Lock()
	defer i.mu.Unlock()
	i.removed = append(i.removed, hook)
}

func (i *Index) Remove(owner string, scope Scope, name string) error {
	i.mu.Lock()
	existing, ok := i.resources[name]
	if ok && (existing.Service != owner || existing.Scope != scope) {
		i.mu.Unlock()
		return errResourceScope
	}
	delete(i.resources, name)
	hooks := i.removed
	i.mu.Unlock()
	if ok {
		notify(hooks, []string{name})
	}
	return nil
}
func (i *Index) RemoveScope(owner string, scope Scope) {
	i.mu.Lock()
	var names []string
	for name, resource := range i.resources {
		if resource.Service == owner && resource.Scope == scope {
			delete(i.resources, name)
			names = append(names, name)
		}
	}
	hooks := i.removed
	i.mu.Unlock()
	notify(hooks, names)
}

// notify runs every removal hook for each removed name.
func notify(hooks []func(string), names []string) {
	for _, name := range names {
		for _, hook := range hooks {
			hook(name)
		}
	}
}
