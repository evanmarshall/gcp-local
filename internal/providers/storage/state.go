// Package storage owns Storage control-plane metadata and authorization.
package storage

import (
	"errors"
	"fmt"
	"maps"
	"sync"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// Bucket placement belongs to this provider, while object bytes and wire object
// metadata belong to the backend. Bucket names are globally unique; placement is
// an attribute, not a second store key for the same bucket.
type Bucket struct{ Name, Location, Metadata string }
type projectState struct {
	Buckets map[string]Bucket
	Folders map[string]Folder
}
type Catalog struct {
	// Serialize publication/reset across provider store and shared index. Backend
	// calls happen before these short critical sections, never while locked.
	mu    sync.Mutex
	store *state.Store[projectState]
	index *state.Index
}

func NewCatalog(index *state.Index) *Catalog {
	return &Catalog{store: state.NewStore(func() projectState {
		return projectState{Buckets: map[string]Bucket{}, Folders: map[string]Folder{}}
	}, state.GlobalScope), index: index}
}
func BucketResource(name string) string { return "//storage.googleapis.com/projects/_/buckets/" + name }

// Catalog sentinels for bucket placement and metadata ownership.
var (
	errBucketIdentity  = errors.New("bucket name and placement are required")
	errBucketImmutable = errors.New("bucket placement is immutable")
	errBucketAbsent    = errors.New("bucket metadata is absent")
)

// PublishBucket is called only after backend creation succeeds. A caller must
// compensate/reconcile a backend creation if publication fails; no false entry
// is published when another project already owns this global name.
func (c *Catalog) PublishBucket(scope state.Scope, bucket Bucket) error {
	if bucket.Name == "" || bucket.Location == "" {
		return errBucketIdentity
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.With(scope, func(value *projectState) error {
		if existing, ok := value.Buckets[bucket.Name]; ok && existing != bucket {
			return errBucketImmutable
		}
		if err := c.index.Put(serviceName, state.Resource{Name: BucketResource(bucket.Name), Service: serviceName, Kind: kindBucket, Parent: "//cloudresourcemanager.googleapis.com/projects/" + scope.Project, Scope: scope}); err != nil {
			return fmt.Errorf("index bucket %s: %w", bucket.Name, err)
		}
		value.Buckets[bucket.Name] = bucket
		return nil
	}); err != nil {
		return fmt.Errorf("publish bucket %s: %w", bucket.Name, err)
	}
	return nil
}
func (c *Catalog) Bucket(scope state.Scope, name string) (Bucket, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var result Bucket
	var found bool
	if err := c.store.With(scope, func(value *projectState) error { result, found = value.Buckets[name]; return nil }); err != nil {
		return result, found, fmt.Errorf("read bucket %s: %w", name, err)
	}
	return result, found, nil
}

// UpdateMetadata changes provider-owned bucket fields under the backend gate.
// It cannot move a bucket or change its global ownership.
func (c *Catalog) UpdateMetadata(scope state.Scope, name, metadata string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.With(scope, func(value *projectState) error {
		bucket, ok := value.Buckets[name]
		if !ok {
			return errBucketAbsent
		}
		bucket.Metadata = metadata
		value.Buckets[name] = bucket
		return nil
	}); err != nil {
		return fmt.Errorf("update bucket %s metadata: %w", name, err)
	}
	return nil
}

// ForgetBucket follows confirmed backend deletion, never an attempted deletion.
func (c *Catalog) ForgetBucket(scope state.Scope, name string) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.With(scope, func(value *projectState) error {
		if err := c.index.Remove(serviceName, scope, BucketResource(name)); err != nil {
			return fmt.Errorf("unindex bucket %s: %w", name, err)
		}
		delete(value.Buckets, name)
		maps.DeleteFunc(value.Folders, func(_ string, folder Folder) bool { return folder.Bucket == name })
		return nil
	}); err != nil {
		return fmt.Errorf("delete bucket %s: %w", name, err)
	}
	return nil
}

// Reset is the manager's after-backend-reset hook, not a standalone API reset.
func (c *Catalog) Reset(scope state.Scope) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.Reset(scope); err != nil {
		return fmt.Errorf("reset scope %s: %w", scope, err)
	}
	c.index.RemoveScope(serviceName, scope)
	return nil
}

// Folder is one hierarchical-namespace folder entry; provider-owned because
// the storage backend does not model folders.
type Folder struct{ Bucket, Name string }

// PublishFolder records one folder whose parent bucket is already known.
func (c *Catalog) PublishFolder(scope state.Scope, folder Folder) error {
	if folder.Name == "" || folder.Bucket == "" {
		return errBucketIdentity
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.With(scope, func(value *projectState) error {
		if _, ok := value.Buckets[folder.Bucket]; !ok {
			return errBucketAbsent
		}
		value.Folders[folder.Bucket+"/"+folder.Name] = folder
		return nil
	}); err != nil {
		return fmt.Errorf("publish folder %s/%s: %w", folder.Bucket, folder.Name, err)
	}
	return nil
}

func (c *Catalog) Folder(scope state.Scope, bucket, name string) (Folder, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var found Folder
	var ok bool
	if err := c.store.With(scope, func(value *projectState) error {
		found, ok = value.Folders[bucket+"/"+name]
		return nil
	}); err != nil {
		return found, false, fmt.Errorf("read folder %s/%s: %w", bucket, name, err)
	}
	return found, ok, nil
}

func (c *Catalog) DeleteFolder(scope state.Scope, bucket, name string) (bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var removed bool
	if err := c.store.With(scope, func(value *projectState) error {
		key := bucket + "/" + name
		if _, ok := value.Folders[key]; ok {
			delete(value.Folders, key)
			removed = true
		}
		return nil
	}); err != nil {
		return false, fmt.Errorf("delete folder %s/%s: %w", bucket, name, err)
	}
	return removed, nil
}

func (c *Catalog) Folders(scope state.Scope, bucket string) ([]Folder, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var items []Folder
	if err := c.store.With(scope, func(value *projectState) error {
		for _, folder := range value.Folders {
			if folder.Bucket == bucket {
				items = append(items, folder)
			}
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("list folders of %s: %w", bucket, err)
	}
	return items, nil
}
