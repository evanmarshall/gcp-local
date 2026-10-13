// Package storage owns Storage control-plane metadata and authorization.
package storage

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/state"
	"github.com/fsouza/fake-gcs-server/fakestorage"
)

// Bucket placement belongs to this provider, while object bytes and wire object
// metadata belong to the backend. Bucket names are globally unique; placement is
// an attribute, not a second store key for the same bucket.
type Bucket struct{ Name, Location, Metadata string }
type projectState struct {
	Buckets map[string]Bucket
	Folders map[string]Folder
	// Uploads holds resumable sessions by upload ID until they expire, a week
	// after initiation (uploadLifetime), or a refused commit ends them.
	Uploads map[string]*uploadSession
	// SoftDeleted holds soft-deleted object generations by bucket, name, and
	// generation until their hard-delete time (softdelete.go).
	SoftDeleted map[string]softObject
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
		return projectState{Buckets: map[string]Bucket{}, Folders: map[string]Folder{}, Uploads: map[string]*uploadSession{}, SoftDeleted: map[string]softObject{}}
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
		maps.DeleteFunc(value.Uploads, func(_ string, upload *uploadSession) bool { return upload.bucket == name })
		// GCP soft-deletes the bucket itself; that is not modeled, so its
		// soft-deleted objects go with it.
		maps.DeleteFunc(value.SoftDeleted, func(_ string, object softObject) bool { return object.Bucket == name })
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

// Folders returns a bucket's folders in name order.
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
	slices.SortFunc(items, func(left, right Folder) int { return cmp.Compare(left.Name, right.Name) })
	return items, nil
}

// publishUpload records a resumable session after its initiation is
// authorized, and forgets the scope's sessions that expired by now.
func (c *Catalog) publishUpload(scope state.Scope, id string, upload *uploadSession, now time.Time) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.store.With(scope, func(value *projectState) error {
		if _, ok := value.Buckets[upload.bucket]; !ok {
			return errBucketAbsent
		}
		for known, session := range value.Uploads {
			if !now.Before(session.expires) {
				delete(value.Uploads, known)
			}
		}
		value.Uploads[id] = upload
		return nil
	}); err != nil {
		return fmt.Errorf("publish upload %s: %w", id, err)
	}
	return nil
}

// upload returns one unexpired resumable session of a bucket, or nil. An
// expired session is forgotten.
func (c *Catalog) upload(scope state.Scope, bucket, id string, now time.Time) *uploadSession {
	c.mu.Lock()
	defer c.mu.Unlock()
	var found *uploadSession
	_ = c.store.With(scope, func(value *projectState) error {
		upload := value.Uploads[id]
		switch {
		case upload == nil:
		case !now.Before(upload.expires):
			delete(value.Uploads, id)
		case upload.bucket == bucket:
			found = upload
		}
		return nil
	})
	return found
}

// forgetUpload removes a cancelled, refused, or failed session.
func (c *Catalog) forgetUpload(scope state.Scope, id string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.store.With(scope, func(value *projectState) error {
		delete(value.Uploads, id)
		return nil
	})
}

// memoryBackend uses upstream's in-process transport. It never opens a listener,
// loads ADC, configures notifications, or forwards a URL to a network transport.
// Manager.Use owns access and serializes Stop/Reset with every request.
type memoryBackend struct {
	server *fakestorage.Server
	// metagenerations holds the metageneration of each patched object, keyed
	// by bucket and name. Upstream reports every object at metageneration 1;
	// GCP advances it on each metadata update. An entry applies only to the
	// generation it names, so a replaced object starts again at 1. Manager.Use
	// serializes access, and Stop clears it with the objects.
	metagenerations map[string]objectVersion
}

// objectVersion is one object generation's provider-owned metageneration.
type objectVersion struct {
	generation     string
	metageneration int64
}

func (b *memoryBackend) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true, Writer: io.Discard})
	if err != nil {
		return fmt.Errorf("start the storage backend: %w", err)
	}
	b.server, b.metagenerations = server, map[string]objectVersion{}
	return nil
}

// errBackendStopped reports a readiness probe after Stop.
var errBackendStopped = errors.New("storage backend is stopped")

func (b *memoryBackend) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.server == nil {
		return errBackendStopped
	}
	return nil
}
func (b *memoryBackend) Stop(ctx context.Context) error {
	if b.server != nil {
		b.server.Stop()
		b.server, b.metagenerations = nil, nil
	}
	return ctx.Err()
}
func (b *memoryBackend) Reset(ctx context.Context) error { return b.Stop(ctx) }
func (b *memoryBackend) request(r *http.Request) (*http.Response, error) {
	if err := b.Ready(r.Context()); err != nil {
		return nil, err
	}
	response, err := b.server.HTTPClient().Transport.RoundTrip(r)
	if err != nil {
		return nil, fmt.Errorf("forward to the storage backend: %w", err)
	}
	return response, nil
}

// metageneration returns an object generation's metageneration: 1 until a
// patch advances it.
func (b *memoryBackend) metageneration(bucket, name, generation string) string {
	if version, ok := b.metagenerations[bucket+"/"+name]; ok && version.generation == generation {
		return strconv.FormatInt(version.metageneration, 10)
	}
	return "1"
}

// advanceMetageneration records a successful metadata update of one object
// generation. It runs only after the backend accepted the update.
func (b *memoryBackend) advanceMetageneration(bucket, name, generation string) {
	current, _ := strconv.ParseInt(b.metageneration(bucket, name, generation), 10, 64)
	b.metagenerations[bucket+"/"+name] = objectVersion{generation: generation, metageneration: current + 1}
}

// errBackendType reports a backend that this provider did not register.
var errBackendType = errors.New("backend is not the storage emulator")

// errPayloadShape reports a backend response that is not a JSON object, which
// means the pinned backend replied in a shape this provider cannot read.
var errPayloadShape = errors.New("backend response is not a JSON object")

// backendOf returns the in-process emulator behind a manager-owned backend. The
// assertion is checked so a foreign backend is an error, not a panic.
func backendOf(driver backend.Backend) (*memoryBackend, error) {
	value, ok := driver.(*memoryBackend)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}

// jsonObject narrows a decoded backend payload to a JSON object.
func jsonObject(value any) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errPayloadShape
	}
	return object, nil
}

// itemList narrows a decoded JSON array of objects, skipping foreign entries.
func itemList(value any) []map[string]any {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	items := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		if object, ok := entry.(map[string]any); ok {
			items = append(items, object)
		}
	}
	return items
}
