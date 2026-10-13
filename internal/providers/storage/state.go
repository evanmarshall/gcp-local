// Package storage owns Storage control-plane metadata and authorization.
package storage

import (
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"maps"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/providers/storage/bucketconfig"
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
	// scopes are the scopes a bucket was published in since their last reset.
	// The store can't enumerate its scopes, and the lifecycle worker visits
	// each one.
	scopes map[state.Scope]bool
}

func NewCatalog(index *state.Index) *Catalog {
	return &Catalog{store: state.NewStore(func() projectState {
		return projectState{Buckets: map[string]Bucket{}, Folders: map[string]Folder{}, Uploads: map[string]*uploadSession{}, SoftDeleted: map[string]softObject{}}
	}, state.GlobalScope), index: index, scopes: map[state.Scope]bool{}}
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
	c.scopes[scope] = true
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
	delete(c.scopes, scope)
	return nil
}

// lifecycleRules returns the lifecycle rules of each bucket in every scope
// that has any.
func (c *Catalog) lifecycleRules() map[state.Scope]map[string][]bucketconfig.Rule {
	c.mu.Lock()
	defer c.mu.Unlock()
	found := map[state.Scope]map[string][]bucketconfig.Rule{}
	for scope := range c.scopes {
		_ = c.store.With(scope, func(value *projectState) error {
			for name, bucket := range value.Buckets {
				var metadata struct {
					Lifecycle struct {
						Rule []bucketconfig.Rule `json:"rule"`
					} `json:"lifecycle"`
				}
				if json.Unmarshal([]byte(bucket.Metadata), &metadata) == nil && len(metadata.Lifecycle.Rule) > 0 {
					if found[scope] == nil {
						found[scope] = map[string][]bucketconfig.Rule{}
					}
					found[scope][name] = metadata.Lifecycle.Rule
				}
			}
			return nil
		})
	}
	return found
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
	// overlays holds the object metadata this provider owns, per object
	// generation (overlayKey), for fields upstream does not model. Upstream
	// reports every object at metageneration 1; GCP advances it on each
	// metadata update. Manager.Use serializes access, and Stop clears it with
	// the objects.
	overlays map[string]objectOverlay
}

// objectOverlay is one object generation's provider-owned metadata.
type objectOverlay struct {
	// metageneration is 0 until a patch, which reports as 1.
	metageneration int64
	// updated is the last patch's time; zero until a patch.
	updated time.Time
	// temporaryHold and eventBasedHold are nil until set: GCP omits a hold
	// that was never set (recorded: storage-retention-holds "retained-upload").
	temporaryHold, eventBasedHold *bool
	// released is when an event-based hold was released. A bucket retention
	// policy counts from it instead of from creation (recorded:
	// storage-retention-holds "release-starts-retention").
	released  time.Time
	retention *bucketconfig.Retention
	// customTime is zero until a patch sets it; it never decreases.
	customTime time.Time
	// storageClass is the class a lifecycle SetStorageClass action moved the
	// generation to, at classUpdated; "" keeps the backend's.
	storageClass string
	classUpdated time.Time
}

// render writes the overlay's fields into an object document.
func (o objectOverlay) render(document map[string]any) {
	delete(document, "retention")
	if !o.updated.IsZero() {
		document["updated"] = storageTime(o.updated)
	}
	for key, hold := range map[string]*bool{"temporaryHold": o.temporaryHold, "eventBasedHold": o.eventBasedHold} {
		if hold != nil {
			document[key] = *hold
		}
	}
	if o.retention != nil {
		document["retention"] = map[string]any{"mode": o.retention.Mode, "retainUntilTime": storageTime(o.retention.Until)}
	}
	if !o.customTime.IsZero() {
		document["customTime"] = customTimeText(o.customTime)
	}
	if o.storageClass != "" {
		document["storageClass"], document["timeStorageClassUpdated"] = o.storageClass, storageTime(o.classUpdated)
	}
}

// customTimeText renders customTime as GCP does: whole seconds without a
// fraction, otherwise milliseconds (recorded: storage-lifecycle-rules
// "custom-time-set", "custom-time-later").
func customTimeText(t time.Time) string {
	if t.Nanosecond() == 0 {
		return t.UTC().Format("2006-01-02T15:04:05Z")
	}
	return storageTime(t)
}

// overlayKey names one object generation in memoryBackend.overlays.
func overlayKey(bucket, name, generation string) string {
	return bucket + "\x00" + name + "\x00" + generation
}

func (b *memoryBackend) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true, Writer: io.Discard})
	if err != nil {
		return fmt.Errorf("start the storage backend: %w", err)
	}
	b.server, b.overlays = server, map[string]objectOverlay{}
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
		b.server, b.overlays = nil, nil
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
	return strconv.FormatInt(max(b.overlays[overlayKey(bucket, name, generation)].metageneration, 1), 10)
}

// overlay returns one object generation's overlay; the zero value until set.
func (b *memoryBackend) overlay(bucket, name, generation string) objectOverlay {
	return b.overlays[overlayKey(bucket, name, generation)]
}

// updateOverlay records a successful metadata update of one object
// generation at now: change edits the overlay, and the metageneration
// advances. It runs only after the backend accepted any backend-owned part.
func (b *memoryBackend) updateOverlay(bucket, name, generation string, now time.Time, change func(*objectOverlay)) {
	key := overlayKey(bucket, name, generation)
	overlay := b.overlays[key]
	change(&overlay)
	overlay.metageneration = max(overlay.metageneration, 1) + 1
	overlay.updated = now
	b.overlays[key] = overlay
}

// setOverlay changes a new generation's overlay without counting it as a
// metadata update.
func (b *memoryBackend) setOverlay(bucket, name, generation string, change func(*objectOverlay)) {
	key := overlayKey(bucket, name, generation)
	overlay := b.overlays[key]
	change(&overlay)
	b.overlays[key] = overlay
}

// forgetGeneration drops the overlay of a generation the backend no longer
// holds, live or noncurrent.
func (b *memoryBackend) forgetGeneration(bucket, name, generation string) {
	delete(b.overlays, overlayKey(bucket, name, generation))
}

// forgetBucket drops the overlays of a deleted bucket's objects.
func (b *memoryBackend) forgetBucket(bucket string) {
	maps.DeleteFunc(b.overlays, func(key string, _ objectOverlay) bool { return strings.HasPrefix(key, bucket+"\x00") })
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
