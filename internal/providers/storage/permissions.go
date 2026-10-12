package storage

import (
	"fmt"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

// RegisterPermissions describes the initial UBLA permission plans. Registering
// these rules does not register API providers or claim service conformance.
func RegisterPermissions(engine *iam.Engine) error {
	rules := map[string]iam.Rule{
		MethodFoldersInsert:       {Baseline: []string{MethodFoldersCreate}},
		MethodFoldersCreate:       {Baseline: []string{MethodFoldersCreate}},
		MethodFoldersGet:          {Baseline: []string{"storage.folders.get"}},
		MethodFoldersList:         {Baseline: []string{"storage.folders.list"}},
		MethodFoldersDelete:       {Baseline: []string{"storage.folders.delete"}},
		MethodBucketsList:         {Baseline: []string{MethodBucketsList}},
		MethodBucketsInsert:       {Baseline: []string{permissionBucketsCreate}},
		MethodBucketsGet:          {Baseline: []string{MethodBucketsGet}},
		MethodBucketsDelete:       {Baseline: []string{MethodBucketsDelete}},
		MethodBucketsPatch:        {Baseline: []string{permissionBucketsUpdate}},
		MethodBucketsGetIamPolicy: {Baseline: []string{MethodBucketsGetIamPolicy}},
		MethodBucketsSetIamPolicy: {Baseline: []string{MethodBucketsSetIamPolicy}},
		// buckets/getStorageLayout requires storage.objects.list.
		MethodBucketsGetLayout: {Baseline: []string{permissionObjectsList}},
		MethodObjectsList:      {Baseline: []string{MethodObjectsList}},
		MethodObjectsGet:       {Baseline: []string{MethodObjectsGet}},
		MethodObjectsDelete:    {Baseline: []string{MethodObjectsDelete}},
		MethodObjectsPatch:     {Baseline: []string{permissionObjectsUpdate}},
		MethodObjectsInsert:    {Baseline: []string{"storage.objects.create"}, When: map[string][]string{objectExistsCondition: {MethodObjectsDelete}}},
		// objects/restore: restore and create, plus delete when a live object
		// is overwritten. The recorded denial names storage.objects.restore
		// first (storage-bucket-defaults "restore-denied").
		MethodObjectsRestore: {Baseline: []string{permissionObjectsRestore, permissionObjectsCreate}, When: map[string][]string{objectExistsCondition: {MethodObjectsDelete}}},
	}
	for method, rule := range rules {
		if err := engine.Register(method, rule); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}

// serviceName is the registered service key for Storage backends and index entries.
const serviceName = "storage"

// Storage JSON API method names. Naming them keeps the dispatch table,
// validation, and permission rules from drifting apart; a typo in any one
// place used to disable a method silently.
const (
	MethodBucketsInsert       = "storage.buckets.insert"
	MethodBucketsList         = "storage.buckets.list"
	MethodBucketsGet          = "storage.buckets.get"
	MethodBucketsPatch        = "storage.buckets.patch"
	MethodBucketsDelete       = "storage.buckets.delete"
	MethodBucketsGetIamPolicy = "storage.buckets.getIamPolicy"
	MethodBucketsSetIamPolicy = "storage.buckets.setIamPolicy"
	MethodBucketsGetLayout    = "storage.buckets.getStorageLayout"
	MethodFoldersCreate       = "storage.folders.create"
	MethodFoldersInsert       = "storage.folders.insert"
	MethodFoldersGet          = "storage.folders.get"
	MethodFoldersList         = "storage.folders.list"
	MethodFoldersDelete       = "storage.folders.delete"
	MethodObjectsInsert       = "storage.objects.insert"
	MethodObjectsList         = "storage.objects.list"
	MethodObjectsGet          = "storage.objects.get"
	MethodObjectsDelete       = "storage.objects.delete"
	MethodObjectsRewrite      = "storage.objects.rewrite"
	MethodObjectsPatch        = "storage.objects.patch"
	MethodObjectsRestore      = "storage.objects.restore"
	// MethodXMLGet and MethodXMLPut are the XML API object methods; they
	// authorize as objects.get and objects.insert.
	MethodXMLGet = "storage.xml.objects.get"
	MethodXMLPut = "storage.xml.objects.put"
)

// Permission names used by the UBLA baseline plans.
const (
	permissionBucketsCreate       = "storage.buckets.create"
	permissionBucketsDelete       = "storage.buckets.delete"
	permissionBucketsGet          = "storage.buckets.get"
	permissionBucketsGetIamPolicy = "storage.buckets.getIamPolicy"
	permissionBucketsList         = "storage.buckets.list"
	permissionBucketsSetIamPolicy = "storage.buckets.setIamPolicy"
	permissionBucketsUpdate       = "storage.buckets.update"
	permissionObjectsCreate       = "storage.objects.create"
	permissionObjectsDelete       = "storage.objects.delete"
	permissionObjectsGet          = "storage.objects.get"
	permissionObjectsList         = "storage.objects.list"
	permissionObjectsRestore      = "storage.objects.restore"
	permissionObjectsUpdate       = "storage.objects.update"
)

// Query and payload literals that repeat across validation and dispatch.
const (
	objectExistsCondition = "object_exists"
	kindBucket            = "bucket"
	altMedia              = "media"
	uploadResumable       = "resumable"
	keyBucket             = "bucket"
	keyGeneration         = "generation"
	keyCacheControl       = "cacheControl"
	keyDisposition        = "contentDisposition"
	keyContentType        = "contentType"
	keyLanguage           = "contentLanguage"
	keyMetadata           = "metadata"
	queryAlt              = "alt"
	queryUploadType       = "uploadType"
	querySoftDeleted      = "softDeleted"
	queryMaxResults       = "maxResults"
	queryPrefix           = "prefix"
	queryVersions         = "versions"
	keyKind               = "kind"
	// queryFoldersAsPrefixes is objects.list includeFoldersAsPrefixes, which
	// gcloud storage ls sends as True.
	queryFoldersAsPrefixes = "includeFoldersAsPrefixes"
	valueFalse             = "false"
	headerContentType      = "Content-Type"
	headerPayloadHash      = "x-goog-content-sha256"
	storageClassStandard   = "STANDARD"
	// keyName is the name key shared by query options, upload metadata, and
	// bucket/folder documents.
	keyName = "name"
)
