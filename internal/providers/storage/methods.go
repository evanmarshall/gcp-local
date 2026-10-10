package storage

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
	MethodObjectsInsert       = "storage.objects.insert"
	MethodObjectsList         = "storage.objects.list"
	MethodObjectsGet          = "storage.objects.get"
	MethodObjectsDelete       = "storage.objects.delete"
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
)

// Query and payload literals that repeat across validation and dispatch.
const (
	objectExistsCondition = "object_exists"
	kindBucket            = "bucket"
	altMedia              = "media"
	storageClassStandard  = "STANDARD"
)
