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
	valueFalse            = "false"
	headerContentType     = "Content-Type"
	headerPayloadHash     = "x-goog-content-sha256"
	paramSignature        = "X-Goog-Signature"
	storageClassStandard  = "STANDARD"
	// keyName is the name key shared by query options, upload metadata, and
	// bucket/folder documents.
	keyName = "name"
)
