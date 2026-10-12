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
