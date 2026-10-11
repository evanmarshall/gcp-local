package storage

import (
	"fmt"

	"github.com/evanmarshall/gcp-local/internal/iam"
)

// RegisterPermissions describes the initial UBLA permission plans. Registering
// these rules does not register API providers or claim service conformance.
func RegisterPermissions(engine *iam.Engine) error {
	rules := map[string]iam.Rule{
		"storage.folders.insert":  {Baseline: []string{"storage.folders.create"}},
		MethodFoldersCreate:       {Baseline: []string{"storage.folders.create"}},
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
		MethodObjectsList:         {Baseline: []string{MethodObjectsList}},
		MethodObjectsGet:          {Baseline: []string{MethodObjectsGet}},
		MethodObjectsDelete:       {Baseline: []string{MethodObjectsDelete}},
		MethodObjectsInsert:       {Baseline: []string{"storage.objects.create"}, When: map[string][]string{objectExistsCondition: {MethodObjectsDelete}}},
	}
	for method, rule := range rules {
		if err := engine.Register(method, rule); err != nil {
			return fmt.Errorf("register %s: %w", method, err)
		}
	}
	return nil
}
