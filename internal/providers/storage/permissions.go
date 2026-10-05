package storage

import "github.com/evanmarshall/gcp-local/internal/iam"

// RegisterPermissions describes the initial UBLA permission plans. Registering
// these rules does not register API providers or claim service conformance.
func RegisterPermissions(engine *iam.Engine) error {
	rules := map[string]iam.Rule{
		"storage.buckets.list":         {Baseline: []string{"storage.buckets.list"}},
		"storage.buckets.insert":       {Baseline: []string{"storage.buckets.create"}},
		"storage.buckets.get":          {Baseline: []string{"storage.buckets.get"}},
		"storage.buckets.delete":       {Baseline: []string{"storage.buckets.delete"}},
		"storage.buckets.getIamPolicy": {Baseline: []string{"storage.buckets.getIamPolicy"}},
		"storage.buckets.setIamPolicy": {Baseline: []string{"storage.buckets.setIamPolicy"}},
		"storage.objects.list":         {Baseline: []string{"storage.objects.list"}},
		"storage.objects.get":          {Baseline: []string{"storage.objects.get"}},
		"storage.objects.delete":       {Baseline: []string{"storage.objects.delete"}},
		"storage.objects.insert":       {Baseline: []string{"storage.objects.create"}, When: map[string][]string{"object_exists": {"storage.objects.delete"}}},
	}
	for method, rule := range rules {
		if err := engine.Register(method, rule); err != nil {
			return err
		}
	}
	return nil
}
