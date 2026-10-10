package state

import (
	"sync"
	"testing"
)

// testProject is the primary fixture project for scope isolation tests.
const testProject = "project-a"

func TestProjectAndLocationIsolationAndConcurrentAccess(t *testing.T) {
	store := NewStore(func() map[string]int { return map[string]int{} }, nil)
	a := Scope{testProject, "us-central1"}
	b := Scope{"project-b", "us-central1"}
	c := Scope{testProject, "europe-west1"}
	var group sync.WaitGroup
	for range 40 {
		group.Go(func() {
			if err := store.With(a, func(value *map[string]int) error { (*value)["count"]++; return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	for scope, expected := range map[Scope]int{a: 40, b: 0, c: 0} {
		if err := store.With(scope, func(value *map[string]int) error {
			if (*value)["count"] != expected {
				t.Errorf("scope %v leaked state: %v", scope, *value)
			}
			(*value)["retained"] = 1
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Reset(a); err != nil {
		t.Fatal(err)
	}
	for _, scope := range []Scope{a, b, c} {
		if err := store.With(scope, func(value *map[string]int) error {
			expected := 1
			if scope == a {
				expected = 0
			}
			if (*value)["retained"] != expected {
				t.Errorf("reset crossed scope: %v", scope)
			}
			return nil
		}); err != nil {
			t.Fatal(err)
		}
	}
	if store.With(Scope{Project: testProject}, func(*map[string]int) error { return nil }) == nil {
		t.Fatal("accepted implicit location")
	}
	if store.With(Scope{Location: Global}, func(*map[string]int) error { return nil }) == nil {
		t.Fatal("accepted implicit project")
	}
}
func TestIndexRejectsCrossOwnerAndCrossScopeChanges(t *testing.T) {
	index := NewIndex()
	resource := Resource{Name: "//storage.googleapis.com/projects/_/buckets/bucket", Service: "storage", Kind: "bucket", Parent: "//cloudresourcemanager.googleapis.com/projects/a", Scope: Scope{testProject, Global}}
	if err := index.Put("storage", resource); err != nil {
		t.Fatal(err)
	}
	copy := resource
	copy.Scope.Project = "project-b"
	if index.Put("storage", copy) == nil {
		t.Fatal("global name changed owner")
	}
	if index.Remove("pubsub", resource.Scope, resource.Name) == nil {
		t.Fatal("foreign service deleted resource")
	}
	if index.Remove("storage", copy.Scope, resource.Name) == nil {
		t.Fatal("foreign project deleted resource")
	}
	index.RemoveScope("pubsub", resource.Scope)
	if got, ok := index.Get(resource.Name); !ok || got != resource {
		t.Fatal("index ownership corrupted")
	}
	index.RemoveScope("storage", resource.Scope)
	if _, ok := index.Get(resource.Name); ok {
		t.Fatal("owned scope retained")
	}
}
