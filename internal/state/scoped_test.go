package state

import (
	"sync"
	"testing"
)

// testProject is the primary fixture project for scope isolation tests.
const testProject = "project-a"

// counters is the per-scope value the isolation test stores.
type counters = map[string]int

// update applies change to scope's counters, failing the test on a store error.
func update(t *testing.T, store *Store[counters], scope Scope, change func(counters)) {
	t.Helper()
	if err := store.With(scope, func(value *counters) error { change(*value); return nil }); err != nil {
		t.Fatal(err)
	}
}

// read returns one counter from scope.
func read(t *testing.T, store *Store[counters], scope Scope, key string) int {
	t.Helper()
	var result int
	update(t, store, scope, func(value counters) { result = value[key] })
	return result
}

func TestProjectAndLocationIsolationAndConcurrentAccess(t *testing.T) {
	store := NewStore(func() counters { return counters{} }, nil)
	a := Scope{testProject, "us-central1"}
	b := Scope{"project-b", "us-central1"}
	c := Scope{testProject, "europe-west1"}
	var group sync.WaitGroup
	for range 40 {
		group.Go(func() {
			if err := store.With(a, func(value *counters) error { (*value)["count"]++; return nil }); err != nil {
				t.Error(err)
			}
		})
	}
	group.Wait()
	for scope, expected := range map[Scope]int{a: 40, b: 0, c: 0} {
		if got := read(t, store, scope, "count"); got != expected {
			t.Errorf("scope %v leaked state: count %d", scope, got)
		}
		update(t, store, scope, func(value counters) { value["retained"] = 1 })
	}
	if err := store.Reset(a); err != nil {
		t.Fatal(err)
	}
	for scope, expected := range map[Scope]int{a: 0, b: 1, c: 1} {
		if read(t, store, scope, "retained") != expected {
			t.Errorf("reset crossed scope: %v", scope)
		}
	}
	if store.With(Scope{Project: testProject}, func(*counters) error { return nil }) == nil {
		t.Fatal("accepted implicit location")
	}
	if store.With(Scope{Location: Global}, func(*counters) error { return nil }) == nil {
		t.Fatal("accepted implicit project")
	}
}
func TestIndexRejectsCrossOwnerAndCrossScopeChanges(t *testing.T) {
	index := NewIndex()
	resource := Resource{Name: "//storage.googleapis.com/projects/_/buckets/bucket", Service: "storage", Kind: "bucket", Parent: "//cloudresourcemanager.googleapis.com/projects/a", Scope: Scope{testProject, Global}}
	if err := index.Put("storage", resource); err != nil {
		t.Fatal(err)
	}
	changed := resource
	changed.Scope.Project = "project-b"
	if index.Put("storage", changed) == nil {
		t.Fatal("global name changed owner")
	}
	if index.Remove("pubsub", resource.Scope, resource.Name) == nil {
		t.Fatal("foreign service deleted resource")
	}
	if index.Remove("storage", changed.Scope, resource.Name) == nil {
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
