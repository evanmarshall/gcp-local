package state

import (
	"slices"
	"testing"
)

// Removal hooks see every name Remove and RemoveScope delete, and nothing
// for a refused or absent removal.
func TestRemovalHooksSeeEveryRemovedName(t *testing.T) {
	index := NewIndex()
	var removed []string
	index.OnRemove(func(name string) { removed = append(removed, name) })
	scope := Scope{Project: testProject, Location: Global}
	names := []string{"//svc.example/a", "//svc.example/b", "//svc.example/c"}
	for _, name := range names {
		if err := index.Put("svc", Resource{Name: name, Service: "svc", Kind: "thing", Scope: scope}); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Remove("other", scope, names[0]); err == nil {
		t.Fatal("another owner removed the resource")
	}
	if err := index.Remove("svc", scope, "//svc.example/missing"); err != nil {
		t.Fatal(err)
	}
	if err := index.Remove("svc", scope, names[0]); err != nil {
		t.Fatal(err)
	}
	index.RemoveScope("svc", scope)
	slices.Sort(removed)
	if !slices.Equal(removed, names) {
		t.Fatalf("removed = %v", removed)
	}
}
