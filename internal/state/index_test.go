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

// Reparent changes only the parent of the owner's resource and runs no
// removal hook; Owned lists one service's resources by name.
func TestReparentKeepsTheResource(t *testing.T) {
	index := NewIndex()
	removed := 0
	index.OnRemove(func(string) { removed++ })
	global := Scope{Location: Global}
	org, folder := "//cloudresourcemanager.googleapis.com/organizations/1", "//cloudresourcemanager.googleapis.com/folders/2"
	project := "//cloudresourcemanager.googleapis.com/projects/" + testProject
	for _, resource := range []Resource{
		{Name: project, Service: "resourcemanager", Kind: "project", Parent: org, Scope: Scope{Project: testProject, Location: Global}},
		{Name: folder, Service: "resourcemanager", Kind: "folder", Parent: org, Scope: global},
		{Name: org, Service: "resourcemanager", Kind: "organization", Scope: global},
	} {
		if err := index.Put("resourcemanager", resource); err != nil {
			t.Fatal(err)
		}
	}
	if err := index.Reparent("storage", project, folder); err == nil {
		t.Fatal("another owner moved the project")
	}
	if err := index.Reparent("resourcemanager", project+"-missing", folder); err == nil {
		t.Fatal("moved a missing resource")
	}
	if err := index.Reparent("resourcemanager", project, folder); err != nil {
		t.Fatal(err)
	}
	if moved, _ := index.Get(project); moved.Parent != folder || removed != 0 {
		t.Fatalf("moved = %+v, removal hooks = %d", moved, removed)
	}
	owned := index.Owned("resourcemanager")
	names := make([]string, 0, len(owned))
	for _, resource := range owned {
		names = append(names, resource.Name)
	}
	if !slices.Equal(names, []string{folder, org, project}) {
		t.Fatalf("owned = %v", names)
	}
}
