package storage

import (
	"testing"

	"github.com/evanmarshall/gcp-local/internal/state"
)

func TestBucketProjectOwnershipAndGlobalNamespace(t *testing.T) {
	index := state.NewIndex()
	catalog := NewCatalog(index)
	a, b := state.Scope{Project: testProject, Location: state.Global}, state.Scope{Project: peerProject, Location: state.Global}
	bucket := Bucket{Name: "shared-name", Location: "us-central1"}
	if err := catalog.PublishBucket(a, bucket); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishBucket(b, bucket); err == nil {
		t.Fatal("bucket global name collided across projects")
	}
	if _, found, err := catalog.Bucket(b, bucket.Name); err != nil || found {
		t.Fatal("foreign project read bucket")
	}
	if catalog.PublishBucket(state.Scope{Project: a.Project, Location: "us-central1"}, bucket) == nil {
		t.Fatal("placement became a second namespace")
	}
	if catalog.PublishBucket(a, Bucket{Name: bucket.Name, Location: "europe-west1"}) == nil {
		t.Fatal("placement silently changed")
	}
	if err := catalog.ForgetBucket(b, bucket.Name); err == nil {
		t.Fatal("foreign project removed bucket index")
	}
	if resource, ok := index.Get(BucketResource(bucket.Name)); !ok || resource.Scope != a {
		t.Fatal("bucket index changed")
	}
}

func TestCatalogResetStaysInItsProject(t *testing.T) {
	index := state.NewIndex()
	catalog := NewCatalog(index)
	a, b := state.Scope{Project: testProject, Location: state.Global}, state.Scope{Project: peerProject, Location: state.Global}
	bucket := Bucket{Name: "shared-name", Location: "us-central1"}
	if err := catalog.PublishBucket(a, bucket); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishBucket(b, Bucket{Name: "other-name", Location: "europe-west1"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.Reset(a); err != nil {
		t.Fatal(err)
	}
	if _, found, _ := catalog.Bucket(a, bucket.Name); found {
		t.Fatal("reset retained bucket metadata")
	}
	if _, found := index.Get(BucketResource(bucket.Name)); found {
		t.Fatal("reset retained bucket index")
	}
	if _, found, _ := catalog.Bucket(b, "other-name"); !found {
		t.Fatal("reset crossed project")
	}
	if _, found := index.Get(BucketResource("other-name")); !found {
		t.Fatal("reset crossed index scope")
	}
}
