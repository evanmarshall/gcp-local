package storage

import (
	"errors"
	"testing"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// Catalog fixture names.
const (
	catalogBucket = "owned"
	absentBucket  = "absent"
)

// TestFolderLifecycleAndAuthorization covers the provider-owned folders
// surface: create, conflict, get, list, delete, and denial.
func TestFolderLifecycleAndAuthorization(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "hns-bucket", 200)
	folders := "/storage/v1/b/hns-bucket/folders"
	created := decodeBody(t, f.request(admin, "POST", folders, []byte(`{"name":"reports"}`), 200).Body.Bytes())
	if created["name"] != "reports" || created["bucket"] != "hns-bucket" {
		t.Fatalf("folder = %v", created)
	}
	f.request(admin, "POST", folders, []byte(`{"name":"reports"}`), 409)
	f.request(admin, "POST", folders, []byte(`{"name":""}`), 400)
	f.request(admin, "POST", folders, []byte(`[]`), 400)
	f.request(admin, "GET", folders+"/reports", nil, 200)
	f.request(admin, "GET", folders+"/absent", nil, 404)
	f.request(admin, "POST", folders, []byte(`{"name":"archive"}`), 200)
	listed := decodeBody(t, f.request(admin, "GET", folders, nil, 200).Body.Bytes())
	if items, _ := listed["folders"].([]any); len(items) != 2 {
		t.Fatalf("folders = %v", listed)
	}

	f.request("stranger@project-a.test", "GET", folders, nil, 403)
	f.request("stranger@project-a.test", "POST", folders, []byte(`{"name":"intrusion"}`), 403)
	f.request("stranger@project-a.test", "DELETE", folders+"/reports", nil, 403)
	f.request(admin, "GET", "/storage/v1/b/absent-bucket/folders", nil, 404)

	// Folders in another project's bucket are unaffected by this bucket's state.
	f.bucket(peerProject, "peer-bucket", 200)
	peer := decodeBody(t, f.request("admin@project-b.test", "GET", "/storage/v1/b/peer-bucket/folders", nil, 200).Body.Bytes())
	if items, _ := peer["folders"].([]any); len(items) != 0 {
		t.Fatalf("another bucket listed these folders: %v", peer)
	}

	f.request(admin, "DELETE", folders+"/reports", nil, 204)
	f.request(admin, "DELETE", folders+"/reports", nil, 404)
	f.request(admin, "GET", folders+"/reports", nil, 404)
}

// TestDeletedBucketDropsItsFolders proves a bucket recreated under the same
// name does not inherit the deleted bucket's folders.
func TestDeletedBucketDropsItsFolders(t *testing.T) {
	f := setup(t)
	f.bucket(testProject, "reused-bucket", 200)
	f.request(admin, "POST", "/storage/v1/b/reused-bucket/folders", []byte(`{"name":"stale"}`), 200)
	f.request(admin, "DELETE", "/storage/v1/b/reused-bucket", nil, 204)
	f.bucket(testProject, "reused-bucket", 200)
	f.request(admin, "GET", "/storage/v1/b/reused-bucket/folders/stale", nil, 404)
}

// TestCatalogFolderAndMetadataOwnership covers the catalog's identity and
// ownership preconditions and metadata replacement directly.
func TestCatalogFolderAndMetadataOwnership(t *testing.T) {
	catalog := NewCatalog(state.NewIndex())
	a := state.Scope{Project: testProject, Location: state.Global}
	if err := catalog.UpdateMetadata(a, absentBucket, "{}"); !errors.Is(err, errBucketAbsent) {
		t.Fatalf("update of an absent bucket = %v", err)
	}
	if err := catalog.PublishFolder(a, Folder{Bucket: absentBucket, Name: "f"}); !errors.Is(err, errBucketAbsent) {
		t.Fatalf("folder in an absent bucket = %v", err)
	}
	if err := catalog.PublishFolder(a, Folder{Bucket: "x"}); !errors.Is(err, errBucketIdentity) {
		t.Fatalf("unnamed folder = %v", err)
	}
	if err := catalog.PublishBucket(a, Bucket{Name: catalogBucket}); !errors.Is(err, errBucketIdentity) {
		t.Fatalf("bucket without placement = %v", err)
	}
	if err := catalog.PublishBucket(a, Bucket{Name: catalogBucket, Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.UpdateMetadata(a, catalogBucket, `{"labels":{}}`); err != nil {
		t.Fatal(err)
	}
	if bucket, _, _ := catalog.Bucket(a, catalogBucket); bucket.Metadata != `{"labels":{}}` {
		t.Fatalf("metadata = %q", bucket.Metadata)
	}
	if _, _, err := catalog.Bucket(state.Scope{Project: testProject, Location: "asia-east1"}, catalogBucket); err == nil {
		t.Fatal("a regional scope was accepted for global bucket state")
	}
}

// TestCatalogFoldersStayInTheirProject covers folder publication, isolation,
// and reset.
func TestCatalogFoldersStayInTheirProject(t *testing.T) {
	catalog := NewCatalog(state.NewIndex())
	a, b := state.Scope{Project: testProject, Location: state.Global}, state.Scope{Project: peerProject, Location: state.Global}
	if err := catalog.PublishBucket(a, Bucket{Name: catalogBucket, Location: "US"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishFolder(a, Folder{Bucket: catalogBucket, Name: "f"}); err != nil {
		t.Fatal(err)
	}
	if err := catalog.PublishFolder(b, Folder{Bucket: catalogBucket, Name: "f"}); !errors.Is(err, errBucketAbsent) {
		t.Fatalf("another project published into this bucket: %v", err)
	}
	if _, found, _ := catalog.Folder(b, catalogBucket, "f"); found {
		t.Fatal("another project read this folder")
	}
	if removed, _ := catalog.DeleteFolder(b, catalogBucket, "f"); removed {
		t.Fatal("another project deleted this folder")
	}
	if items, _ := catalog.Folders(a, catalogBucket); len(items) != 1 {
		t.Fatalf("folders = %v", items)
	}
	if err := catalog.Reset(a); err != nil {
		t.Fatal(err)
	}
	if items, _ := catalog.Folders(a, catalogBucket); len(items) != 0 {
		t.Fatal("reset retained folders")
	}
}
