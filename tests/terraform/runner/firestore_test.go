package terraform_test

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localFirestoreCase drives one Firestore case through the adapter plumbing.
// The first document request starts the emulator's JVM, which takes over two
// seconds on hosted runners; a shorter timeout cancels a create the emulator
// still applies, and the provider's retry then fails with 409.
func localFirestoreCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 firestore_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "30s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// firestoreName mints one owned document name part.
func firestoreName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccFirestoreDocument creates one document, reads it back, and destroys it.
func TestAccFirestoreDocument(t *testing.T) {
	docID := firestoreName()

	localFirestoreCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccFirestoreDestroy(t, docID),
		Steps: []resource.TestStep{
			{
				Config: testFirestoreConfig(docID),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_firestore_document.document", "document_id", docID),
					resource.TestCheckResourceAttr("google_firestore_field.field", "field", "title"),
					resource.TestCheckResourceAttrSet("google_firestore_document.document", "create_time"),
				),
			},
		},
	})
}

// testFirestoreConfig renders one document in the fixture collection.
func testFirestoreConfig(docID string) string {
	return fmt.Sprintf(`
resource "google_firestore_document" "document" {
 project     = %q
 collection  = "gcp-local-collection"
 document_id = %q
 fields      = jsonencode({title = {stringValue = "fire-and-fog"}})
}

resource "google_firestore_field" "field" {
 project  = %q
 database = "(default)"
 collection = "gcp-local-collection"
 field    = "title"
 ttl_config {}
}
`, projectID, docID, projectID)
}

// firestoreAPI reads one local Firestore path as the fixture principal.
func firestoreAPI(path string) (int, error) {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint+path, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	response, err := httpClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 1<<10))
	return response.StatusCode, nil
}

// testAccFirestoreDestroy requires the document to be gone.
func testAccFirestoreDestroy(t *testing.T, docID string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := firestoreAPI("/v1/projects/" + projectID + "/databases/(default)/documents/gcp-local-collection/" + docID)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("document read after destroy = %d", code)
		}
		return nil
	}
}
