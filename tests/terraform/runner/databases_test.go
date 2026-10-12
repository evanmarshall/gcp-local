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

// localBigQueryCase drives one BigQuery case through the adapter plumbing.
func localBigQueryCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 big_query_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/bigquery/v2/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// bqName mints one owned resource name part.
func bqName() string {
	return "gcp_local_tf_" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalBigQueryDatasetAndTable creates a dataset, a table, and destroys both.
func TestLocalBigQueryDatasetAndTable(t *testing.T) {
	name := bqName()

	localBigQueryCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccBigQueryDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testBigQueryConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_bigquery_dataset.dataset", "dataset_id", name),
					resource.TestCheckResourceAttr("google_bigquery_table.table", "table_id", "events"),
				),
			},
		},
	})
}

// testBigQueryConfig renders one dataset plus a table with a schema.
func testBigQueryConfig(name string) string {
	return fmt.Sprintf(`
resource "google_bigquery_dataset" "dataset" {
 dataset_id    = "%s"
 friendly_name = "acceptance"
 delete_contents_on_destroy = true
}

resource "google_bigquery_table" "table" {
 dataset_id = google_bigquery_dataset.dataset.dataset_id
 table_id   = "events"
 deletion_protection = false
 schema = <<EOS
[
 {"name": "title", "type": "STRING"}
]
EOS
}

`, name)
}

// bqAPI reads one local BigQuery path as the fixture principal.
func bqAPI(path string) (int, error) {
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

// testAccBigQueryDestroy requires the dataset to be gone.
func testAccBigQueryDestroy(t *testing.T, name string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := bqAPI("/bigquery/v2/projects/" + projectID + "/datasets/" + name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("dataset read after destroy = %d", code)
		}
		return nil
	}
}

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

// TestLocalFirestoreDocument creates one document, reads it back, and destroys it.
func TestLocalFirestoreDocument(t *testing.T) {
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

// localSpannerCase drives one Spanner case through the adapter plumbing.
func localSpannerCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 spanner_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// spannerName mints one owned resource name part.
func spannerName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalSpannerInstanceAndDatabase creates an instance, a database, and destroys both.
func TestLocalSpannerInstanceAndDatabase(t *testing.T) {
	name := spannerName()

	localSpannerCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccSpannerDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testSpannerConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_spanner_instance.instance", "name", name),
					resource.TestCheckResourceAttr("google_spanner_database.database", "name", "events"),
				),
			},
		},
	})
}

// testSpannerConfig renders one instance plus a database.
func testSpannerConfig(name string) string {
	return fmt.Sprintf(`
resource "google_spanner_instance" "instance" {
 name           = "%s"
 config         = "regional-us-central1"
 display_name   = "acceptance"
 processing_units = 1000
 force_destroy  = true
}

resource "google_spanner_database" "database" {
 instance     = google_spanner_instance.instance.name
 name         = "events"
 ddl          = ["CREATE TABLE entries (id INT64) PRIMARY KEY(id)"]
 deletion_protection = false
}
`, name)
}

// testAccSpannerDestroy verifies both resources are gone.
func testAccSpannerDestroy(t *testing.T, name string) func(*terraform.State) error {
	t.Helper()
	return func(state *terraform.State) error {
		client := &http.Client{Timeout: 5 * time.Second}
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		request, err := http.NewRequestWithContext(ctx, http.MethodGet,
			fmt.Sprintf("%s/v1/projects/%s/instances/%s", endpoint, projectID, name), nil)
		if err != nil {
			return fmt.Errorf("build the instance probe: %w", err)
		}
		request.Header.Set("Authorization", "Bearer "+fixtureToken)
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("probe the instance: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("instance %s still exists (status %d)", name, response.StatusCode)
		}
		return nil
	}
}
