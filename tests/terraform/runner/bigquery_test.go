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

// TestAccBigQueryDatasetAndTable creates a dataset, a table, and destroys both.
func TestAccBigQueryDatasetAndTable(t *testing.T) {
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
