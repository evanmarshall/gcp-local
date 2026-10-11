package terraform_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

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

// TestAccSpannerInstanceAndDatabase creates an instance, a database, and destroys both.
func TestAccSpannerInstanceAndDatabase(t *testing.T) {
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
