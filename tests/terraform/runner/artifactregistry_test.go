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

// localArtifactRegistryCase drives one Artifact Registry case through the adapter plumbing.
func localArtifactRegistryCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 artifact_registry_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// arName mints one owned resource name part.
func arName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccArtifactRegistryRepository creates a repository and destroys it.
func TestAccArtifactRegistryRepository(t *testing.T) {
	name := arName()

	localArtifactRegistryCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccArtifactRegistryDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testArtifactRegistryConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_artifact_registry_repository.repository", "format", "DOCKER"),
				),
			},
		},
	})
}

// testArtifactRegistryConfig renders one Docker repository.
func testArtifactRegistryConfig(name string) string {
	return fmt.Sprintf(`
resource "google_artifact_registry_repository" "repository" {
 provider = google
 repository_id = "%s"
 location      = "us-central1"
 format        = "DOCKER"
 description   = "acceptance"
 labels = {
   "gcp-local-run" = "terraform"
 }
}

resource "google_artifact_registry_repository_iam_member" "member" {
 repository = google_artifact_registry_repository.repository.name
 location   = "us-central1"
 project    = %q
 role       = "roles/artifactregistry.reader"
 member     = "serviceAccount:fixture@gcp-local-test.iam.gserviceaccount.com"
}
`, name, projectID)
}

// testAccArtifactRegistryDestroy verifies the repository is gone.
func testAccArtifactRegistryDestroy(t *testing.T, name string) func(*terraform.State) error {
	t.Helper()
	return func(_ *terraform.State) error {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
			fmt.Sprintf("%s/v1/projects/%s/locations/us-central1/repositories/%s", endpoint, projectID, name), nil)
		if err != nil {
			return fmt.Errorf("build the probe: %w", err)
		}
		request.Header.Set("Authorization", "Bearer "+fixtureToken)
		client := &http.Client{Timeout: 5 * time.Second}
		response, err := client.Do(request)
		if err != nil {
			return fmt.Errorf("probe the repository: %w", err)
		}
		defer response.Body.Close()
		if response.StatusCode != http.StatusNotFound {
			return fmt.Errorf("repository %s still exists (status %d)", name, response.StatusCode)
		}
		return nil
	}
}
