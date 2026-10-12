package terraform_test

import (
	"fmt"
	"net/http"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localCloudRunCase drives one Cloud Run v2 case. The v2 resources read the
// cloud_run_v2_custom_endpoint override; cloud_run_custom_endpoint only
// redirects the v1 API, which left the provider dialing run.googleapis.com.
func localCloudRunCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 cloud_run_v2_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "5s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v2/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// TestLocalCloudRunV2Service creates, reads, and destroys one Cloud Run v2 service.
func TestLocalCloudRunV2Service(t *testing.T) {
	name := pubsubName()
	localCloudRunCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy: func(*terraform.State) error {
			code, _, err := api(http.MethodGet, "/v2/projects/"+projectID+"/locations/us-central1/services/"+name)
			if err != nil || code != http.StatusNotFound {
				return fmt.Errorf("cloud run service %s survived destroy: %d %v", name, code, err)
			}
			return nil
		},
		Steps: []resource.TestStep{{
			Config: fmt.Sprintf(`resource "google_cloud_run_v2_service" "service" {
 name = %q
 location = "us-central1"
 deletion_protection = false
 template {
  containers {
   image = "us-docker.pkg.dev/cloudrun/container/hello"
  }
 }
}
`, name),
			Check: resource.TestCheckResourceAttr("google_cloud_run_v2_service.service", "name", name),
		}},
	})
}
