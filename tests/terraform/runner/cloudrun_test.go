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

// TestLocalCloudRunV2Service creates a Cloud Run v2 service, splits its
// traffic between two revisions with a tag and scaling, then moves all
// traffic to the latest revision behind internal ingress without a new
// revision, and destroys it.
func TestLocalCloudRunV2Service(t *testing.T) {
	name := pubsubName()
	address := "google_cloud_run_v2_service.service"
	config := func(env, scaling, traffic, ingress string) string {
		return fmt.Sprintf(`resource "google_cloud_run_v2_service" "service" {
 name = %q
 location = "us-central1"
 deletion_protection = false
 %s
 template {
  %s
  containers {
   image = "us-docker.pkg.dev/cloudrun/container/hello"
   %s
  }
 }
 %s
}
`, name, ingress, scaling, env, traffic)
	}
	second := `env {
    name = "MODE"
    value = "second"
   }`
	scaling := `scaling {
   max_instance_count = 2
  }
  max_instance_request_concurrency = 10`
	split := fmt.Sprintf(`traffic {
  type = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION"
  revision = "%s-00001"
  percent = 50
  tag = "blue"
 }
 traffic {
  type = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  percent = 50
 }`, name)
	latest := `traffic {
  type = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
  percent = 100
 }`
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
			Config: config("", "", "", ""),
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr(address, "name", name),
				resource.TestCheckResourceAttr(address, "traffic_statuses.#", "1"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.0.revision", name+"-00001"),
			),
		}, {
			Config: config(second, scaling, split, ""),
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr(address, "latest_ready_revision", "projects/"+projectID+"/locations/us-central1/services/"+name+"/revisions/"+name+"-00002"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.#", "2"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.0.tag", "blue"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.0.percent", "50"),
				resource.TestCheckResourceAttrSet(address, "traffic_statuses.0.uri"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.1.revision", name+"-00002"),
				resource.TestCheckResourceAttr(address, "template.0.scaling.0.max_instance_count", "2"),
				resource.TestCheckResourceAttr(address, "template.0.max_instance_request_concurrency", "10"),
			),
		}, {
			Config: config(second, scaling, latest, `ingress = "INGRESS_TRAFFIC_INTERNAL_ONLY"`),
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr(address, "ingress", "INGRESS_TRAFFIC_INTERNAL_ONLY"),
				// Traffic and ingress alone create no revision.
				resource.TestCheckResourceAttr(address, "latest_created_revision", "projects/"+projectID+"/locations/us-central1/services/"+name+"/revisions/"+name+"-00002"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.#", "1"),
				resource.TestCheckResourceAttr(address, "traffic_statuses.0.percent", "100"),
			),
		}},
	})
}
