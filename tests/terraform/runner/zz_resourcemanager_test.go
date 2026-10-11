package terraform_test

import (
	"fmt"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
)

// localResourceManagerCase drives one project-IAM case through the adapter.
func localResourceManagerCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 resource_manager_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// testProjectPolicy renders the project policy: the accepted binding plus the
// seeded service grants, because google_project_iam_policy replaces the whole
// policy and the other acceptance cases read their grants from it afterwards.
func testProjectPolicy() string {
	roles := []string{
		"roles/resourcemanager.projectIamAdmin", "roles/storage.admin",
		"roles/cloudkms.admin", "roles/secretmanager.admin", "roles/cloudtasks.admin",
		"roles/datastore.user", "roles/bigquery.dataEditor", "roles/spanner.admin",
		"roles/pubsub.admin", "roles/run.admin", "roles/compute.networkAdmin",
		"roles/artifactregistry.admin", "roles/bigtable.admin",
	}
	bindings := ""
	for _, role := range roles {
		bindings += fmt.Sprintf(`
 binding {
   role = %q
   members = ["serviceAccount:fixture@%s.iam.gserviceaccount.com"]
 }
`, role, projectID)
	}
	return fmt.Sprintf(`
resource "google_project_iam_policy" "policy" {
 project     = %q
 policy_data = data.google_iam_policy.policy.policy_data
}

data "google_iam_policy" "policy" {%s}
`, projectID, bindings)
}

// TestAccProjectIamPolicy sets and reads the project IAM policy.
func TestAccProjectIamPolicy(t *testing.T) {
	localResourceManagerCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		Steps: []resource.TestStep{
			{
				Config: testProjectPolicy(),
			},
		},
	})
}
