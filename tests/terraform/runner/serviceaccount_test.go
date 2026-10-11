package terraform_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
)

// localIAMCase drives one service-account case through the adapter.
func localIAMCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 iam_custom_endpoint = %q
 iam_beta_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/", endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// saName mints one owned service-account id.
func saName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccServiceAccount creates a service account and destroys it.
func TestAccServiceAccount(t *testing.T) {
	name := saName()
	localIAMCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		Steps: []resource.TestStep{
			{
				Config: fmt.Sprintf(`
resource "google_service_account" "account" {
 account_id   = "%s"
 display_name = "acceptance"
}
`, name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_service_account.account", "account_id", name),
				),
			},
		},
	})
}
