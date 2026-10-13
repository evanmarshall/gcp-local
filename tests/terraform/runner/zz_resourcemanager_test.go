package terraform_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"regexp"
	"testing"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
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

// terraformFolder is the conformance server's seeded folder where the
// fixture holds roles/resourcemanager.folderAdmin.
const terraformFolder = "folders/271828182845"

// localFolderCase drives one folder case through the v1 (operation polling)
// and v3 (folders) Resource Manager endpoints.
func localFolderCase(t *testing.T, test resource.TestCase) {
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
 resource_manager_v3_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/", endpoint+"/v3/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// testFolderConfig renders one folder under the seeded folder and one
// folder IAM member on it.
func testFolderConfig(displayName string) string {
	return fmt.Sprintf(`
resource "google_folder" "folder" {
 display_name        = %q
 parent              = %q
 deletion_protection = false
}

resource "google_folder_iam_member" "viewer" {
 folder = google_folder.folder.name
 role   = "roles/resourcemanager.folderViewer"
 member = "serviceAccount:allowed@%s.iam.gserviceaccount.com"
}
`, displayName, terraformFolder, projectID)
}

// testFolderDeleted requires the destroyed folder to read back in
// DELETE_REQUESTED, as GCP keeps a deleted folder for 30 days.
func testFolderDeleted(state *terraform.State) error {
	for _, item := range state.RootModule().Resources {
		if item.Type != "google_folder" {
			continue
		}
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint+"/v3/"+item.Primary.ID, nil)
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+fixtureToken)
		response, err := httpClient.Do(request)
		if err != nil {
			return err
		}
		var folder struct {
			State string `json:"state"`
		}
		err = json.NewDecoder(io.LimitReader(response.Body, 1<<16)).Decode(&folder)
		response.Body.Close()
		if err != nil || response.StatusCode != http.StatusOK || folder.State != "DELETE_REQUESTED" {
			return fmt.Errorf("folder %s after destroy: HTTP %d, state %q, %v", item.Primary.ID, response.StatusCode, folder.State, err)
		}
	}
	return nil
}

// TestLocalFolderAndFolderIam creates a folder through a polled operation,
// grants a member on it, renames it, imports it, and destroys both.
func TestLocalFolderAndFolderIam(t *testing.T) {
	member := "serviceAccount:allowed@" + projectID + ".iam.gserviceaccount.com"
	localFolderCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testFolderDeleted,
		Steps: []resource.TestStep{
			{
				Config: testFolderConfig("gcp-local-tf-folder"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_folder.folder", "display_name", "gcp-local-tf-folder"),
					resource.TestCheckResourceAttr("google_folder.folder", "parent", terraformFolder),
					resource.TestCheckResourceAttr("google_folder.folder", "lifecycle_state", "ACTIVE"),
					resource.TestCheckResourceAttrSet("google_folder.folder", "folder_id"),
					resource.TestCheckResourceAttr("google_folder_iam_member.viewer", "member", member),
				),
			},
			{
				Config: testFolderConfig("gcp-local-tf-renamed"),
				Check:  resource.TestCheckResourceAttr("google_folder.folder", "display_name", "gcp-local-tf-renamed"),
			},
			{
				ResourceName:            "google_folder.folder",
				ImportState:             true,
				ImportStateVerify:       true,
				ImportStateVerifyIgnore: []string{"deletion_protection", "deletion_policy"},
			},
		},
	})
}

// testProjectPolicy renders the project policy: the accepted binding plus the
// seeded service grants, because google_project_iam_policy replaces the whole
// policy and the other acceptance cases read their grants from it afterwards.
func testProjectPolicy() string {
	roles := []string{
		"roles/resourcemanager.projectIamAdmin", "roles/storage.admin",
		"roles/cloudkms.admin", "roles/secretmanager.admin", "roles/cloudtasks.admin",
		"roles/datastore.user", "roles/datastore.indexAdmin", "roles/bigquery.dataOwner", "roles/bigquery.jobUser",
		"roles/spanner.admin", "roles/pubsub.admin", "roles/run.admin", "roles/compute.admin",
		"roles/artifactregistry.admin", "roles/bigtable.admin", "roles/networkmanagement.admin",
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

// TestLocalProjectIamPolicy sets and reads the project IAM policy.
func TestLocalProjectIamPolicy(t *testing.T) {
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

// terraformPolicyProject is the conformance server's project under the
// seeded folder, where the fixture holds Organization Policy Administrator
// and Storage Admin through the folder. No recording reads it.
const terraformPolicyProject = "gcp-local-tf-policy"

// localOrgPolicyCase drives one Organization Policy case through the v2 and
// Storage endpoints.
func localOrgPolicyCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		if step.ExpectError == nil {
			step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		}
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 org_policy_custom_endpoint = %q
 storage_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, terraformPolicyProject, fixtureToken, endpoint+"/v2/", endpoint+"/storage/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// testOrgPolicyConfig renders a gcp.resourceLocations policy on the project
// allowing the listed value groups and, when bucket is set, a bucket in
// europe-west1 created after it.
func testOrgPolicyConfig(bucket bool, allowed ...string) string {
	config := fmt.Sprintf(`
resource "google_org_policy_policy" "locations" {
 name   = "projects/%s/policies/gcp.resourceLocations"
 parent = "projects/%s"
 spec {
  rules {
   values {
    allowed_values = %s
   }
  }
 }
}
`, terraformPolicyProject, terraformPolicyProject, mustJSON(allowed))
	if bucket {
		config += fmt.Sprintf(`
resource "google_storage_bucket" "europe" {
 name                        = "gcp-local-tf-policy-europe"
 project                     = %q
 location                    = "EUROPE-WEST1"
 force_destroy               = true
 uniform_bucket_level_access = true
 public_access_prevention    = "enforced"
 soft_delete_policy { retention_duration_seconds = 0 }
 depends_on                  = [google_org_policy_policy.locations]
}
`, terraformPolicyProject)
	}
	return config
}

// mustJSON renders a string list as an HCL list.
func mustJSON(values []string) string {
	encoded, _ := json.Marshal(values)
	return string(encoded)
}

// testOrgPolicyDeleted requires the destroyed policy to read back absent.
func testOrgPolicyDeleted(*terraform.State) error {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		endpoint+"/v2/projects/"+terraformPolicyProject+"/policies/gcp.resourceLocations", nil)
	if err != nil {
		return err
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	response, err := httpClient.Do(request)
	if err != nil {
		return err
	}
	response.Body.Close()
	if response.StatusCode != http.StatusNotFound {
		return fmt.Errorf("policy after destroy: HTTP %d", response.StatusCode)
	}
	return nil
}

// TestLocalOrgPolicyProject sets a project resource-location policy, has it
// refuse a bucket in europe-west1, widens it so the same bucket is created,
// imports it, and destroys both.
func TestLocalOrgPolicyProject(t *testing.T) {
	localOrgPolicyCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testOrgPolicyDeleted,
		Steps: []resource.TestStep{
			{
				Config: testOrgPolicyConfig(false, "in:us-locations"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_org_policy_policy.locations", "spec.0.rules.0.values.0.allowed_values.0", "in:us-locations"),
					resource.TestCheckResourceAttrSet("google_org_policy_policy.locations", "spec.0.etag"),
				),
			},
			{
				Config:      testOrgPolicyConfig(true, "in:us-locations"),
				ExpectError: regexp.MustCompile(`Request violates constraint 'constraints/gcp.resourceLocations'`),
			},
			{
				Config: testOrgPolicyConfig(true, "in:us-locations", "in:europe-west1-locations"),
				Check:  resource.TestCheckResourceAttr("google_storage_bucket.europe", "location", "EUROPE-WEST1"),
			},
			{
				ResourceName:      "google_org_policy_policy.locations",
				ImportState:       true,
				ImportStateVerify: true,
			},
		},
	})
}
