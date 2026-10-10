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

// localSecretCase drives one Secret Manager case through the same adapter
// plumbing as the storage cases.
func localSecretCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 secret_manager_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// secretName mints one owned secret name.
func secretName() string {
	return "gcp-local-tf-secret-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccSecretManagerSecretAndVersion creates a secret with a replication
// policy, adds a version, reads it back, and destroys both.
func TestAccSecretManagerSecretAndVersion(t *testing.T) {
	name := secretName()

	localSecretCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccSecretDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testSecretConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_secret_manager_secret.secret", "secret_id", name),
					resource.TestCheckResourceAttrSet("google_secret_manager_secret.secret", "create_time"),
					resource.TestCheckResourceAttr("google_secret_manager_secret_version.version", "secret_data", "payload-value"),
				),
			},
		},
	})
}

// testSecretConfig renders one replicated secret with one version.
func testSecretConfig(name string) string {
	return fmt.Sprintf(`
resource "google_secret_manager_secret" "secret" {
 secret_id = "%s"
 replication {
  auto {}
 }
}

resource "google_secret_manager_secret_version" "version" {
 secret = google_secret_manager_secret.secret.id
 secret_data = "payload-value"
}
`, name)
}

// secretAPI reads one local Secret Manager path as the fixture principal.
func secretAPI(path string) (int, error) {
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

// testAccSecretDestroy requires the secret to be gone; versions go with it.
func testAccSecretDestroy(t *testing.T, name string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := secretAPI("/v1/projects/" + projectID + "/secrets/" + name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("secret read after destroy = %d", code)
		}
		return nil
	}
}
