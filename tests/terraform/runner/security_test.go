package terraform_test

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localKMSCase drives one KMS test case through the same adapter plumbing as
// the storage cases: the pinned provider pointed at the local endpoint.
func localKMSCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 kms_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// kmsName mints one owned resource name.
func kmsName(prefix string) string {
	return "gcp-local-tf-" + prefix + "-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalKMSKeyRingCryptoKey exercises the local Cloud KMS slice with the
// pinned provider: a key ring, a symmetric crypto key, and its rotation.
func TestLocalKMSKeyRingCryptoKey(t *testing.T) {
	ringName := kmsName("ring")
	keyName := kmsName("key")

	localKMSCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccKMSDestroy(t, ringName, keyName),
		Steps: []resource.TestStep{
			{
				Config: testKMSConfig(ringName, keyName),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_kms_key_ring.ring", "name", ringName),
					resource.TestCheckResourceAttr("google_kms_crypto_key.key", "name", keyName),
					resource.TestCheckResourceAttr("google_kms_crypto_key.key", "rotation_period", "7776000s"),
					resource.TestCheckResourceAttr("google_kms_key_ring_iam_member.member", "role", "roles/cloudkms.cryptoKeyEncrypterDecrypter"),
				),
			},
		},
	})
}

// testKMSConfig renders one key ring plus crypto key with a rotation period.
func testKMSConfig(ringName, keyName string) string {
	return fmt.Sprintf(`
resource "google_kms_key_ring" "ring" {
 name     = "%s"
 location = "us-central1"
}

resource "google_kms_crypto_key" "key" {
 name           = "%s"
 key_ring       = google_kms_key_ring.ring.id
 rotation_period = "7776000s"
}

resource "google_kms_key_ring_iam_member" "member" {
 key_ring_id = google_kms_key_ring.ring.id
 role        = "roles/cloudkms.cryptoKeyEncrypterDecrypter"
 member      = "serviceAccount:fixture@gcp-local-test.iam.gserviceaccount.com"
}

resource "google_kms_crypto_key_iam_member" "key_member" {
 crypto_key_id = google_kms_crypto_key.key.id
 role          = "roles/cloudkms.cryptoKeyEncrypter"
 member        = "serviceAccount:fixture@gcp-local-test.iam.gserviceaccount.com"
}
`, ringName, keyName)
}

// kmsAPI reads one local KMS path as the fixture principal.
func kmsAPI(path string) (int, error) {
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

// testAccKMSDestroy requires the crypto key's versions to be destroyed. Key
// rings cannot be deleted, matching the real surface.
func testAccKMSDestroy(t *testing.T, ringName, keyName string) func(*terraform.State) error {
	return func(state *terraform.State) error {
		code, err := kmsAPI("/v1/projects/" + projectID + "/locations/us-central1/keyRings/" + ringName + "/cryptoKeys/" + keyName)
		if err != nil {
			return err
		}
		if code != 200 {
			t.Errorf("crypto key read after destroy = %d", code)
		}
		code, err = kmsAPI("/v1/projects/" + projectID + "/locations/us-central1/keyRings/" + ringName + "/cryptoKeys/" + keyName + "/cryptoKeyVersions")
		if err != nil {
			return err
		}
		if code != 200 {
			t.Errorf("version list after destroy = %d", code)
		}
		return nil
	}
}

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

// TestLocalSecretManagerSecretAndVersion creates a secret with a replication
// policy, adds a version, reads it back, and destroys both.
func TestLocalSecretManagerSecretAndVersion(t *testing.T) {
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

// localIAMCase drives one IAM case (service accounts, custom roles, and
// their project bindings) through the adapter.
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
 resource_manager_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/", endpoint+"/v1/", endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// saName mints one owned service-account id.
func saName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalServiceAccount creates a service account and destroys it.
func TestLocalServiceAccount(t *testing.T) {
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

resource "google_service_account_key" "key" {
 service_account_id = google_service_account.account.name
}
`, name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_service_account.account", "account_id", name),
				),
			},
		},
	})
}

// testCustomRoleConfig renders a project custom role and one project binding
// that grants it.
func testCustomRoleConfig(roleID, title string, permissions ...string) string {
	quoted := make([]string, len(permissions))
	for i, permission := range permissions {
		quoted[i] = fmt.Sprintf("%q", permission)
	}
	return fmt.Sprintf(`
resource "google_project_iam_custom_role" "role" {
 role_id     = "%s"
 title       = "%s"
 description = "gcp-local acceptance"
 stage       = "GA"
 permissions = [%s]
}

resource "google_project_iam_member" "member" {
 project = "%s"
 role    = google_project_iam_custom_role.role.name
 member  = "serviceAccount:allowed@%s.iam.gserviceaccount.com"
}
`, roleID, title, strings.Join(quoted, ", "), projectID, projectID)
}

// TestLocalProjectIamCustomRole creates a custom role and a binding to it,
// changes the role's title and permissions, and destroys both. The role is
// soft-deleted, so it reads back with deleted: true.
func TestLocalProjectIamCustomRole(t *testing.T) {
	roleID := "gcp_local_tf_" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
	name := "projects/" + projectID + "/roles/" + roleID
	localIAMCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testCustomRoleDestroyed(name),
		Steps: []resource.TestStep{
			{
				Config: testCustomRoleConfig(roleID, "Reader", "iam.serviceAccounts.get", "iam.serviceAccounts.list"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_project_iam_custom_role.role", "name", name),
					resource.TestCheckResourceAttr("google_project_iam_custom_role.role", "permissions.#", "2"),
					resource.TestCheckResourceAttr("google_project_iam_custom_role.role", "deleted", "false"),
					resource.TestCheckResourceAttr("google_project_iam_member.member", "role", name),
				),
			},
			{
				Config: testCustomRoleConfig(roleID, "Lister", "iam.serviceAccounts.list"),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_project_iam_custom_role.role", "title", "Lister"),
					resource.TestCheckResourceAttr("google_project_iam_custom_role.role", "permissions.#", "1"),
				),
			},
		},
	})
}

// testCustomRoleDestroyed requires the destroyed role to read back deleted.
func testCustomRoleDestroyed(name string) func(*terraform.State) error {
	return func(*terraform.State) error {
		request, err := http.NewRequestWithContext(context.Background(), http.MethodGet, endpoint+"/v1/"+name, nil)
		if err != nil {
			return err
		}
		request.Header.Set("Authorization", "Bearer "+fixtureToken)
		response, err := httpClient.Do(request)
		if err != nil {
			return err
		}
		defer response.Body.Close()
		var role struct {
			Deleted bool `json:"deleted"`
		}
		if err := json.NewDecoder(io.LimitReader(response.Body, 1<<16)).Decode(&role); err != nil || response.StatusCode != 200 || !role.Deleted {
			return fmt.Errorf("role %s after destroy: status %d, deleted %v, %v", name, response.StatusCode, role.Deleted, err)
		}
		return nil
	}
}
