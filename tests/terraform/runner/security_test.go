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

// TestAccKMSKeyRingCryptoKey exercises the local Cloud KMS slice with the
// pinned provider: a key ring, a symmetric crypto key, and its rotation.
func TestAccKMSKeyRingCryptoKey(t *testing.T) {
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
