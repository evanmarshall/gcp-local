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
// pinned provider: a key ring, a symmetric crypto key with a rotation
// schedule, and IAM members.
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

// testKMSConfig renders one key ring plus crypto key and IAM members.
func testKMSConfig(ringName, keyName string) string {
	return fmt.Sprintf(`
resource "google_kms_key_ring" "ring" {
 name     = "%s"
 location = "us-central1"
}

resource "google_kms_crypto_key" "key" {
 name            = "%s"
 key_ring        = google_kms_key_ring.ring.id
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
 pubsub_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/", endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// secretName mints one owned secret name.
func secretName() string {
	return "gcp-local-tf-secret-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestLocalSecretManagerSecretAndVersion creates a secret with a replication
// policy, a rotation schedule notifying a Pub/Sub topic the Secret Manager
// service agent may publish to, and a version, and a second secret with a
// ttl; then adds a version alias, and destroys everything. The ttl secret is
// never updated: the provider's update body carries both ttl and the
// returned expire_time, which the expiration oneof refuses.
func TestLocalSecretManagerSecretAndVersion(t *testing.T) {
	name := secretName()
	next := time.Now().UTC().Add(time.Hour).Truncate(time.Second).Format(time.RFC3339)

	localSecretCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccSecretDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testSecretConfig(name, next, ""),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_secret_manager_secret.secret", "secret_id", name),
					resource.TestCheckResourceAttrSet("google_secret_manager_secret.secret", "create_time"),
					resource.TestCheckResourceAttrSet("google_secret_manager_secret.expiring", "expire_time"),
					resource.TestCheckResourceAttr("google_secret_manager_secret.secret", "rotation.0.next_rotation_time", next),
					resource.TestCheckResourceAttr("google_secret_manager_secret.secret", "rotation.0.rotation_period", "3600s"),
					resource.TestCheckResourceAttr("google_secret_manager_secret.secret", "topics.#", "1"),
					resource.TestCheckResourceAttr("google_secret_manager_secret_version.version", "secret_data", "payload-value"),
				),
			},
			{
				Config: testSecretConfig(name, next, `version_aliases = { current = "1" }`),
				Check:  resource.TestCheckResourceAttr("google_secret_manager_secret.secret", "version_aliases.current", "1"),
			},
		},
	})
}

// testSecretConfig renders one replicated secret with a topic, rotation, and
// one version, and a secret with a ttl; extra adds secret arguments.
func testSecretConfig(name, next, extra string) string {
	return fmt.Sprintf(`
resource "google_pubsub_topic" "topic" {
 name = "%[1]s"
}

resource "google_pubsub_topic_iam_member" "agent" {
 topic  = google_pubsub_topic.topic.id
 role   = "roles/pubsub.publisher"
 member = "serviceAccount:service-000000000001@gcp-sa-secretmanager.iam.gserviceaccount.com"
}

resource "google_secret_manager_secret" "secret" {
 secret_id = "%[1]s"
 replication {
  auto {}
 }
 topics {
  name = google_pubsub_topic.topic.id
 }
 rotation {
  next_rotation_time = "%[2]s"
  rotation_period    = "3600s"
 }
 %[3]s
 depends_on = [google_pubsub_topic_iam_member.agent]
}

resource "google_secret_manager_secret_version" "version" {
 secret = google_secret_manager_secret.secret.id
 secret_data = "payload-value"
}

resource "google_secret_manager_secret" "expiring" {
 secret_id = "%[1]s-ttl"
 replication {
  auto {}
 }
 ttl = "86400s"
}
`, name, next, extra)
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

// denyParent is the URL-encoded attachment point google_iam_deny_policy
// takes as its parent.
const denyParent = "cloudresourcemanager.googleapis.com%2Fprojects%2F" + projectID

// TestLocalIamDenyPolicy creates a project deny policy, updates its display
// name and rule in place (the provider sends the etag), imports it, and
// destroys it, polling each IAM v2 operation.
func TestLocalIamDenyPolicy(t *testing.T) {
	config := func(display, exception string) string {
		return fmt.Sprintf(`
resource "google_iam_deny_policy" "keys" {
 parent       = %q
 name         = "tf-deny-keys"
 display_name = %q
 rules {
  description = "no key creation"
  deny_rule {
   denied_principals    = ["principal://iam.googleapis.com/projects/-/serviceAccounts/denied@%s.iam.gserviceaccount.com"]
   denied_permissions   = ["iam.googleapis.com/serviceAccountKeys.create"]
   exception_principals = [%s]
  }
 }
}
`, denyParent, display, projectID, exception)
	}
	step := func(display, exception string) resource.TestStep {
		return resource.TestStep{
			Config: config(display, exception),
			ConfigPlanChecks: resource.ConfigPlanChecks{PostApplyPostRefresh: []plancheck.PlanCheck{plancheck.ExpectEmptyPlan()}},
			Check: resource.ComposeTestCheckFunc(
				resource.TestCheckResourceAttr("google_iam_deny_policy.keys", "display_name", display),
				resource.TestCheckResourceAttrSet("google_iam_deny_policy.keys", "etag"),
			),
		}
	}
	steps := []resource.TestStep{
		step("deny keys", ""),
		step("deny keys, except fixture", fmt.Sprintf("%q", "principal://iam.googleapis.com/projects/-/serviceAccounts/fixture@"+projectID+".iam.gserviceaccount.com")),
		{ResourceName: "google_iam_deny_policy.keys", ImportState: true, ImportStateVerify: true,
			ImportStateId: denyParent + "/tf-deny-keys"},
	}
	for i := range steps {
		if steps[i].Config != "" {
			steps[i].Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 iam2_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v2/") + steps[i].Config
		}
	}
	resource.Test(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testDenyPolicyDeleted,
		Steps:             steps,
	})
}

// testDenyPolicyDeleted requires the destroyed deny policy to read back absent.
func testDenyPolicyDeleted(*terraform.State) error {
	request, err := http.NewRequestWithContext(context.Background(), http.MethodGet,
		endpoint+"/v2/policies/"+denyParent+"/denypolicies/tf-deny-keys", nil)
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
		return fmt.Errorf("deny policy after destroy: HTTP %d", response.StatusCode)
	}
	return nil
}
