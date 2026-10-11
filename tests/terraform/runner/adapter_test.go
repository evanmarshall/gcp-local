package terraform_test

import (
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

const projectID = "gcp-local-test"
const endpoint = "http://127.0.0.1:4566"
const fixtureToken = "gcp-local-test:fixture"

var httpClient = &http.Client{Transport: &http.Transport{Proxy: nil}, Timeout: 5 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return fmt.Errorf("redirect forbidden") }}

func preCheck(t *testing.T) {
	t.Helper()
	interfaces, err := net.Interfaces()
	if err != nil || len(interfaces) != 1 || interfaces[0].Name != "lo" || os.Getenv("GCP_LOCAL_ISOLATED") != "1" {
		t.Fatal("Terraform tests require the isolated local runner")
	}
	if os.Getenv("TF_ACC") != "1" || os.Getenv("TF_ACC_TERRAFORM_PATH") == "" || os.Getenv("TF_CLI_CONFIG_FILE") == "" {
		t.Fatal("Use the pinned Terraform runner configuration")
	}
	for _, key := range []string{"VCR_MODE", "VCR_PATH", "GOOGLE_APPLICATION_CREDENTIALS", "GOOGLE_CREDENTIALS", "GOOGLE_IMPERSONATE_SERVICE_ACCOUNT"} {
		if os.Getenv(key) != "" {
			t.Fatal("Ambient credentials/VCR are forbidden")
		}
	}
}
func pinnedProvider() map[string]resource.ExternalProvider {
	return map[string]resource.ExternalProvider{"google": {Source: "hashicorp/google", VersionConstraint: "=8.5.0"}}
}
func api(method, path string) (int, map[string]any, error) {
	if !strings.HasPrefix(path, "/storage/v1/") {
		return 0, nil, fmt.Errorf("invalid local API path")
	}
	r, err := http.NewRequest(method, endpoint+path, nil)
	if err != nil {
		return 0, nil, err
	}
	r.Header.Set("Authorization", "Bearer "+fixtureToken)
	response, err := httpClient.Do(r)
	if err != nil {
		return 0, nil, err
	}
	defer response.Body.Close()
	payload, err := io.ReadAll(io.LimitReader(response.Body, 2<<20))
	if err != nil {
		return 0, nil, err
	}
	value := map[string]any{}
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &value); err != nil {
			return response.StatusCode, nil, err
		}
	}
	return response.StatusCode, value, nil
}
func ownedBucket(t *testing.T) string {
	t.Helper()
	preCheck(t)
	code, listing, err := api("GET", "/storage/v1/b?project="+projectID+"&maxResults=2")
	if err != nil || code != 200 {
		t.Fatal("Cannot inspect local fixture project")
	}
	if items, _ := listing["items"].([]any); len(items) > 0 {
		t.Fatal("Fixture project is already occupied")
	}
	raw := make([]byte, 12)
	if _, err := rand.Read(raw); err != nil {
		t.Fatal(err)
	}
	name := "gcp-local-tf-" + hex.EncodeToString(raw)
	directory := os.Getenv("GCP_LOCAL_TF_ARTIFACTS")
	if directory == "" {
		t.Fatal("Explicit fixture artifacts directory is required")
	}
	manifest := filepath.Join(directory, name+".json")
	write := func(cleanup string) {
		data, _ := json.MarshalIndent(map[string]string{"project": projectID, "bucket": name, "test": t.Name(), "cleanup": cleanup}, "", "  ")
		if err := os.WriteFile(manifest, append(data, '\n'), 0600); err != nil {
			t.Error(err)
		}
	}
	write("pending")
	t.Cleanup(func() {
		code, bucket, err := api("GET", "/storage/v1/b/"+name)
		if err != nil {
			t.Error(err)
			return
		}
		if code == 404 {
			write("passed")
			return
		}
		if code != 200 || bucket["projectNumber"] != "100000000001" || bucket["name"] != name {
			t.Error("Fixture ownership mismatch")
			return
		}
		labels, _ := bucket["labels"].(map[string]any)
		if labels["gcp-local-run"] != name {
			t.Error("Fixture ownership label missing")
			return
		}
		code, objects, err := api("GET", "/storage/v1/b/"+name+"/o?maxResults=11")
		items, _ := objects["items"].([]any)
		if err != nil || code != 200 || len(items) > 10 || objects["nextPageToken"] != nil {
			t.Error("Cleanup object bounds exceeded")
			return
		}
		for _, item := range items {
			object := item.(map[string]any)
			if object["name"] != objectName {
				t.Error("Unowned object in fixture bucket")
				return
			}
			code, _, err = api("DELETE", "/storage/v1/b/"+name+"/o/"+url.PathEscape(objectName)+"?ifGenerationMatch="+object["generation"].(string))
			if err != nil || code != 204 {
				t.Error("Object cleanup failed")
				return
			}
		}
		code, _, err = api("DELETE", "/storage/v1/b/"+name)
		if err != nil || code != 204 {
			t.Error("Bucket cleanup failed")
			return
		}
		code, _, err = api("GET", "/storage/v1/b/"+name)
		if err != nil || code != 404 {
			t.Error("Bucket cleanup was not confirmed")
			return
		}
		write("passed")
	})
	return name
}
func localCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	steps := []resource.TestStep{}
	bucketName := ""
	for _, step := range test.Steps {
		if step.Config != "" {
			name := regexp.MustCompile(`name\s*=\s*"(gcp-local-tf-[0-9a-f]+)"`).FindStringSubmatch(step.Config)
			if len(name) != 2 {
				t.Fatal("Fixture bucket is not owned")
			}
			bucketName = name[1]
			config := regexp.MustCompile(`location\s*=\s*"US"`).ReplaceAllString(step.Config, `location = "us-central1"`)
			config = strings.Replace(config, `resource "google_storage_bucket" "bucket" {`, `resource "google_storage_bucket" "bucket" {
 project = "gcp-local-test"
 timeouts {
  create = "10s"
  update = "10s"
 }
 uniform_bucket_level_access = true
 public_access_prevention = "enforced"
 soft_delete_policy { retention_duration_seconds = 0 }
`, 1)
			// Provider default labels remain present while upstream labels are removed.
			step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 storage_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 default_labels = { "gcp-local-run" = %q }
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/storage/v1/", name[1]) + config
			step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		}
		if step.ImportState && step.ImportStateId == "" {
			// Qualified imports avoid the provider's auxiliary Compute lookup.
			step.ImportStateId = projectID + "/" + bucketName
		}
		if step.ImportState {
			// Provider default labels are not configuration-owned after import.
			step.ImportStateVerifyIgnore = append(step.ImportStateVerifyIgnore, "terraform_labels")
		}
		steps = append(steps, step)
		if step.Config != "" {
			steps = append(steps, resource.TestStep{RefreshState: true, RefreshPlanChecks: resource.RefreshPlanChecks{PostRefresh: []plancheck.PlanCheck{plancheck.ExpectEmptyPlan()}}})
			steps = append(steps, resource.TestStep{Config: step.Config, PlanOnly: true, ConfigPlanChecks: resource.ConfigPlanChecks{PostApplyPostRefresh: []plancheck.PlanCheck{plancheck.ExpectEmptyPlan()}}})
		}
	}
	test.Steps = steps
	resource.Test(t, test)
}
func testAccStorageBucketDestroyProducer(t *testing.T) func(*terraform.State) error {
	return func(s *terraform.State) error {
		for _, r := range s.RootModule().Resources {
			if r.Type == "google_storage_bucket" {
				code, _, err := api("GET", "/storage/v1/b/"+url.PathEscape(r.Primary.ID))
				if err != nil || code != 404 {
					return fmt.Errorf("bucket destroy was not confirmed")
				}
			}
		}
		return nil
	}
}
func testAccStorageObjectDestroyProducer(t *testing.T) func(*terraform.State) error {
	return func(s *terraform.State) error {
		for _, r := range s.RootModule().Resources {
			if r.Type == "google_storage_bucket_object" {
				code, _, err := api("GET", "/storage/v1/b/"+url.PathEscape(r.Primary.Attributes["bucket"])+"/o/"+url.PathEscape(r.Primary.Attributes["name"]))
				if err != nil || code != 404 {
					return fmt.Errorf("object destroy was not confirmed")
				}
			}
		}
		return testAccStorageBucketDestroyProducer(t)(s)
	}
}
func testAccCheckGoogleStorageObjectCrc32cHash(t *testing.T, bucket, object, checksum string) resource.TestCheckFunc {
	return func(*terraform.State) error {
		code, value, err := api("GET", "/storage/v1/b/"+url.PathEscape(bucket)+"/o/"+url.PathEscape(object))
		if err != nil || code != 200 {
			return fmt.Errorf("object metadata unavailable")
		}
		if value["crc32c"] != checksum {
			return fmt.Errorf("object contents do not match the expected CRC32C")
		}
		return nil
	}
}
func getNewTmpTestFile(t *testing.T, prefix string) *os.File {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), prefix)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { file.Close() })
	return file
}
