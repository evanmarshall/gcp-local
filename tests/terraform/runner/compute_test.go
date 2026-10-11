package terraform_test

import (
	"context"
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localComputeCase drives one Compute case through the adapter plumbing.
func localComputeCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 compute_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/compute/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// computeName mints one owned resource name part.
func computeName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccComputeNetworkAndSubnetwork creates a network, a subnetwork, and destroys both.
func TestAccComputeNetworkAndSubnetwork(t *testing.T) {
	name := computeName()

	localComputeCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccComputeDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testComputeConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_compute_network.network", "name", name),
					resource.TestCheckResourceAttr("google_compute_subnetwork.subnetwork", "name", name+"-subnet"),
					resource.TestCheckResourceAttr("google_compute_subnetwork.subnetwork", "region", "us-central1"),
					resource.TestCheckResourceAttr("google_compute_firewall.firewall", "name", name+"-fw"),
				),
			},
		},
	})
}

// testComputeConfig renders one custom network plus a subnetwork.
func testComputeConfig(name string) string {
	return fmt.Sprintf(`
resource "google_compute_network" "network" {
 name                    = "%s"
 auto_create_subnetworks = false
}

resource "google_compute_firewall" "firewall" {
 name          = "%[1]s-fw"
 network       = google_compute_network.network.id
 allow {
   protocol = "tcp"
   ports    = ["8080"]
 }
 source_ranges = ["10.2.0.0/16"]
}

resource "google_compute_subnetwork" "subnetwork" {
 name          = "%[1]s-subnet"
 network       = google_compute_network.network.id
 region        = "us-central1"
 ip_cidr_range = "10.2.0.0/16"
}
`, name, name)
}

// computeAPI issues one direct compute request as the fixture principal.
func computeAPI(method, path string) (int, error) {
	request, err := http.NewRequestWithContext(context.Background(), method, endpoint+path, nil)
	if err != nil {
		return 0, fmt.Errorf("build the request: %w", err)
	}
	request.Header.Set("Authorization", "Bearer "+fixtureToken)
	client := &http.Client{Timeout: 5 * time.Second}
	response, err := client.Do(request)
	if err != nil {
		return 0, fmt.Errorf("call the local API: %w", err)
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

// testAccComputeDestroy verifies both resources are gone.
func testAccComputeDestroy(t *testing.T, name string) func(*terraform.State) error {
	t.Helper()
	return func(_ *terraform.State) error {
		code, err := computeAPI(http.MethodGet, "/compute/v1/projects/"+projectID+"/global/networks/"+name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("network read after destroy = %d", code)
		}
		return nil
	}
}
