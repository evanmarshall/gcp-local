package terraform_test

import (
	"fmt"
	"net/http"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localPubSubCase drives one Pub/Sub case through the adapter plumbing.
func localPubSubCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 pubsub_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v1/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// pubsubName mints one owned resource name part.
func pubsubName() string {
	return "gcp-local-tf-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccPubSubTopicAndSubscription creates a topic, a subscription, and destroys both.
func TestAccPubSubTopicAndSubscription(t *testing.T) {
	name := pubsubName()

	localPubSubCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccPubSubDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testPubSubConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_pubsub_topic.topic", "name", name),
					resource.TestCheckResourceAttr("google_pubsub_subscription.subscription", "name", "fanout"),
				),
			},
		},
	})
}

// testPubSubConfig renders one topic plus a pull subscription.
func testPubSubConfig(name string) string {
	return fmt.Sprintf(`
resource "google_pubsub_topic" "topic" {
 name = "%s"
}

resource "google_pubsub_subscription" "subscription" {
 name  = "fanout"
 topic = google_pubsub_topic.topic.id
 message_retention_duration = "3600s"
}
`, name)
}

// pubsubAPI issues one direct JSON API request as the fixture principal.
func pubsubAPI(method, path string) (int, error) {
	request, err := http.NewRequest(method, endpoint+path, nil)
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

// testAccPubSubDestroy verifies both resources are gone.
func testAccPubSubDestroy(t *testing.T, name string) func(*terraform.State) error {
	t.Helper()
	return func(_ *terraform.State) error {
		code, err := pubsubAPI(http.MethodGet, "/v1/projects/"+projectID+"/topics/"+name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("topic read after destroy = %d", code)
		}
		return nil
	}
}
