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
					resource.TestCheckResourceAttr("google_pubsub_topic.topic", "labels.gcp-local-run", "terraform"),
					resource.TestCheckResourceAttr("google_pubsub_subscription.subscription", "name", "fanout"),
					resource.TestCheckResourceAttr("google_pubsub_subscription.subscription", "labels.gcp-local-run", "terraform"),
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
 labels = {
   "gcp-local-run" = "terraform"
 }
}

resource "google_pubsub_topic_iam_member" "topic_member" {
 topic  = google_pubsub_topic.topic.name
 role   = "roles/pubsub.viewer"
 member = "serviceAccount:fixture@gcp-local-test.iam.gserviceaccount.com"
}

resource "google_pubsub_subscription" "subscription" {
 name  = "fanout"
 topic = google_pubsub_topic.topic.id
 message_retention_duration = "3600s"
 labels = {
   "gcp-local-run" = "terraform"
 }
}

resource "google_pubsub_subscription_iam_member" "member" {
 subscription = google_pubsub_subscription.subscription.name
 role         = "roles/pubsub.viewer"
 member       = "serviceAccount:fixture@gcp-local-test.iam.gserviceaccount.com"
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

// localTasksCase drives one Cloud Tasks case through the adapter plumbing.
func localTasksCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 cloud_tasks_custom_endpoint = %q
 universe_domain = "gcp-local.invalid"
 request_timeout = "2s"
 add_terraform_attribution_label = false
}
`, projectID, fixtureToken, endpoint+"/v2beta3/") + step.Config
		test.Steps[i] = step
	}
	resource.Test(t, test)
}

// queueName mints one owned queue name.
func queueName() string {
	return "gcp-local-tf-queue-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)
}

// TestAccCloudTasksQueueLifecycle creates a queue with rate limits, updates
// it, then destroys it.
func TestAccCloudTasksQueueLifecycle(t *testing.T) {
	name := queueName()

	localTasksCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccQueueDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testQueueConfig(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_cloud_tasks_queue.queue", "name", name),
					resource.TestCheckResourceAttr("google_cloud_tasks_queue.queue", "rate_limits.0.max_dispatches_per_second", "5"),
				),
			},
			{
				Config: testQueueConfigUpdated(name),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_cloud_tasks_queue.queue", "rate_limits.0.max_dispatches_per_second", "10"),
				),
			},
		},
	})
}

// testQueueConfig renders one queue with rate limits.
func testQueueConfig(name string) string {
	return fmt.Sprintf(`
resource "google_cloud_tasks_queue" "queue" {
 name = "%[1]s"
 location = "us-central1"
 rate_limits {
  max_dispatches_per_second = 5
  max_concurrent_dispatches = 2
 }
}

resource "google_cloud_tasks_queue_iam_member" "member" {
 name     = google_cloud_tasks_queue.queue.name
 location = "us-central1"
 role     = "roles/cloudtasks.enqueuer"
 member   = "serviceAccount:fixture@%[2]s.iam.gserviceaccount.com"
}
`, name, projectID)
}

// testQueueConfigUpdated raises the dispatch rate.
func testQueueConfigUpdated(name string) string {
	return fmt.Sprintf(`
resource "google_cloud_tasks_queue" "queue" {
 name = "%s"
 location = "us-central1"
 rate_limits {
  max_dispatches_per_second = 10
  max_concurrent_dispatches = 4
 }
}
`, name)
}

// tasksAPI reads one local Cloud Tasks path as the fixture principal.
func tasksAPI(path string) (int, error) {
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

// testAccQueueDestroy requires the queue to be gone after destroy.
func testAccQueueDestroy(t *testing.T, name string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := tasksAPI("/v2beta3/projects/" + projectID + "/locations/us-central1/queues/" + name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("queue read after destroy = %d", code)
		}
		return nil
	}
}
