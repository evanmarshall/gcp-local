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
