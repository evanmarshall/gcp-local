package terraform_test

import (
	"fmt"
	"testing"
	"time"

	"github.com/hashicorp/terraform-plugin-testing/helper/resource"
	"github.com/hashicorp/terraform-plugin-testing/plancheck"
	"github.com/hashicorp/terraform-plugin-testing/terraform"
)

// localSchedulerCase drives one Cloud Scheduler case through the adapter
// plumbing; the Pub/Sub endpoint serves the job's topic.
func localSchedulerCase(t *testing.T, test resource.TestCase) {
	t.Helper()
	for i, step := range test.Steps {
		if step.Config == "" {
			continue
		}
		step.ConfigPlanChecks.PostApplyPostRefresh = append(step.ConfigPlanChecks.PostApplyPostRefresh, plancheck.ExpectEmptyPlan())
		step.Config = fmt.Sprintf(`provider "google" {
 project = %q
 access_token = %q
 cloud_scheduler_custom_endpoint = %q
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

// TestLocalCloudSchedulerJob creates a yearly Pub/Sub target job, updates its
// description and schedule and pauses it, then destroys it. The job never
// runs inside the test.
func TestLocalCloudSchedulerJob(t *testing.T) {
	name := "gcp-local-tf-job-" + fmt.Sprintf("%d", time.Now().UnixNano()%1_000_000)

	localSchedulerCase(t, resource.TestCase{
		PreCheck:          func() { preCheck(t) },
		ExternalProviders: pinnedProvider(),
		CheckDestroy:      testAccJobDestroy(t, name),
		Steps: []resource.TestStep{
			{
				Config: testJobConfig(name, "gcp-local", "0 0 1 1 *", false),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_cloud_scheduler_job.job", "state", "ENABLED"),
					resource.TestCheckResourceAttr("google_cloud_scheduler_job.job", "retry_config.0.retry_count", "1"),
					resource.TestCheckResourceAttr("google_cloud_scheduler_job.job", "retry_config.0.min_backoff_duration", "5s"),
				),
			},
			{
				Config: testJobConfig(name, "updated", "30 2 1 1 *", true),
				Check: resource.ComposeTestCheckFunc(
					resource.TestCheckResourceAttr("google_cloud_scheduler_job.job", "description", "updated"),
					resource.TestCheckResourceAttr("google_cloud_scheduler_job.job", "schedule", "30 2 1 1 *"),
					resource.TestCheckResourceAttr("google_cloud_scheduler_job.job", "state", "PAUSED"),
				),
			},
		},
	})
}

// testJobConfig renders the topic and one Pub/Sub target job.
func testJobConfig(name, description, schedule string, paused bool) string {
	return fmt.Sprintf(`
resource "google_pubsub_topic" "topic" {
 name = "%[1]s-topic"
}

resource "google_cloud_scheduler_job" "job" {
 name        = "%[1]s"
 region      = "us-central1"
 description = "%[2]s"
 schedule    = "%[3]s"
 time_zone   = "America/New_York"
 paused      = %[4]t

 retry_config {
  retry_count = 1
 }

 pubsub_target {
  topic_name = google_pubsub_topic.topic.id
  data       = base64encode("gcp-local")
  attributes = {
   origin = "gcp-local"
  }
 }
}
`, name, description, schedule, paused)
}

// testAccJobDestroy requires the job to be gone after destroy.
func testAccJobDestroy(t *testing.T, name string) func(*terraform.State) error {
	return func(_ *terraform.State) error {
		code, err := tasksAPI("/v1/projects/" + projectID + "/locations/us-central1/jobs/" + name)
		if err != nil {
			return err
		}
		if code != 404 {
			t.Errorf("job read after destroy = %d", code)
		}
		return nil
	}
}
