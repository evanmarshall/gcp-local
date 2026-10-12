package main

import (
	"context"
	"fmt"

	scheduler "cloud.google.com/go/scheduler/apiv1"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
)

// runScheduler creates a yearly HTTP job, reads, lists, pauses, and resumes
// it, is refused as the ungranted identity, and deletes it. The job never
// runs, so its unreachable target is never called.
func runScheduler(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := scheduler.NewCloudSchedulerClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	location := "projects/" + e.project + "/locations/us-central1"
	job, err := client.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: location, Job: &schedulerpb.Job{
		Name: location + "/jobs/go-matrix", Schedule: "0 0 1 1 *", TimeZone: "America/New_York",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{
			Uri: "http://127.0.0.1:9/job", HttpMethod: schedulerpb.HttpMethod_POST, Body: []byte("hello from go"),
		}},
	}})
	if err != nil {
		return step("create job", err)
	}
	if got, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: job.GetName()}); err != nil || got.GetState() != schedulerpb.Job_ENABLED {
		return fmt.Errorf("get job: got %v, %v", got, err)
	}
	if err := listed(client.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: location}).Next, job.GetName(), (*schedulerpb.Job).GetName); err != nil {
		return step("list jobs", err)
	}
	if paused, err := client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: job.GetName()}); err != nil || paused.GetState() != schedulerpb.Job_PAUSED {
		return fmt.Errorf("pause job: got %v, %v", paused, err)
	}
	if _, err := client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: job.GetName()}); err != nil {
		return step("resume job", err)
	}
	deniedConn, err := e.grpcOption(e.denied)
	if err != nil {
		return err
	}
	denied, err := scheduler.NewCloudSchedulerClient(ctx, deniedConn)
	if err != nil {
		return step("new ungranted client", err)
	}
	defer denied.Close()
	_, err = denied.RunJob(ctx, &schedulerpb.RunJobRequest{Name: job.GetName()})
	if err := expectDenied("ungranted run job", err); err != nil {
		return err
	}
	return step("delete job", client.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: job.GetName()}))
}
