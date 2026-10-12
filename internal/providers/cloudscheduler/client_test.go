package cloudscheduler

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	scheduler "cloud.google.com/go/scheduler/apiv1"
	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/local"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// bearerADC attaches a local ADC access token to each call over a local
// (loopback-only) channel, which carries no transport security.
type bearerADC struct{ credentials *auth.Credentials }

func (b bearerADC) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := b.credentials.Token(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // the test reports it
	}
	return map[string]string{"authorization": token.Type + " " + token.Value}, nil
}

func (bearerADC) RequireTransportSecurity() bool { return false }

// TestGoClientOverGRPCWithLocalADC drives cloud.google.com/go/scheduler
// (v1, default gRPC transport) with external-account ADC through the job
// lifecycle and a forced run to a loopback HTTP target, and is refused as an
// ungranted peer.
func TestGoClientOverGRPCWithLocalADC(t *testing.T) {
	const subject = "gls.synthetic-scheduler-go-subject"
	listeners := startRuntime(t, subject)
	delivered := make(chan string, 1)
	target := httptest.NewServer(http.HandlerFunc(func(_ http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		delivered <- r.Method + " " + string(body)
	}))
	defer target.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	sts := "http://" + listeners.HTTP.Addr().String()
	client := schedulerGoClient(ctx, t, writeADC(t, sts, subject), listeners.GRPC.Addr().String())
	parent := "projects/project-a/locations/us-central1"
	job := createAndRun(ctx, t, client, parent, target.URL)
	if got := <-delivered; got != "PUT go-job" {
		t.Fatalf("delivered %q", got)
	}
	changeAndList(ctx, t, client, parent, job.GetName())
	denied := schedulerGoClient(ctx, t, writeADC(t, sts, subject+"-peer"), listeners.GRPC.Addr().String())
	if _, err := denied.GetJob(ctx, &schedulerpb.GetJobRequest{Name: job.GetName()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("peer get = %v", err)
	}
	if err := client.DeleteJob(ctx, &schedulerpb.DeleteJobRequest{Name: job.GetName()}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetJob(ctx, &schedulerpb.GetJobRequest{Name: job.GetName()}); status.Code(err) != codes.NotFound {
		t.Fatalf("deleted get = %v", err)
	}
}

// createAndRun creates an HTTP job to target with the client and runs it.
func createAndRun(ctx context.Context, t *testing.T, client *scheduler.CloudSchedulerClient, parent, target string) *schedulerpb.Job {
	t.Helper()
	job, err := client.CreateJob(ctx, &schedulerpb.CreateJobRequest{Parent: parent, Job: &schedulerpb.Job{
		Name: parent + "/jobs/go-client", Schedule: "0 9 * * 1", TimeZone: "Europe/London",
		Target: &schedulerpb.Job_HttpTarget{HttpTarget: &schedulerpb.HttpTarget{Uri: target, HttpMethod: schedulerpb.HttpMethod_PUT, Body: []byte("go-job")}},
	}})
	must(t, err)
	if job.GetState() != schedulerpb.Job_ENABLED || job.GetAttemptDeadline().AsDuration() != 3*time.Minute || job.GetStatus().GetCode() != notAttempted {
		t.Fatalf("create job = %v", job)
	}
	_, err = client.RunJob(ctx, &schedulerpb.RunJobRequest{Name: job.GetName()})
	must(t, err)
	return job
}

// changeAndList updates, pauses, resumes, and lists the one job of parent.
func changeAndList(ctx context.Context, t *testing.T, client *scheduler.CloudSchedulerClient, parent, name string) {
	t.Helper()
	updated, err := client.UpdateJob(ctx, &schedulerpb.UpdateJobRequest{
		Job: &schedulerpb.Job{Name: name, Description: "weekly"}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"description"}},
	})
	must(t, err)
	if updated.GetDescription() != "weekly" || updated.GetSchedule() != "0 9 * * 1" {
		t.Fatalf("update job = %v", updated)
	}
	paused, err := client.PauseJob(ctx, &schedulerpb.PauseJobRequest{Name: name})
	must(t, err)
	resumed, err := client.ResumeJob(ctx, &schedulerpb.ResumeJobRequest{Name: name})
	must(t, err)
	if paused.GetState() != schedulerpb.Job_PAUSED || resumed.GetState() != schedulerpb.Job_ENABLED {
		t.Fatalf("pause %v then resume %v", paused.GetState(), resumed.GetState())
	}
	jobs := client.ListJobs(ctx, &schedulerpb.ListJobsRequest{Parent: parent})
	if listed, err := jobs.Next(); err != nil || listed.GetName() != name {
		t.Fatalf("list = %v, %v", listed, err)
	}
	if _, err := jobs.Next(); !errors.Is(err, iterator.Done) {
		t.Fatalf("list end = %v", err)
	}
}

// startRuntime serves the provider behind the runtime's STS and gRPC
// listeners. The subject token is project-a's Cloud Scheduler admin;
// subject+"-peer" holds no role.
func startRuntime(t *testing.T, subject string) core.Listeners {
	t.Helper()
	const owner, peer = "serviceAccount:app@project-a.iam.gserviceaccount.com", "serviceAccount:peer@project-a.iam.gserviceaccount.com"
	registry, index := gateway.NewRegistry(), state.NewIndex()
	project := "//cloudresourcemanager.googleapis.com/projects/project-a"
	if err := index.Put("resourcemanager", state.Resource{Name: project, Service: "resourcemanager", Kind: "project", Number: "123456", Scope: state.Scope{Project: "project-a", Location: state.Global}}); err != nil {
		t.Fatal(err)
	}
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := engine.SetPolicy(project, iam.Policy{Bindings: []iam.Binding{{Role: "roles/cloudscheduler.admin", Members: []string{owner}}}}); err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Principals: []identity.Enrollment{
		{Subject: owner, Project: "project-a", SubjectToken: subject},
		{Subject: peer, Project: "project-a", SubjectToken: subject + "-peer"},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := core.WithGateway("cloudscheduler-client", 5*time.Second, gateway.New(registry, identities.Resolve))
	server.Identity, server.IAM = identities, engine
	if _, err := New(context.Background(), Config{Identity: identities}, registry, server.Backends, index, engine); err != nil {
		t.Fatal(err)
	}
	listen := func() net.Listener {
		listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return listener
	}
	listeners := core.Listeners{HTTP: listen(), GRPC: listen()}
	serving, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serving, listeners) }()
	t.Cleanup(func() { stop(); <-done })
	return listeners
}

// writeADC writes an external-account ADC file whose subject token the
// local STS exchanges.
func writeADC(t *testing.T, sts, subject string) string {
	t.Helper()
	directory := t.TempDir()
	token := filepath.Join(directory, "subject.txt")
	if err := os.WriteFile(token, []byte(subject), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, //nolint:errchkjson // plain map
		"token_url": sts + "/v1/token", "credential_source": map[string]string{"file": token}})
	adc := filepath.Join(directory, "adc.json")
	if err := os.WriteFile(adc, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return adc
}

// schedulerGoClient builds the default gRPC client over a local channel with ADC.
func schedulerGoClient(ctx context.Context, t *testing.T, adc, endpoint string) *scheduler.CloudSchedulerClient {
	t.Helper()
	creds, err := credentials.NewCredentialsFromFile(credentials.ExternalAccount, adc, &credentials.DetectOptions{Scopes: []string{identity.CloudPlatform}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(local.NewCredentials()), grpc.WithPerRPCCredentials(bearerADC{creds}))
	if err != nil {
		t.Fatal(err)
	}
	client, err := scheduler.NewCloudSchedulerClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
