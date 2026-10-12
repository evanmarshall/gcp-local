package cloudtasks

import (
	"context"
	"encoding/json"
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
	cloudtasksclient "cloud.google.com/go/cloudtasks/apiv2"
	"cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/local"
	"google.golang.org/grpc/status"
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

// TestGoClientOverGRPCWithLocalADC drives cloud.google.com/go/cloudtasks
// (v2, default gRPC transport) with external-account ADC: it creates a
// queue, enqueues an HTTP task that the provider dispatches to a loopback
// target, and is refused as an ungranted peer.
func TestGoClientOverGRPCWithLocalADC(t *testing.T) {
	const subject = "gls.synthetic-cloudtasks-go-subject"
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
	client := tasksGoClient(ctx, t, writeADC(t, sts, subject), listeners.GRPC.Addr().String())
	parent := "projects/project-a/locations/us-central1"
	queue, err := client.CreateQueue(ctx, &cloudtaskspb.CreateQueueRequest{Parent: parent, Queue: &cloudtaskspb.Queue{Name: parent + "/queues/go-client"}})
	if err != nil || queue.GetState() != cloudtaskspb.Queue_RUNNING || queue.GetRetryConfig().GetMaxAttempts() != 100 {
		t.Fatalf("create queue = %v, %v", queue, err)
	}
	task, err := client.CreateTask(ctx, &cloudtaskspb.CreateTaskRequest{Parent: queue.GetName(), Task: &cloudtaskspb.Task{
		MessageType: &cloudtaskspb.Task_HttpRequest{HttpRequest: &cloudtaskspb.HttpRequest{Url: target.URL, HttpMethod: cloudtaskspb.HttpMethod_PUT, Body: []byte("go-task")}},
	}})
	if err != nil || task.GetHttpRequest().GetHttpMethod() != cloudtaskspb.HttpMethod_PUT {
		t.Fatalf("create task = %v, %v", task, err)
	}
	if got := await(ctx, t, delivered); got != "PUT go-task" {
		t.Fatalf("delivered %q", got)
	}
	denied := tasksGoClient(ctx, t, writeADC(t, sts, subject+"-peer"), listeners.GRPC.Addr().String())
	if _, err := denied.GetQueue(ctx, &cloudtaskspb.GetQueueRequest{Name: queue.GetName()}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("peer get = %v", err)
	}
	if err := client.DeleteQueue(ctx, &cloudtaskspb.DeleteQueueRequest{Name: queue.GetName()}); err != nil {
		t.Fatal(err)
	}
}

// await returns the first delivery, failing the test at the deadline.
func await(ctx context.Context, t *testing.T, delivered <-chan string) string {
	t.Helper()
	select {
	case got := <-delivered:
		return got
	case <-ctx.Done():
		t.Fatal("the task was not dispatched")
		return ""
	}
}

// startRuntime serves the provider behind the runtime's STS and gRPC
// listeners. The subject token is project-a's Cloud Tasks admin;
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
	if _, err := engine.SetPolicy(project, iam.Policy{Bindings: []iam.Binding{{Role: "roles/cloudtasks.admin", Members: []string{owner}}}}); err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Principals: []identity.Enrollment{
		{Subject: owner, Project: "project-a", SubjectToken: subject},
		{Subject: peer, Project: "project-a", SubjectToken: subject + "-peer"},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := core.WithGateway("cloudtasks-client", 5*time.Second, gateway.New(registry, identities.Resolve))
	server.Identity, server.IAM = identities, engine
	if _, err := New(context.Background(), DeliveryConfig{}, registry, server.Backends, index, engine); err != nil {
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

// tasksGoClient builds the default gRPC client over a local channel with ADC.
func tasksGoClient(ctx context.Context, t *testing.T, adc, endpoint string) *cloudtasksclient.Client {
	t.Helper()
	creds, err := credentials.NewCredentialsFromFile(credentials.ExternalAccount, adc, &credentials.DetectOptions{Scopes: []string{identity.CloudPlatform}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(local.NewCredentials()), grpc.WithPerRPCCredentials(bearerADC{creds}))
	if err != nil {
		t.Fatal(err)
	}
	client, err := cloudtasksclient.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
