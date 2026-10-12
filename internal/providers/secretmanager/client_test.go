package secretmanager

import (
	"context"
	"encoding/json"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	secretmanagerclient "cloud.google.com/go/secretmanager/apiv1"
	"cloud.google.com/go/secretmanager/apiv1/secretmanagerpb"
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
)

// adcToken attaches a local ADC access token to each call. The connection
// uses local (loopback-only) channel credentials, as Python's
// grpc.local_channel_credentials does, which carry no transport security.
type adcToken struct{ credentials *auth.Credentials }

func (a adcToken) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := a.credentials.Token(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // the test reports it
	}
	return map[string]string{"authorization": "Bearer " + token.Value}, nil
}

func (adcToken) RequireTransportSecurity() bool { return false }

// TestGoClientOverGRPCWithLocalADC drives cloud.google.com/go/secretmanager
// at its default gRPC transport with external-account ADC against the
// runtime's STS and gRPC listeners.
func TestGoClientOverGRPCWithLocalADC(t *testing.T) {
	const subject = "gls.synthetic-secretmanager-go-subject"
	listeners := startRuntime(t, subject)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := goClient(ctx, t, adcFile(t, "http://"+listeners.HTTP.Addr().String(), subject), listeners.GRPC.Addr().String())
	name := createAndAccess(ctx, t, client)
	if names := secretNames(ctx, t, client); len(names) != 1 || names[0] != name {
		t.Fatalf("list = %v", names)
	}
	denied := goClient(ctx, t, adcFile(t, "http://"+listeners.HTTP.Addr().String(), subject+"-peer"), listeners.GRPC.Addr().String())
	if _, err := denied.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: name + "/versions/1"}); status.Code(err) != codes.PermissionDenied {
		t.Fatalf("peer access = %v", err)
	}
	if err := client.DeleteSecret(ctx, &secretmanagerpb.DeleteSecretRequest{Name: name}); err != nil {
		t.Fatal(err)
	}
}

// createAndAccess creates a secret with one version, reads the payload
// back, and returns the secret's name.
func createAndAccess(ctx context.Context, t *testing.T, client *secretmanagerclient.Client) string {
	t.Helper()
	created, err := client.CreateSecret(ctx, &secretmanagerpb.CreateSecretRequest{Parent: "projects/project-a", SecretId: "go-client", Secret: &secretmanagerpb.Secret{
		Replication: &secretmanagerpb.Replication{Replication: &secretmanagerpb.Replication_Automatic_{Automatic: &secretmanagerpb.Replication_Automatic{}}},
	}})
	if err != nil || created.GetName() != "projects/123456/secrets/go-client" {
		t.Fatalf("create = %v, %v", created, err)
	}
	version, err := client.AddSecretVersion(ctx, &secretmanagerpb.AddSecretVersionRequest{Parent: created.GetName(), Payload: &secretmanagerpb.SecretPayload{Data: []byte("go-payload")}})
	if err != nil || version.GetState() != secretmanagerpb.SecretVersion_ENABLED {
		t.Fatalf("add version = %v, %v", version, err)
	}
	accessed, err := client.AccessSecretVersion(ctx, &secretmanagerpb.AccessSecretVersionRequest{Name: created.GetName() + "/versions/latest"})
	if err != nil || string(accessed.GetPayload().GetData()) != "go-payload" || accessed.GetPayload().GetDataCrc32C() == 0 {
		t.Fatalf("access = %v, %v", accessed, err)
	}
	return created.GetName()
}

// secretNames pages through the client's secret listing.
func secretNames(ctx context.Context, t *testing.T, client *secretmanagerclient.Client) []string {
	t.Helper()
	listing := client.ListSecrets(ctx, &secretmanagerpb.ListSecretsRequest{Parent: "projects/project-a"})
	names := []string{}
	for {
		secret, err := listing.Next()
		if errors.Is(err, iterator.Done) {
			return names
		}
		if err != nil {
			t.Fatal(err)
		}
		names = append(names, secret.GetName())
	}
}

// startRuntime serves the provider behind the runtime's STS and gRPC
// listeners. The subject token is project-a's admin; subject+"-peer" holds
// no role.
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
	if _, err := engine.SetPolicy(project, iam.Policy{Bindings: []iam.Binding{{Role: "roles/secretmanager.admin", Members: []string{owner}}}}); err != nil {
		t.Fatal(err)
	}
	identities, err := identity.New(identity.Config{Principals: []identity.Enrollment{
		{Subject: owner, Project: "project-a", SubjectToken: subject},
		{Subject: peer, Project: "project-a", SubjectToken: subject + "-peer"},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := core.WithGateway("secretmanager-go", 5*time.Second, gateway.New(registry, identities.Resolve))
	server.Identity, server.IAM = identities, engine
	if _, err := New(context.Background(), registry, server.Backends, index, engine); err != nil {
		t.Fatal(err)
	}
	listeners := core.Listeners{HTTP: loopback(t), GRPC: loopback(t)}
	serving, stop := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(serving, listeners) }()
	t.Cleanup(func() { stop(); <-done })
	return listeners
}

// loopback opens one loopback listener.
func loopback(t *testing.T) net.Listener {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return listener
}

// adcFile writes an external-account ADC file whose subject token the local
// STS exchanges.
func adcFile(t *testing.T, endpoint, subject string) string {
	t.Helper()
	directory := t.TempDir()
	token := filepath.Join(directory, "subject.txt")
	if err := os.WriteFile(token, []byte(subject), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, _ := json.Marshal(map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, //nolint:errchkjson // plain map
		"token_url": endpoint + "/v1/token", "credential_source": map[string]string{"file": token}})
	path := filepath.Join(directory, "adc.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// goClient builds the default gRPC client over a local channel with ADC.
func goClient(ctx context.Context, t *testing.T, adc, endpoint string) *secretmanagerclient.Client {
	t.Helper()
	creds, err := credentials.NewCredentialsFromFile(credentials.ExternalAccount, adc, &credentials.DetectOptions{Scopes: []string{identity.CloudPlatform}})
	if err != nil {
		t.Fatal(err)
	}
	conn, err := grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(local.NewCredentials()), grpc.WithPerRPCCredentials(adcToken{creds}))
	if err != nil {
		t.Fatal(err)
	}
	client, err := secretmanagerclient.NewClient(ctx, option.WithGRPCConn(conn))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}
