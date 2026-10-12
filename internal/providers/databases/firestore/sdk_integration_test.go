//go:build firestoreintegration

package firestore

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	pb "cloud.google.com/go/firestore/apiv1/firestorepb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
)

const sdkSubjectSecret = "gls.synthetic-firestore-sdk-subject-token"

// sdkServer runs the real core server with local identity, IAM, and the
// emulator-backed provider on loopback HTTP and gRPC listeners.
type sdkServer struct {
	httpURL, grpcAddress string
	connection           *grpc.ClientConn
	owner                context.Context
}

func runSDKServer(t *testing.T) *sdkServer {
	t.Helper()
	config := Config{Java: os.Getenv("GCP_LOCAL_JAVA"), JAR: os.Getenv("GCP_LOCAL_FIRESTORE_JAR")}
	if err := config.validate(); err != nil {
		t.Fatalf("set GCP_LOCAL_JAVA and GCP_LOCAL_FIRESTORE_JAR to the pinned runtime: %v", err)
	}
	registry, index := gateway.NewRegistry(), state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	for _, project := range []string{testProject, otherProject} {
		if err := index.Put("resourcemanager", state.Resource{Name: provider.ProjectResource(project), Service: "resourcemanager", Kind: "project", Number: "123456", Scope: provider.ProjectScope(project)}); err != nil {
			t.Fatal(err)
		}
		if _, err := engine.SetPolicy(provider.ProjectResource(project), iam.Policy{Bindings: []iam.Binding{
			{Role: "roles/datastore.user", Members: []string{principal("actor", project)}},
		}}); err != nil {
			t.Fatal(err)
		}
	}
	identities, err := identity.New(identity.Config{Principals: []identity.Enrollment{
		{Subject: principal("actor", testProject), Project: testProject, SubjectToken: sdkSubjectSecret},
		{Subject: principal("actor", otherProject), Project: otherProject, SubjectToken: sdkSubjectSecret + "-peer"},
	}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	server := core.WithGateway("firestore-sdk-integration", 5*time.Second, gateway.New(registry, identities.Resolve))
	server.Identity, server.IAM = identities, engine
	if _, err := New(context.Background(), registry, server.Backends, index, engine, config); err != nil {
		t.Fatal(err)
	}
	listen := func() net.Listener {
		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		return listener
	}
	listeners := core.Listeners{HTTP: listen(), GRPC: listen()}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- server.Serve(ctx, listeners) }()
	connection, err := grpc.NewClient(listeners.GRPC.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = connection.Close()
		cancel()
		if err := <-done; err != nil {
			t.Errorf("serve: %v", err)
		}
	})
	form := url.Values{"grant_type": {"urn:ietf:params:oauth:grant-type:token-exchange"}, "audience": {identity.Audience}, "subject_token_type": {identity.AccessTokenType}, "requested_token_type": {identity.AccessTokenType}, "subject_token": {sdkSubjectSecret}, "scope": {identity.CloudPlatform}}
	request := httptest.NewRequest(http.MethodPost, "/v1/token", strings.NewReader(form.Encode()))
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	recorder := httptest.NewRecorder()
	identities.ServeHTTP(recorder, request)
	var token struct {
		AccessToken string `json:"access_token"`
	}
	if recorder.Code != http.StatusOK || json.Unmarshal(recorder.Body.Bytes(), &token) != nil {
		t.Fatalf("local token exchange = %d", recorder.Code)
	}
	testCtx, cancelTest := context.WithTimeout(context.Background(), 90*time.Second)
	t.Cleanup(cancelTest)
	return &sdkServer{
		httpURL: "http://" + listeners.HTTP.Addr().String(), grpcAddress: listeners.GRPC.Addr().String(), connection: connection,
		owner: metadata.AppendToOutgoingContext(testCtx, "authorization", "Bearer "+token.AccessToken),
	}
}

// writeADC writes an external-account local ADC profile for one subject token.
func writeADC(t *testing.T, directory, name, tokenURL, secret string) string {
	t.Helper()
	secretFile := filepath.Join(directory, name+"-subject.txt")
	if err := os.WriteFile(secretFile, []byte(secret), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"type": "external_account", "audience": identity.Audience, "subject_token_type": identity.AccessTokenType, "token_url": tokenURL, "credential_source": map[string]string{"file": secretFile}})
	if err != nil {
		t.Fatal(err)
	}
	profile := filepath.Join(directory, name+"-adc.json")
	if err := os.WriteFile(profile, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return profile
}

// TestPythonSDKQueriesTransactionsAndSubcollections runs the official
// google-cloud-firestore client over the gRPC gateway with local ADC.
func TestPythonSDKQueriesTransactionsAndSubcollections(t *testing.T) {
	python, script := os.Getenv("GCP_LOCAL_TEST_PYTHON"), os.Getenv("GCP_LOCAL_FIRESTORE_SDK")
	if python == "" || script == "" {
		t.Fatal("set GCP_LOCAL_TEST_PYTHON and GCP_LOCAL_FIRESTORE_SDK; run scripts/check/backend/firestore.sh")
	}
	f := runSDKServer(t)
	directory := t.TempDir()
	adc := writeADC(t, directory, "owner", f.httpURL+"/v1/token", sdkSubjectSecret)
	peer := writeADC(t, directory, "peer", f.httpURL+"/v1/token", sdkSubjectSecret+"-peer")
	command := exec.CommandContext(f.owner, python, script, f.grpcAddress)
	command.Env = []string{"PATH=/usr/bin:/bin", "GOOGLE_APPLICATION_CREDENTIALS=" + adc, "GCP_LOCAL_DENIED_ADC=" + peer,
		"GOOGLE_CLOUD_PROJECT=" + testProject, "CLOUDSDK_CONFIG=" + filepath.Join(directory, "empty-cloudsdk"),
		"GOOGLE_API_USE_CLIENT_CERTIFICATE=false", "GOOGLE_API_USE_MTLS_ENDPOINT=never", "GCP_LOCAL_ISOLATED=1"}
	output, err := command.CombinedOutput()
	if err != nil {
		t.Fatalf("Python Firestore SDK: %v\n%s", err, output)
	}
	t.Log(string(output))
	// The SDK cleaned up through the provider; confirm with a separate client.
	listed, err := pb.NewFirestoreClient(f.connection).ListCollectionIds(f.owner, &pb.ListCollectionIdsRequest{Parent: "projects/" + testProject + "/databases/(default)/documents"})
	if err != nil || len(listed.GetCollectionIds()) != 0 {
		t.Fatalf("SDK left collections behind: %v %v", listed.GetCollectionIds(), err)
	}
}
