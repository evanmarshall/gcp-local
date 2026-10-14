package run

import (
	"context"
	"encoding/json"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/auth/credentials"
	"cloud.google.com/go/iam/apiv1/iampb"
	"cloud.google.com/go/longrunning/autogen/longrunningpb"
	runapi "cloud.google.com/go/run/apiv2"
	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

// Local identities of the gRPC tests: the harness grants the owner
// roles/run.admin on the project; the peer has no grant.
const (
	ownerSubject = "serviceAccount:fixture-actor@demo-project.iam.gserviceaccount.com"
	peerSubject  = "serviceAccount:peer@demo-project.iam.gserviceaccount.com"
	testParent   = "projects/demo-project/locations/us-central1"
)

// grpcHarness serves the provider's registry on loopback gRPC with local
// identity resolving tokens, and writes an external-account ADC file per
// identity whose token URL is the local STS endpoint.
type grpcHarness struct {
	*harness
	endpoint string
	adc      map[string]string // subject -> ADC file
}

func newGRPCHarness(t *testing.T) *grpcHarness {
	t.Helper()
	h := newHarness(t)
	revision := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(revision.Close)
	h.runtime.address = revision.Listener.Addr().String()
	enrollments := []identity.Enrollment{
		{Subject: ownerSubject, Project: "demo-project", SubjectToken: "gls.synthetic-run-grpc-owner-subject-token"}, //nolint:gosec // a synthetic local subject token
		{Subject: peerSubject, Project: "demo-project", SubjectToken: "gls.synthetic-run-grpc-peer-subject-token"},   //nolint:gosec // a synthetic local subject token
	}
	identities, err := identity.New(identity.Config{Principals: enrollments}, nil)
	if err != nil {
		t.Fatal(err)
	}
	sts := httptest.NewServer(identities)
	t.Cleanup(sts.Close)
	g := &grpcHarness{harness: h, endpoint: serveGRPC(t, gateway.New(h.gateway.Registry, identities.Resolve)), adc: map[string]string{}}
	directory := t.TempDir()
	for _, enrollment := range enrollments {
		g.adc[enrollment.Subject] = writeADC(t, directory, enrollment.SubjectToken, sts.URL+"/v1/token")
	}
	return g
}

// serveGRPC starts the gateway's gRPC server on loopback and returns its address.
func serveGRPC(t *testing.T, g *gateway.Gateway) string {
	t.Helper()
	listener, err := (&net.ListenConfig{}).Listen(context.Background(), "tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	server := g.NewGRPCServer()
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return listener.Addr().String()
}

// writeADC writes one external-account credential file for a subject token.
func writeADC(t *testing.T, directory, subjectToken, tokenURL string) string {
	t.Helper()
	subject := filepath.Join(directory, subjectToken+".txt")
	if err := os.WriteFile(subject, []byte(subjectToken), 0o600); err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{"type": "external_account", "audience": identity.Audience,
		"subject_token_type": identity.AccessTokenType, "token_url": tokenURL, "credential_source": map[string]string{"file": subject}})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(directory, subjectToken+"-adc.json")
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// adcCredentials attaches tokens from local ADC to each RPC. The gateway
// listens on plaintext loopback, so the credential does not demand TLS.
type adcCredentials struct{ credentials *auth.Credentials }

func (a adcCredentials) GetRequestMetadata(ctx context.Context, _ ...string) (map[string]string, error) {
	token, err := a.credentials.Token(ctx)
	if err != nil {
		return nil, err //nolint:wrapcheck // the test reports the ADC failure as is
	}
	return map[string]string{"authorization": "Bearer " + token.Value}, nil
}
func (adcCredentials) RequireTransportSecurity() bool { return false }

// conn dials the gateway as one subject through its ADC file; an empty
// subject dials anonymously.
func (g *grpcHarness) conn(t *testing.T, subject string) *grpc.ClientConn {
	t.Helper()
	options := []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	if subject != "" {
		adc, err := credentials.NewCredentialsFromFile(credentials.ExternalAccount, g.adc[subject], &credentials.DetectOptions{Scopes: []string{identity.CloudPlatform}})
		if err != nil {
			t.Fatal(err)
		}
		options = append(options, grpc.WithPerRPCCredentials(adcCredentials{adc}))
	}
	conn, err := grpc.NewClient("passthrough:///"+g.endpoint, options...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return conn
}

// services opens the official Go Services client over one subject's connection.
func (g *grpcHarness) services(t *testing.T, subject string) *runapi.ServicesClient {
	t.Helper()
	client, err := runapi.NewServicesClient(context.Background(), option.WithGRPCConn(g.conn(t, subject)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = client.Close() })
	return client
}

// deploy creates the test service through the Go client and waits for the
// rollout operation.
func (g *grpcHarness) deploy(t *testing.T, client *runapi.ServicesClient) (*runapi.CreateServiceOperation, *runpb.Service) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	operation, err := client.CreateService(ctx, &runpb.CreateServiceRequest{Parent: testParent, ServiceId: "svc", Service: &runpb.Service{
		Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{
			Image: imageReference, Env: []*runpb.EnvVar{{Name: "MODE", Values: &runpb.EnvVar_Value{Value: "grpc"}}},
		}}},
	}})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	service, err := operation.Wait(ctx)
	if err != nil {
		t.Fatalf("wait for the create operation: %v", err)
	}
	return operation, service
}

func TestGoClientDeploysListsAndDeletesOverGRPC(t *testing.T) {
	g := newGRPCHarness(t)
	client := g.services(t, ownerSubject)
	operation, service := g.deploy(t, client)
	checkCreated(t, g, operation, service)
	checkReads(t, g, client)
	checkUpdate(t, client)
	checkRevisions(t, g)
	checkPolicyAndOperations(t, client, operation.Name())
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	deletion, err := client.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: serviceReference})
	if err != nil {
		t.Fatalf("DeleteService: %v", err)
	}
	if deleted, err := deletion.Wait(ctx); err != nil || deleted.GetName() != serviceReference {
		t.Fatalf("deleted service = %v, %v", deleted, err)
	}
	if _, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: serviceReference}); status.Code(err) != codes.NotFound {
		t.Fatalf("GetService after delete = %v", err)
	}
}

// checkCreated verifies the service a finished create operation returns.
func checkCreated(t *testing.T, g *grpcHarness, operation *runapi.CreateServiceOperation, service *runpb.Service) {
	t.Helper()
	if service.GetName() != serviceReference || service.GetLatestReadyRevision() != serviceReference+"/revisions/"+testFirstRevision || service.GetUri() == "" {
		t.Fatalf("created service = %v", service)
	}
	if condition := service.GetTerminalCondition(); condition.GetState() != runpb.Condition_CONDITION_SUCCEEDED {
		t.Fatalf("terminal condition = %v", condition)
	}
	if metadata, err := operation.Metadata(); err != nil || metadata.GetName() != serviceReference {
		t.Fatalf("operation metadata = %v, %v", metadata, err)
	}
	if env := g.runtime.started[0].Env; !containsEntry(env, "MODE=grpc") {
		t.Fatalf("container env = %v", env)
	}
}

// checkReads gets and lists the service over gRPC and reads it over REST.
func checkReads(t *testing.T, g *grpcHarness, client *runapi.ServicesClient) {
	t.Helper()
	ctx := context.Background()
	got, err := client.GetService(ctx, &runpb.GetServiceRequest{Name: serviceReference})
	if err != nil || got.GetTemplate().GetContainers()[0].GetEnv()[0].GetValue() != "grpc" {
		t.Fatalf("GetService = %v, %v", got, err)
	}
	// REST reads the service the gRPC call created: one state, two encodings.
	recorder := g.invoke(http.MethodGet, "/v2/"+serviceReference, nil)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"name":"`+serviceReference+`"`) {
		t.Fatalf("REST get = %d %s", recorder.Code, recorder.Body.String())
	}
	var names []string
	for listed, err := range client.ListServices(ctx, &runpb.ListServicesRequest{Parent: testParent}).All() {
		if err != nil {
			t.Fatalf("ListServices: %v", err)
		}
		names = append(names, listed.GetName())
	}
	if len(names) != 1 || names[0] != serviceReference {
		t.Fatalf("listed services = %v", names)
	}
}

// checkUpdate rolls out a second revision and waits for its operation.
func checkUpdate(t *testing.T, client *runapi.ServicesClient) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	update, err := client.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: &runpb.Service{Name: serviceReference,
		Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: "example.local/svc:2"}}}}})
	if err != nil {
		t.Fatalf("UpdateService: %v", err)
	}
	updated, err := update.Wait(ctx)
	if err != nil || updated.GetLatestReadyRevision() != serviceReference+"/revisions/svc-00002" || updated.GetTrafficStatuses()[0].GetRevision() != "svc-00002" {
		t.Fatalf("updated service = %v, %v", updated, err)
	}
}

// checkRevisions gets, lists, and deletes revisions with the Revisions client.
func checkRevisions(t *testing.T, g *grpcHarness) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	revisions, err := runapi.NewRevisionsClient(ctx, option.WithGRPCConn(g.conn(t, ownerSubject)))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = revisions.Close() })
	first := serviceReference + "/revisions/" + testFirstRevision
	revision, err := revisions.GetRevision(ctx, &runpb.GetRevisionRequest{Name: first})
	if err != nil || revision.GetService() != serviceReference || revision.GetContainers()[0].GetImage() != imageReference {
		t.Fatalf("GetRevision = %v, %v", revision, err)
	}
	count := 0
	for _, err := range revisions.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: serviceReference}).All() {
		if err != nil {
			t.Fatalf("ListRevisions: %v", err)
		}
		count++
	}
	if count != 2 {
		t.Fatalf("listed %d revisions, want 2", count)
	}
	checkRevisionDelete(ctx, t, revisions, first)
}

// checkRevisionDelete deletes a revision without traffic and waits for the
// operation's Revision response.
func checkRevisionDelete(ctx context.Context, t *testing.T, revisions *runapi.RevisionsClient, first string) {
	t.Helper()
	removal, err := revisions.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: first})
	if err != nil {
		t.Fatalf("DeleteRevision: %v", err)
	}
	if removed, err := removal.Wait(ctx); err != nil || removed.GetName() != first {
		t.Fatalf("deleted revision = %v, %v", removed, err)
	}
}

// checkPolicyAndOperations round-trips the service policy and reads the
// create operation through the client's operations mixin.
func checkPolicyAndOperations(t *testing.T, client *runapi.ServicesClient, operation string) {
	t.Helper()
	ctx := context.Background()
	policy, err := client.GetIamPolicy(ctx, &iampb.GetIamPolicyRequest{Resource: serviceReference})
	if err != nil {
		t.Fatalf("GetIamPolicy: %v", err)
	}
	policy.Bindings = append(policy.Bindings, &iampb.Binding{Role: "roles/run.invoker", Members: []string{"allUsers"}})
	if set, err := client.SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: serviceReference, Policy: policy}); err != nil || len(set.GetBindings()) != 1 {
		t.Fatalf("SetIamPolicy = %v, %v", set, err)
	}
	var names []string
	for listed, err := range client.ListOperations(ctx, &longrunningpb.ListOperationsRequest{Name: testParent}).All() {
		if err != nil {
			t.Fatalf("ListOperations: %v", err)
		}
		names = append(names, listed.GetName())
	}
	if !slices.Contains(names, operation) {
		t.Fatalf("listed operations = %v, want %s", names, operation)
	}
	if fetched, err := client.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: operation}); err != nil || !fetched.GetDone() {
		t.Fatalf("GetOperation = %v, %v", fetched, err)
	}
	if err := client.DeleteOperation(ctx, &longrunningpb.DeleteOperationRequest{Name: operation}); err != nil {
		t.Fatalf("DeleteOperation: %v", err)
	}
}

func TestGRPCDeniesUngrantedIdentityBeforeLaunch(t *testing.T) {
	g := newGRPCHarness(t)
	ctx := context.Background()
	_, err := g.services(t, peerSubject).CreateService(ctx, &runpb.CreateServiceRequest{Parent: testParent, ServiceId: "svc",
		Service: &runpb.Service{Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: imageReference}}}}})
	want := "Permission 'run.services.create' denied on resource 'projects/demo-project/locations/us-central1/services/svc' (or resource may not exist)."
	if status.Code(err) != codes.PermissionDenied || status.Convert(err).Message() != want {
		t.Fatalf("ungranted CreateService = %v", err)
	}
	if len(g.runtime.started) != 0 {
		t.Fatalf("a denied create launched %v", g.runtime.started)
	}
	_, err = runpb.NewServicesClient(g.conn(t, "")).ListServices(ctx, &runpb.ListServicesRequest{Parent: testParent})
	if status.Code(err) != codes.Unauthenticated || status.Convert(err).Message() != "Request had invalid authentication credentials." {
		t.Fatalf("anonymous ListServices = %v", err)
	}
}

func TestGRPCMapsProviderErrors(t *testing.T) {
	g := newGRPCHarness(t)
	ctx := context.Background()
	client := g.services(t, ownerSubject)
	g.deploy(t, client)
	_, err := client.CreateService(ctx, &runpb.CreateServiceRequest{Parent: testParent, ServiceId: "svc",
		Service: &runpb.Service{Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: imageReference}}}}})
	if status.Code(err) != codes.AlreadyExists || status.Convert(err).Message() != "Resource 'svc' already exists." {
		t.Fatalf("duplicate CreateService = %v", err)
	}
	conn := g.conn(t, ownerSubject)
	_, err = runpb.NewRevisionsClient(conn).GetRevision(ctx, &runpb.GetRevisionRequest{Name: serviceReference + "/revisions/missing"})
	if status.Code(err) != codes.NotFound || status.Convert(err).Message() != "Revision 'missing' does not exist." {
		t.Fatalf("missing GetRevision = %v", err)
	}
	operations := longrunningpb.NewOperationsClient(conn)
	_, err = operations.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: testParent + "/operations/missing"})
	if status.Code(err) != codes.NotFound || status.Convert(err).Message() != "The resource 'operations/missing' was not found" {
		t.Fatalf("missing GetOperation = %v", err)
	}
	// Only Cloud Run's operation names reach the provider: a Spanner name
	// goes to Spanner, which this server does not run, and a name no service
	// binds is a gap.
	for name, message := range map[string]string{
		"projects/demo-project/instances/i/operations/o": "Service spanner is not enabled on this server; add spanner to -services",
		"operations/o/p": "No provider implements /google.longrunning.Operations/GetOperation",
	} {
		_, err = operations.GetOperation(ctx, &longrunningpb.GetOperationRequest{Name: name})
		if status.Code(err) != codes.Unimplemented || status.Convert(err).Message() != message {
			t.Fatalf("GetOperation %s = %v", name, err)
		}
	}
}

func TestGRPCRefusesUnimplementedMethods(t *testing.T) {
	g := newGRPCHarness(t)
	ctx := context.Background()
	conn := g.conn(t, ownerSubject)
	operation := testParent + "/operations/missing"
	_, err := runpb.NewServicesClient(conn).TestIamPermissions(ctx, &iampb.TestIamPermissionsRequest{Resource: serviceReference, Permissions: []string{"run.services.get"}})
	if refused := status.Convert(err); refused.Code() != codes.Unimplemented || refused.Message() != "No provider implements /google.cloud.run.v2.Services/TestIamPermissions" {
		t.Errorf("TestIamPermissions = %v", refused)
	}
	// Cloud Run binds no cancel (run_v2.yaml); the mixin names the resource
	// kind it does not serve.
	_, err = longrunningpb.NewOperationsClient(conn).CancelOperation(ctx, &longrunningpb.CancelOperationRequest{Name: operation})
	want := "No provider implements /google.longrunning.Operations/CancelOperation for projects/*/locations/*/operations/* resources"
	if refused := status.Convert(err); refused.Code() != codes.Unimplemented || refused.Message() != want {
		t.Errorf("CancelOperation = %v", refused)
	}
}

// WaitOperation blocks until the rollout is done and returns it done.
func TestGRPCWaitOperationReturnsTheDoneRollout(t *testing.T) {
	g := newGRPCHarness(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	client := g.services(t, ownerSubject)
	operation, err := client.CreateService(ctx, &runpb.CreateServiceRequest{Parent: testParent, ServiceId: "svc", Service: &runpb.Service{
		Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: imageReference}}}}})
	if err != nil {
		t.Fatalf("CreateService: %v", err)
	}
	waited, err := longrunningpb.NewOperationsClient(g.conn(t, ownerSubject)).WaitOperation(ctx, &longrunningpb.WaitOperationRequest{
		Name: operation.Name(), Timeout: durationpb.New(20 * time.Second)})
	if err != nil || !waited.GetDone() || waited.GetResponse() == nil {
		t.Fatalf("WaitOperation = %v, %v", waited, err)
	}
	_, err = longrunningpb.NewOperationsClient(g.conn(t, ownerSubject)).WaitOperation(ctx, &longrunningpb.WaitOperationRequest{Name: testParent + "/operations/missing"})
	if status.Code(err) != codes.NotFound {
		t.Fatalf("WaitOperation on a missing operation = %v", err)
	}
}

// optionCase is one gRPC request that sets a refused Admin API option.
type optionCase struct {
	option string
	call   func() *status.Status
}

// optionCases sets each refused option through the generated gRPC stubs.
func optionCases(ctx context.Context, conn *grpc.ClientConn) []optionCase {
	services, revisions := runpb.NewServicesClient(conn), runpb.NewRevisionsClient(conn)
	template := &runpb.Service{Name: serviceReference, Template: &runpb.RevisionTemplate{Containers: []*runpb.Container{{Image: imageReference}}}}
	revision := serviceReference + "/revisions/" + testFirstRevision
	refuse := func(_ any, err error) *status.Status { return status.Convert(err) }
	return []optionCase{
		{"validateOnly", func() *status.Status {
			return refuse(services.CreateService(ctx, &runpb.CreateServiceRequest{Parent: testParent, ServiceId: "other", Service: template, ValidateOnly: true}))
		}},
		{"showDeleted", func() *status.Status {
			return refuse(services.ListServices(ctx, &runpb.ListServicesRequest{Parent: testParent, ShowDeleted: true}))
		}},
		{"updateMask", func() *status.Status {
			return refuse(services.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: template, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"template"}}}))
		}},
		{"validateOnly", func() *status.Status {
			return refuse(services.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: template, ValidateOnly: true}))
		}},
		{"allowMissing", func() *status.Status {
			return refuse(services.UpdateService(ctx, &runpb.UpdateServiceRequest{Service: template, AllowMissing: true}))
		}},
		{"etag", func() *status.Status {
			return refuse(services.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: serviceReference, Etag: `"abc"`}))
		}},
		{"validateOnly", func() *status.Status {
			return refuse(services.DeleteService(ctx, &runpb.DeleteServiceRequest{Name: serviceReference, ValidateOnly: true}))
		}},
		{"showDeleted", func() *status.Status {
			return refuse(revisions.ListRevisions(ctx, &runpb.ListRevisionsRequest{Parent: serviceReference, ShowDeleted: true}))
		}},
		{"etag", func() *status.Status {
			return refuse(revisions.DeleteRevision(ctx, &runpb.DeleteRevisionRequest{Name: revision, Etag: `"abc"`}))
		}},
	}
}

func TestGRPCRefusesUnmodeledOptions(t *testing.T) {
	g := newGRPCHarness(t)
	ctx := context.Background()
	conn := g.conn(t, ownerSubject)
	g.deploy(t, g.services(t, ownerSubject))
	for _, c := range optionCases(ctx, conn) {
		if refused := c.call(); refused.Code() != codes.Unimplemented || refused.Message() != "Cloud Run coverage gap: the "+c.option+" option is not implemented" {
			t.Errorf("%s = %v", c.option, refused)
		}
	}
	_, err := runpb.NewServicesClient(conn).SetIamPolicy(ctx, &iampb.SetIamPolicyRequest{Resource: serviceReference, Policy: &iampb.Policy{}, UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"audit_configs"}}})
	if status.Code(err) != codes.Unimplemented || status.Convert(err).Message() != "Cloud Run coverage gap: setIamPolicy updateMask audit_configs is not modeled" {
		t.Errorf("SetIamPolicy updateMask = %v", err)
	}
	if len(g.runtime.started) != 1 || len(g.runtime.stopped) != 0 {
		t.Fatalf("a refused option changed containers: started %v stopped %v", g.runtime.started, g.runtime.stopped)
	}
}

func TestRESTRefusesUnmodeledOptions(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	base := "/v2/" + serviceReference
	cases := []struct{ option, verb, path string }{
		{"validateOnly", http.MethodPost, "/v2/" + testParent + "/services?serviceId=other&validateOnly=true"},
		{"showDeleted", http.MethodGet, "/v2/" + testParent + "/services?showDeleted=true"},
		{"allowMissing", http.MethodPatch, base + "?allowMissing=true"},
		{"updateMask", http.MethodPatch, base + "?updateMask=template"},
		{"etag", http.MethodDelete, base + "?etag=abc"},
	}
	body := map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": imageReference}}}}
	for _, c := range cases {
		recorder := h.invoke(c.verb, c.path, body)
		want := "Cloud Run coverage gap: the " + c.option + " option is not implemented"
		if recorder.Code != http.StatusNotImplemented || !strings.Contains(recorder.Body.String(), want) {
			t.Errorf("%s %s = %d %s", c.verb, c.path, recorder.Code, recorder.Body.String())
		}
	}
	// Default values are the documented defaults, not refusals.
	if recorder := h.invoke(http.MethodGet, "/v2/"+testParent+"/services?showDeleted=false&pageSize=0", nil); recorder.Code != http.StatusOK {
		t.Fatalf("default options = %d %s", recorder.Code, recorder.Body.String())
	}
}

func TestCreateRefusesSecretEnvironmentSources(t *testing.T) {
	h := newHarness(t)
	recorder := h.invoke(http.MethodPost, "/v2/"+testParent+"/services?serviceId=svc", map[string]any{
		"template": map[string]any{"containers": []any{map[string]any{"image": imageReference, "env": []any{
			map[string]any{"name": "TOKEN", "valueSource": map[string]any{"secretKeyRef": map[string]any{"secret": "s", "version": "1"}}},
		}}}},
	})
	if recorder.Code != http.StatusNotImplemented || !strings.Contains(recorder.Body.String(), "Cloud Run coverage gap: template.containers.env.valueSource is not modeled") {
		t.Fatalf("valueSource create = %d %s", recorder.Code, recorder.Body.String())
	}
	if len(h.runtime.started) != 0 {
		t.Fatalf("a refused template launched %v", h.runtime.started)
	}
}

func TestGRPCNamesDisabledService(t *testing.T) {
	g := gateway.New(gateway.NewRegistry(), func(context.Context, string) (gateway.Principal, error) {
		return gateway.Principal{State: gateway.Anonymous}, nil
	})
	conn, err := grpc.NewClient("passthrough:///"+serveGRPC(t, g), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	_, err = runpb.NewServicesClient(conn).GetService(context.Background(), &runpb.GetServiceRequest{Name: serviceReference})
	if status.Code(err) != codes.Unimplemented || status.Convert(err).Message() != "Service run is not enabled on this server; add run to -services" {
		t.Fatalf("GetService without the provider = %v", err)
	}
}

func TestRESTRevisionsAndDeletesReturnOperations(t *testing.T) {
	h := newHarness(t)
	h.create(t)
	h.invoke(http.MethodPatch, "/v2/"+serviceReference, map[string]any{"template": map[string]any{"containers": []any{map[string]any{"image": "example.local/svc:2"}}}})
	revision := serviceReference + "/revisions/" + testFirstRevision
	recorder := h.invoke(http.MethodGet, "/v2/"+revision, nil)
	if recorder.Code != http.StatusOK || !strings.Contains(recorder.Body.String(), `"name":"`+revision+`"`) {
		t.Fatalf("REST revision get = %d %s", recorder.Code, recorder.Body.String())
	}
	for _, deletion := range []struct{ path, typeURL string }{{revision, typeRevision}, {serviceReference, typeService}} {
		var operation struct {
			Done     bool           `json:"done"`
			Response map[string]any `json:"response"`
		}
		recorder = h.invoke(http.MethodDelete, "/v2/"+deletion.path, nil)
		if err := json.Unmarshal(recorder.Body.Bytes(), &operation); err != nil || recorder.Code != http.StatusOK || operation.Done {
			t.Fatalf("REST delete %s = %d %s", deletion.path, recorder.Code, recorder.Body.String())
		}
		h.wait(t, recorder, &operation)
		if !operation.Done || operation.Response["@type"] != deletion.typeURL || operation.Response["name"] != deletion.path {
			t.Fatalf("REST delete %s operation = %+v", deletion.path, operation)
		}
	}
}

// wait waits on the operation a response names and decodes it into done.
func (h *harness) wait(t *testing.T, started *httptest.ResponseRecorder, done any) {
	t.Helper()
	var name struct {
		Name string `json:"name"`
	}
	_ = json.Unmarshal(started.Body.Bytes(), &name)
	recorder := h.invoke(http.MethodPost, "/v2/"+name.Name+":wait", map[string]any{"timeout": "5s"})
	if err := json.Unmarshal(recorder.Body.Bytes(), done); err != nil || recorder.Code != http.StatusOK {
		t.Fatalf("REST wait %s = %d %s", name.Name, recorder.Code, recorder.Body.String())
	}
}
