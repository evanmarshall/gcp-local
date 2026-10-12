package identity

import (
	"context"
	"encoding/base64"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// testProject and pushWorkerEmail are the fixture identities shared by the
// identity tests.
const (
	testProject     = "project-a"
	pushWorkerEmail = "push-worker@project-a.iam.gserviceaccount.com"
)

// loadIDTokens bootstraps an IAM engine from config and the ID token minter over it.
func loadIDTokens(t *testing.T, config iam.Config) (*iam.Engine, *IDTokens) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "iam.json")
	if err := os.WriteFile(path, mustMarshal(t, config), 0600); err != nil {
		t.Fatal(err)
	}
	engine, index, err := iam.Load(path)
	if err != nil {
		t.Fatal(err)
	}
	tokens, err := NewIDTokens(engine, index)
	if err != nil {
		t.Fatal(err)
	}
	return engine, tokens
}

// rejectToken requires Verify to refuse token for audience as unauthenticated.
func rejectToken(t *testing.T, tokens *IDTokens, token, audience, label string) {
	t.Helper()
	if _, err := tokens.Verify(token, audience); status.Code(err) != codes.Unauthenticated {
		t.Fatal(label, err)
	}
}

// refuseMint requires Mint for account to fail with code.
func refuseMint(t *testing.T, tokens *IDTokens, call gateway.Call, account string, code codes.Code, label string) {
	t.Helper()
	if _, err := tokens.Mint(call, account, "receiver"); status.Code(err) != code {
		t.Fatal(label, err)
	}
}

func TestIDTokensRequireAccountGrantAndValidateClaims(t *testing.T) {
	project := testProject
	account := iam.AccountResource(project, pushWorkerEmail)
	agent := "serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"
	parent := "//cloudresourcemanager.googleapis.com/projects/" + project
	config := iam.Config{Resources: []state.Resource{{Name: parent, Service: "resourcemanager", Kind: "project", Scope: state.Scope{Project: project, Location: state.Global}}},
		ServiceAccounts: []iam.ServiceAccount{{Project: project, Email: pushWorkerEmail, UniqueID: "123456789012345678901"}},
		Policies:        map[string]iam.Policy{account: {Bindings: []iam.Binding{{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{agent}}}}}}
	engine, tokens := loadIDTokens(t, config)
	now := time.Now()
	tokens.now = func() time.Time { return now }
	call := gateway.Call{Context: context.Background(), Principal: gateway.Principal{State: gateway.Authenticated, Subject: agent, Project: project, Scopes: []string{CloudPlatform}}}
	encoded, err := tokens.Mint(call, account, "receiver")
	if err != nil {
		t.Fatal(err)
	}
	claims, err := tokens.Verify(encoded, "receiver")
	if err != nil || claims.Email != config.ServiceAccounts[0].Email || claims.Subject != config.ServiceAccounts[0].UniqueID || claims.Issuer != IDTokenIssuer {
		t.Fatal("wrong local token claims", err)
	}
	unsigned := base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + strings.Split(encoded, ".")[1] + "."
	rejectToken(t, tokens, encoded, "wrong-audience", "accepted invalid token")
	rejectToken(t, tokens, encoded+"x", "receiver", "accepted invalid token")
	rejectToken(t, tokens, unsigned, "receiver", "accepted invalid token")
	tokens.now = func() time.Time { return now.Add(301 * time.Second) }
	rejectToken(t, tokens, encoded, "receiver", "accepted expired token")
	if _, err := engine.SetPolicy(account, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	refuseMint(t, tokens, call, account, codes.PermissionDenied, "revoked agent minted token")
	call.Principal.Subject = "serviceAccount:unrelated@project-a.iam.gserviceaccount.com"
	refuseMint(t, tokens, call, account, codes.PermissionDenied, "implicit token privilege")
	refuseMint(t, tokens, call, iam.AccountResource(project, "missing@project-a.iam.gserviceaccount.com"), codes.Unimplemented, "minted unknown account")
}

func TestAccountBootstrapRejectsConflictingOwnershipAndIDs(t *testing.T) {
	parent := "//cloudresourcemanager.googleapis.com/projects/project-a"
	base := iam.Config{Resources: []state.Resource{{Name: parent, Service: "resourcemanager", Kind: "project", Scope: state.Scope{Project: testProject, Location: state.Global}}}}
	for _, accounts := range [][]iam.ServiceAccount{
		{{Project: testProject, Email: "push-worker@project-b.iam.gserviceaccount.com", UniqueID: "123456"}},
		{{Project: "project-b", Email: "push-worker@project-b.iam.gserviceaccount.com", UniqueID: "123456"}},
		{{Project: testProject, Email: pushWorkerEmail, UniqueID: "not-numeric"}},
		{{Project: testProject, Email: pushWorkerEmail, UniqueID: "123456"}, {Project: testProject, Email: "another-worker@project-a.iam.gserviceaccount.com", UniqueID: "123456"}},
	} {
		base.ServiceAccounts = accounts
		raw := mustMarshal(t, base)
		path := filepath.Join(t.TempDir(), "iam.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := iam.Load(path); err == nil {
			t.Fatal("accepted invalid account bootstrap")
		}
	}
}
