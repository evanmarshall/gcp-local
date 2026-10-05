package identity

import (
	"context"
	"encoding/base64"
	"encoding/json"
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

func TestIDTokensRequireAccountGrantAndValidateClaims(t *testing.T) {
	project := "project-a"
	account := iam.AccountResource(project, "push-worker@project-a.iam.gserviceaccount.com")
	agent := "serviceAccount:service-123456@gcp-sa-pubsub.iam.gserviceaccount.com"
	parent := "//cloudresourcemanager.googleapis.com/projects/" + project
	config := iam.Config{Resources: []state.Resource{{Name: parent, Service: "resourcemanager", Kind: "project", Scope: state.Scope{Project: project, Location: state.Global}}},
		ServiceAccounts: []iam.ServiceAccount{{Project: project, Email: "push-worker@project-a.iam.gserviceaccount.com", UniqueID: "123456789012345678901"}},
		Policies:        map[string]iam.Policy{account: {Bindings: []iam.Binding{{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{agent}}}}}}
	path := filepath.Join(t.TempDir(), "iam.json")
	raw, _ := json.Marshal(config)
	if err := os.WriteFile(path, raw, 0600); err != nil {
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
	for _, invalid := range []struct{ token, audience string }{
		{encoded, "wrong-audience"}, {encoded + "x", "receiver"},
		{base64.RawURLEncoding.EncodeToString([]byte(`{"alg":"none","typ":"JWT"}`)) + "." + strings.Split(encoded, ".")[1] + ".", "receiver"},
	} {
		if _, err := tokens.Verify(invalid.token, invalid.audience); status.Code(err) != codes.Unauthenticated {
			t.Fatal("accepted invalid token", err)
		}
	}
	tokens.now = func() time.Time { return now.Add(301 * time.Second) }
	if _, err := tokens.Verify(encoded, "receiver"); status.Code(err) != codes.Unauthenticated {
		t.Fatal("accepted expired token", err)
	}
	if _, err := engine.SetPolicy(account, iam.Policy{}); err != nil {
		t.Fatal(err)
	}
	if _, err := tokens.Mint(call, account, "receiver"); status.Code(err) != codes.PermissionDenied {
		t.Fatal("revoked agent minted token", err)
	}
	call.Principal.Subject = "serviceAccount:unrelated@project-a.iam.gserviceaccount.com"
	if _, err := tokens.Mint(call, account, "receiver"); status.Code(err) != codes.PermissionDenied {
		t.Fatal("implicit token privilege", err)
	}
	if _, err := tokens.Mint(call, iam.AccountResource(project, "missing@project-a.iam.gserviceaccount.com"), "receiver"); status.Code(err) != codes.Unimplemented {
		t.Fatal("minted unknown account", err)
	}
}

func TestAccountBootstrapRejectsConflictingOwnershipAndIDs(t *testing.T) {
	parent := "//cloudresourcemanager.googleapis.com/projects/project-a"
	base := iam.Config{Resources: []state.Resource{{Name: parent, Service: "resourcemanager", Kind: "project", Scope: state.Scope{Project: "project-a", Location: state.Global}}}}
	for _, accounts := range [][]iam.ServiceAccount{
		{{Project: "project-a", Email: "push-worker@project-b.iam.gserviceaccount.com", UniqueID: "123456"}},
		{{Project: "project-b", Email: "push-worker@project-b.iam.gserviceaccount.com", UniqueID: "123456"}},
		{{Project: "project-a", Email: "push-worker@project-a.iam.gserviceaccount.com", UniqueID: "not-numeric"}},
		{{Project: "project-a", Email: "push-worker@project-a.iam.gserviceaccount.com", UniqueID: "123456"}, {Project: "project-a", Email: "another-worker@project-a.iam.gserviceaccount.com", UniqueID: "123456"}},
	} {
		base.ServiceAccounts = accounts
		raw, _ := json.Marshal(base)
		path := filepath.Join(t.TempDir(), "iam.json")
		if err := os.WriteFile(path, raw, 0600); err != nil {
			t.Fatal(err)
		}
		if _, _, err := iam.Load(path); err == nil {
			t.Fatal("accepted invalid account bootstrap")
		}
	}
}
