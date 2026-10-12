package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"

	"cloud.google.com/go/auth"
	"cloud.google.com/go/iam"
	admin "cloud.google.com/go/iam/admin/apiv1"
	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"golang.org/x/oauth2"
	"google.golang.org/api/impersonate"
	"google.golang.org/api/option"
)

// localIAMCredentials sends the impersonate package's requests, which name
// https://iamcredentials.googleapis.com and take no endpoint option, to the
// gateway, authorized as the source identity. option.WithHTTPClient is the
// package's only hook for its transport.
type localIAMCredentials struct {
	gateway *url.URL
	source  *auth.Credentials
}

func (l localIAMCredentials) RoundTrip(request *http.Request) (*http.Response, error) {
	if request.URL.Host != "iamcredentials.googleapis.com" {
		return nil, fmt.Errorf("unexpected impersonation host %s", request.URL.Host)
	}
	token, err := l.source.Token(request.Context())
	if err != nil {
		return nil, fmt.Errorf("mint a local token: %w", err)
	}
	local := request.Clone(request.Context())
	local.URL.Scheme, local.URL.Host, local.Host = l.gateway.Scheme, l.gateway.Host, ""
	local.Header.Set("Authorization", "Bearer "+token.Value)
	return http.DefaultTransport.RoundTrip(local)
}

// impersonatedSource mints access tokens for target with the impersonate
// package, as the source identity.
func (e *env) impersonatedSource(ctx context.Context, target string, source *auth.Credentials) (oauth2.TokenSource, error) {
	gateway, err := url.Parse(e.http)
	if err != nil {
		return nil, fmt.Errorf("parse the gateway address: %w", err)
	}
	client := &http.Client{Transport: localIAMCredentials{gateway, source}}
	return impersonate.CredentialsTokenSource(ctx, impersonate.CredentialsConfig{
		TargetPrincipal: target, Scopes: []string{cloudPlatform}, Lifetime: 10 * time.Minute,
	}, option.WithHTTPClient(client))
}

// getStatus reads one REST resource with a bearer token and returns its status.
func (e *env) getStatus(ctx context.Context, path, token string) (int, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, e.http+path, nil)
	if err != nil {
		return 0, err
	}
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := http.DefaultClient.Do(request)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	return response.StatusCode, nil
}

// runIAMCredentials impersonates a new account with
// google.golang.org/api/impersonate and calls gcp-local as it.
func runIAMCredentials(ctx context.Context, e *env) error {
	conn, err := e.grpcOption(e.owner)
	if err != nil {
		return err
	}
	client, err := admin.NewIamClient(ctx, conn)
	if err != nil {
		return step("new client", err)
	}
	defer client.Close()
	project := "projects/" + e.project
	account, err := client.CreateServiceAccount(ctx, &adminpb.CreateServiceAccountRequest{Name: project, AccountId: "go-impersonated", ServiceAccount: &adminpb.ServiceAccount{}})
	if err != nil {
		return step("create service account", err)
	}
	name := project + "/serviceAccounts/" + account.GetEmail()
	// scripts/run/clients.py enrolls the granted identity as app@PROJECT.
	if _, err := client.SetIamPolicy(ctx, &admin.SetIamPolicyRequest{Resource: name, Policy: &iam.Policy{InternalProto: &iampb.Policy{Bindings: []*iampb.Binding{
		{Role: "roles/iam.serviceAccountTokenCreator", Members: []string{"serviceAccount:app@" + e.project + ".iam.gserviceaccount.com"}},
		{Role: "roles/iam.serviceAccountViewer", Members: []string{"serviceAccount:" + account.GetEmail()}},
	}}}}); err != nil {
		return step("grant token creator", err)
	}
	source, err := e.impersonatedSource(ctx, account.GetEmail(), e.owner)
	if err != nil {
		return step("impersonate", err)
	}
	token, err := source.Token()
	if err != nil {
		return step("impersonated token", err)
	}
	// The account may read only itself: a project-wide list runs as it, not as the caller.
	for path, want := range map[string]int{"/v1/" + name: 200, "/v1/" + project + "/serviceAccounts": 403} {
		code, err := e.getStatus(ctx, path, token.AccessToken)
		if err != nil || code != want {
			return step("impersonated GET "+path, errors.Join(err, fmt.Errorf("status %d, want %d", code, want)))
		}
	}
	_, err = e.impersonatedSource(ctx, account.GetEmail(), e.denied)
	if err == nil || !strings.Contains(err.Error(), "status code 403") {
		return step("ungranted impersonation", fmt.Errorf("want status code 403, got %v", err))
	}
	return step("delete service account", client.DeleteServiceAccount(ctx, &adminpb.DeleteServiceAccountRequest{Name: name}))
}
