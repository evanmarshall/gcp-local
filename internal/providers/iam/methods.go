package iam

import (
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodAccountsCreate:
		return p.accountsCreate(request, scope)
	case methodAccountsGet:
		return p.accountsGet(request, scope)
	case methodAccountsList:
		return p.accountsList(scope)
	case methodAccountsUpdate:
		return p.accountsUpdate(request, scope)
	case methodAccountsGetIam:
		return p.getIam(target)
	case methodAccountsSetIam:
		return p.setIam(request, target)
	case methodKeysList:
		return p.keysList(request, scope)
	case methodKeysGet:
		return p.keysGet(request, scope)
	case methodKeysCreate:
		return p.keysCreate(request, scope)
	case methodKeysDelete:
		return p.keysDelete(request, scope)
	default:
		return p.accountsDelete(request, scope)
	}
}

// accountsCreate validates and stores one service account.
func (p *Provider) accountsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body struct {
		AccountID      string `json:"accountId"`
		Name           string `json:"name"`
		ServiceAccount struct {
			DisplayName string `json:"displayName"`
			Description string `json:"description"`
		} `json:"serviceAccount"`
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid service account body")
	}
	id := body.AccountID
	if id == "" && body.Name != "" {
		id = provider.LastSegment(body.Name)
	}
	if !uid.MatchString(id) {
		return nil, status.Errorf(codes.InvalidArgument, "Invalid service account id %q", id)
	}
	now := p.clock()
	var created *accountRecord
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.accounts[id] != nil {
			return status.Errorf(codes.AlreadyExists, "Service account %s already exists", id)
		}
		displayName := body.ServiceAccount.DisplayName
		if displayName == "" {
			displayName = body.DisplayName
		}
		description := body.ServiceAccount.Description
		if description == "" {
			description = body.Description
		}
		created = &accountRecord{
			Project: scope.Project, ID: id, DisplayName: displayName,
			Description: description, Keys: map[string]*keyRecord{}, CreatedAt: now,
		}
		s.accounts[id] = created
		return nil
	})
	if err != nil {
		return nil, err
	}
	if indexErr := p.index.Put(serviceName, state.Resource{
		Name: accountResource(scope.Project, accountEmail(scope.Project, id)), Service: serviceName,
		Kind: kindAccount, Parent: provider.ProjectResource(scope.Project), Scope: scope,
		// The unique id is the account's numeric identity (an ID token's sub).
		Number: accountUniqueID(created),
	}); indexErr != nil {
		return nil, fmt.Errorf("index the service account: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: accountWire(created)}, nil
}

func accountWire(record *accountRecord) map[string]any {
	return map[string]any{
		keyName:          accountName(record.Project, record.ID),
		"projectId":      record.Project,
		"uniqueId":       accountUniqueID(record),
		"email":          accountEmail(record.Project, record.ID),
		"displayName":    record.DisplayName,
		"description":    record.Description,
		"oauth2ClientId": accountUniqueID(record),
		"etag":           accountETag(record),
	}
}

// accountUniqueID renders the 21-digit numeric id GCP assigns; the OAuth2
// client id carries the same value.
func accountUniqueID(record *accountRecord) string {
	return fmt.Sprintf("1%020d", record.CreatedAt.UnixNano())
}

// accountETag renders a revision tag that changes with each update.
func accountETag(record *accountRecord) string {
	return base64.StdEncoding.EncodeToString([]byte(strconv.FormatInt(record.CreatedAt.UnixNano()+int64(record.Revision), 36)))
}

// accountLocked reads one account under the scope lock.
func (p *Provider) accountLocked(scope state.Scope, account string) *accountRecord {
	var record *accountRecord
	_ = p.scopes.With(scope, func(s *scopeState) error { record = s.accounts[account]; return nil })
	return record
}

func (p *Provider) accountsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.accountLocked(scope, request.Path["serviceAccount"])
	if record == nil {
		return nil, provider.NotFound(kindAccount, request.Path["serviceAccount"])
	}
	return &gateway.RESTResponse{JSON: accountWire(record)}, nil
}

// accountsList returns the project's accounts.
func (p *Provider) accountsList(scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.accounts {
			items = append(items, accountWire(record))
		}
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{"accounts": items}}, nil
}

// accountsUpdate replaces display name and description.
func (p *Provider) accountsUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.accountLocked(scope, request.Path["serviceAccount"])
	if record == nil {
		return nil, provider.NotFound(kindAccount, request.Path["serviceAccount"])
	}
	var body struct {
		ServiceAccount struct {
			DisplayName string `json:"displayName"`
			Description string `json:"description"`
		} `json:"serviceAccount"`
		UpdateMask string `json:"updateMask"`
	}
	if json.Unmarshal(request.JSON, &body) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid service account update")
	}
	// The patch applies and echoes only the masked fields, as GCP does.
	response := map[string]any{keyName: accountName(record.Project, record.ID)}
	for field := range strings.SplitSeq(body.UpdateMask, ",") {
		switch strings.TrimSpace(field) {
		case "displayName", "display_name":
			record.DisplayName = body.ServiceAccount.DisplayName
			response["displayName"] = record.DisplayName
		case "description":
			record.Description = body.ServiceAccount.Description
			response["description"] = record.Description
		case "":
		default:
			return nil, status.Errorf(codes.InvalidArgument, "Unsupported update mask field %q", field)
		}
	}
	if len(response) == 1 {
		return nil, status.Error(codes.InvalidArgument, "An update mask is required")
	}
	record.Revision++
	return &gateway.RESTResponse{JSON: response}, nil
}

// accountsDelete removes one account and its keys.
func (p *Provider) accountsDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	account := request.Path["serviceAccount"]
	if p.accountLocked(scope, account) == nil {
		return nil, provider.NotFound(kindAccount, account)
	}
	var indexErr error
	_ = p.scopes.With(scope, func(s *scopeState) error {
		delete(s.accounts, account)
		indexErr = p.index.Remove(serviceName, scope, accountResource(scope.Project, accountEmail(scope.Project, account)))
		return nil
	})
	if indexErr != nil {
		return nil, fmt.Errorf("unregister the service account: %w", indexErr)
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// keysList returns the account's synthesized keys.
func (p *Provider) keysList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.accountLocked(scope, request.Path["serviceAccount"])
	if record == nil {
		return nil, provider.NotFound(kindAccount, request.Path["serviceAccount"])
	}
	items := []any{}
	for _, key := range record.Keys {
		items = append(items, keyWire(record, key))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"keys": items}}, nil
}

// keysCreate synthesizes one key record for the account.
func (p *Provider) keysCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.accountLocked(scope, request.Path["serviceAccount"])
	if record == nil {
		return nil, provider.NotFound(kindAccount, request.Path["serviceAccount"])
	}
	buffer := make([]byte, 24)
	if _, err := rand.Read(buffer); err != nil {
		return nil, status.Errorf(codes.Internal, "generate the key id: %v", err)
	}
	key := &keyRecord{
		ID:        "projects/" + record.Project + "/serviceAccounts/" + record.ID + "/keys/" + base64.RawURLEncoding.EncodeToString(buffer[:12]),
		CreatedAt: p.clock(),
	}
	_ = p.scopes.With(scope, func(_ *scopeState) error {
		record.Keys[key.ID] = key
		return nil
	})
	response := keyWire(record, key)
	response["privateKeyData"] = base64.StdEncoding.EncodeToString(buffer)
	return &gateway.RESTResponse{JSON: response}, nil
}

func keyWire(_ *accountRecord, key *keyRecord) map[string]any {
	return map[string]any{
		keyName:           key.ID,
		"keyAlgorithm":    "KEY_ALG_RSA_2048",
		"keyOrigin":       "GOOGLE_PROVIDED",
		"keyType":         "USER_MANAGED",
		"validAfterTime":  provider.Stamp(key.CreatedAt),
		"validBeforeTime": "3999-12-31T23:59:59.999999Z",
	}
}

// keysGet returns one synthesized key.
func (p *Provider) keysGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.accountLocked(scope, request.Path["serviceAccount"])
	if record == nil {
		return nil, provider.NotFound(kindAccount, request.Path["serviceAccount"])
	}
	keyID := "projects/" + record.Project + "/serviceAccounts/" + record.ID + "/keys/" + request.Path["keyId"]
	key, ok := record.Keys[keyID]
	if !ok {
		return nil, provider.NotFound(kindKey, request.Path["keyId"])
	}
	return &gateway.RESTResponse{JSON: keyWire(record, key)}, nil
}

// keysDelete removes one synthesized key.
func (p *Provider) keysDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	record := p.accountLocked(scope, request.Path["serviceAccount"])
	if record == nil {
		return nil, provider.NotFound(kindAccount, request.Path["serviceAccount"])
	}
	keyID := "projects/" + record.Project + "/serviceAccounts/" + record.ID + "/keys/" + request.Path["keyId"]
	if _, ok := record.Keys[keyID]; !ok {
		return nil, provider.NotFound(kindKey, request.Path["keyId"])
	}
	_ = p.scopes.With(scope, func(_ *scopeState) error {
		delete(record.Keys, keyID)
		return nil
	})
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// getIam returns the stored policy of the account.
func (p *Provider) getIam(target iam.Target) (*gateway.RESTResponse, error) {
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the IAM policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// setIam stores the policy of the account.
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	var envelope struct {
		Policy iam.Policy `json:"policy"`
	}
	if json.Unmarshal(request.JSON, &envelope) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid IAM policy")
	}
	policy, err := p.engine.SetPolicy(target.Name, envelope.Policy)
	if err != nil {
		// The engine's own statuses (a stale etag is ABORTED) reach the caller.
		if _, ok := status.FromError(err); ok {
			return nil, err //nolint:wrapcheck // already a wire status
		}
		return nil, status.Errorf(codes.Internal, "write the IAM policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
