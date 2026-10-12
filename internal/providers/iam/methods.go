package iam

import (
	"cmp"
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
	case methodAccountsGetIam, methodAccountsSetIam:
		// The policy is read or written under the account's lock, so a racing
		// delete cannot unregister it in between.
		var response *gateway.RESTResponse
		err := p.withAccount(scope, request.Path["serviceAccount"], func(*scopeState, *accountRecord) error {
			var policyErr error
			if call.Method == methodAccountsGetIam {
				response, policyErr = p.getIam(target)
			} else {
				response, policyErr = p.setIam(request, target)
			}
			return policyErr
		})
		return response, err
	case methodKeysList, methodKeysGet, methodKeysCreate, methodKeysDelete:
		return p.executeKey(call.Method, request, scope)
	case methodSignBlob:
		return p.signBlob(request, scope)
	default:
		return p.accountsDelete(request, scope)
	}
}

// executeKey dispatches one authorized service account key request.
func (p *Provider) executeKey(method string, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch method {
	case methodKeysList:
		return p.keysList(request, scope)
	case methodKeysGet:
		return p.keysGet(request, scope)
	case methodKeysCreate:
		return p.keysCreate(request, scope)
	default:
		return p.keysDelete(request, scope)
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
	var response map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.accounts[id] != nil {
			return status.Errorf(codes.AlreadyExists, "Service account %s already exists", id)
		}
		record := &accountRecord{
			Project: scope.Project, ID: id,
			DisplayName: cmp.Or(body.ServiceAccount.DisplayName, body.DisplayName),
			Description: cmp.Or(body.ServiceAccount.Description, body.Description),
			Keys:        map[string]*keyRecord{}, CreatedAt: now,
		}
		if err := p.index.Put(serviceName, state.Resource{
			Name: accountResource(scope.Project, accountEmail(scope.Project, id)), Service: serviceName,
			Kind: kindAccount, Parent: provider.ProjectResource(scope.Project), Scope: scope,
			// The unique id is the account's numeric identity (an ID token's sub).
			Number: accountUniqueID(record),
		}); err != nil {
			return fmt.Errorf("index the service account: %w", err)
		}
		s.accounts[id] = record
		response = accountWire(record)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // a wire status or the wrapped index error
	}
	return &gateway.RESTResponse{JSON: response}, nil
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

// withAccount runs use on one account under the scope lock, or reports it
// missing. use must not retain the record or anything it points to.
func (p *Provider) withAccount(scope state.Scope, account string, use func(*scopeState, *accountRecord) error) error {
	return p.scopes.With(scope, func(s *scopeState) error { //nolint:wrapcheck // the callback's own errors
		record := s.accounts[account]
		if record == nil {
			return provider.NotFound(kindAccount, account)
		}
		return use(s, record)
	})
}

func (p *Provider) accountsGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withAccount(scope, request.Path["serviceAccount"], func(_ *scopeState, record *accountRecord) error {
		document = accountWire(record)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
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

// accountUpdate is a validated patch: the masked fields and their values.
type accountUpdate struct {
	ServiceAccount struct {
		DisplayName string `json:"displayName"`
		Description string `json:"description"`
	} `json:"serviceAccount"`
	UpdateMask  string `json:"updateMask"`
	displayName bool
	description bool
}

// parseAccountUpdate validates the whole body and mask before any field changes.
func parseAccountUpdate(request *gateway.RESTRequest) (accountUpdate, error) {
	var update accountUpdate
	if json.Unmarshal(request.JSON, &update) != nil {
		return update, status.Error(codes.InvalidArgument, "Invalid service account update")
	}
	for field := range strings.SplitSeq(update.UpdateMask, ",") {
		switch strings.TrimSpace(field) {
		case "displayName", "display_name":
			update.displayName = true
		case "description":
			update.description = true
		case "":
		default:
			return update, status.Errorf(codes.InvalidArgument, "Unsupported update mask field %q", field)
		}
	}
	if !update.displayName && !update.description {
		return update, status.Error(codes.InvalidArgument, "An update mask is required")
	}
	return update, nil
}

// accountsUpdate applies a validated patch all-or-nothing and echoes only the
// masked fields, as GCP does. A missing account is reported before an invalid patch.
func (p *Provider) accountsUpdate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	update, invalid := parseAccountUpdate(request)
	var response map[string]any
	if err := p.withAccount(scope, request.Path["serviceAccount"], func(_ *scopeState, record *accountRecord) error {
		if invalid != nil {
			return invalid
		}
		response = map[string]any{keyName: accountName(record.Project, record.ID)}
		if update.displayName {
			record.DisplayName = update.ServiceAccount.DisplayName
			response["displayName"] = record.DisplayName
		}
		if update.description {
			record.Description = update.ServiceAccount.Description
			response["description"] = record.Description
		}
		record.Revision++
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// accountsDelete removes one account, its keys, and its index entry together.
func (p *Provider) accountsDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	account := request.Path["serviceAccount"]
	var uniqueID string
	if err := p.withAccount(scope, account, func(s *scopeState, record *accountRecord) error {
		uniqueID = accountUniqueID(record)
		email := accountEmail(scope.Project, account)
		if err := p.index.Remove(serviceName, scope, accountResource(scope.Project, email)); err != nil {
			return fmt.Errorf("unregister the service account: %w", err)
		}
		delete(s.accounts, account)
		delete(s.systemKeys, email)
		p.engine.ForgetSigningKeys(email, "")
		return nil
	}); err != nil {
		return nil, err
	}
	// Removing the index entry dropped the account's own policy; grants made
	// to the account elsewhere become deleted members.
	p.engine.RetireMember("serviceAccount:"+accountEmail(scope.Project, account), uniqueID)
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// keysList returns the account's user-managed keys.
func (p *Provider) keysList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	items := []any{}
	if err := p.withAccount(scope, request.Path["serviceAccount"], func(_ *scopeState, record *accountRecord) error {
		for _, key := range record.Keys {
			items = append(items, keyWire(key))
		}
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{"keys": items}}, nil
}

// keysCreate generates one user-managed RSA key, publishes its public half for
// signature verification, and returns the private key file once. The key is
// generated outside the lock from a copy of the account's identity fields.
func (p *Provider) keysCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	account := request.Path["serviceAccount"]
	var identity accountRecord
	if err := p.withAccount(scope, account, func(_ *scopeState, record *accountRecord) error {
		identity = accountRecord{Project: record.Project, ID: record.ID, CreatedAt: record.CreatedAt}
		return nil
	}); err != nil {
		return nil, err
	}
	id, err := newKeyID()
	if err != nil {
		return nil, err
	}
	private, err := newRSAKey()
	if err != nil {
		return nil, err
	}
	document, err := keyFile(&identity, id, private, request.Host)
	if err != nil {
		return nil, err
	}
	key := &keyRecord{
		ID:        "projects/" + identity.Project + "/serviceAccounts/" + identity.ID + "/keys/" + id,
		CreatedAt: p.clock(),
	}
	// The account may have been deleted, or deleted and re-created, meanwhile.
	if err := p.withAccount(scope, account, func(_ *scopeState, record *accountRecord) error {
		if !record.CreatedAt.Equal(identity.CreatedAt) {
			return provider.NotFound(kindAccount, account)
		}
		record.Keys[key.ID] = key
		p.engine.PublishSigningKey(serviceName, scope, accountEmail(record.Project, record.ID), key.ID, &private.PublicKey)
		return nil
	}); err != nil {
		return nil, err
	}
	response := keyWire(key)
	response["privateKeyType"] = "TYPE_GOOGLE_CREDENTIALS_FILE"
	response["privateKeyData"] = base64.StdEncoding.EncodeToString(document)
	return &gateway.RESTResponse{JSON: response}, nil
}

// keyWire renders one key; key records are immutable once stored.
func keyWire(key *keyRecord) map[string]any {
	return map[string]any{
		keyName:           key.ID,
		"keyAlgorithm":    "KEY_ALG_RSA_2048",
		"keyOrigin":       "GOOGLE_PROVIDED",
		"keyType":         "USER_MANAGED",
		"validAfterTime":  provider.Stamp(key.CreatedAt),
		"validBeforeTime": "3999-12-31T23:59:59.999999Z",
	}
}

// keysGet returns one user-managed key.
func (p *Provider) keysGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var document map[string]any
	if err := p.withAccount(scope, request.Path["serviceAccount"], func(_ *scopeState, record *accountRecord) error {
		key, ok := record.Keys[userKeyID(record, request.Path["keyId"])]
		if !ok {
			return provider.NotFound(kindKey, request.Path["keyId"])
		}
		document = keyWire(key)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// keysDelete removes one user-managed key; signatures made with it stop verifying.
func (p *Provider) keysDelete(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if err := p.withAccount(scope, request.Path["serviceAccount"], func(_ *scopeState, record *accountRecord) error {
		keyID := userKeyID(record, request.Path["keyId"])
		if _, ok := record.Keys[keyID]; !ok {
			return provider.NotFound(kindKey, request.Path["keyId"])
		}
		delete(record.Keys, keyID)
		p.engine.ForgetSigningKeys(accountEmail(record.Project, record.ID), keyID)
		return nil
	}); err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// userKeyID is the stored name of one of the account's user-managed keys.
func userKeyID(record *accountRecord, keyID string) string {
	return "projects/" + record.Project + "/serviceAccounts/" + record.ID + "/keys/" + keyID
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
