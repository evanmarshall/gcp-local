package iam

import (
	"cmp"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/base64"
	"encoding/pem"
	"errors"
	"fmt"
	"maps"
	"math/big"
	"slices"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/iam/admin/apiv1/adminpb"
	"cloud.google.com/go/iam/apiv1/iampb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/iam/orgpolicy"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// execute dispatches one authorized request.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest, scope state.Scope, target iam.Target, decision string) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodAccountsCreate:
		return p.accountsCreate(request, scope)
	case methodAccountsGet:
		return p.accountsGet(request, scope)
	case methodAccountsList:
		return p.accountsList(request, scope)
	case methodAccountsUpdate:
		return p.accountsUpdate(request, scope)
	case methodAccountsGetIam, methodAccountsSetIam:
		// The policy is read or written under the account's lock, so a racing
		// delete cannot unregister it in between.
		var response *gateway.RESTResponse
		err := p.withAccount(scope, request.Path["serviceAccount"], func(*scopeState, *accountRecord) error {
			var policyErr error
			if call.Method == methodAccountsGetIam {
				response, policyErr = p.getIam(request, target)
			} else {
				response, policyErr = p.setIam(request, target, decision)
			}
			return policyErr
		})
		return response, err
	case methodKeysList, methodKeysGet, methodKeysCreate, methodKeysDelete:
		return p.executeKey(call.Method, request, scope, target, decision)
	case methodSignBlob, methodSignJwt, methodAccessToken, methodIDToken:
		return p.executeCredentials(call.Method, request, scope)
	default:
		return p.accountsDelete(request, scope)
	}
}

// executeCredentials dispatches one authorized IAM Credentials request.
func (p *Provider) executeCredentials(method string, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	email := accountEmail(scope.Project, request.Path["serviceAccount"])
	switch method {
	case methodSignBlob:
		return p.signBlob(request, scope, email)
	case methodSignJwt:
		return p.signJwt(request, scope, email)
	case methodAccessToken:
		return p.generateAccessToken(request, scope, email)
	default:
		return p.generateIDToken(request, scope, email)
	}
}

// executeKey dispatches one authorized service account key request.
func (p *Provider) executeKey(method string, request *gateway.RESTRequest, scope state.Scope, target iam.Target, decision string) (*gateway.RESTResponse, error) {
	switch method {
	case methodKeysList:
		return p.keysList(request, scope)
	case methodKeysGet:
		return p.keysGet(request, scope)
	case methodKeysCreate:
		// GCP refuses a malformed body before the organization policy
		// (recorded: serviceaccount-lifecycle-authorization "key-unknown-field").
		if err := checkKeyRequest(request.JSON); err != nil {
			return nil, err
		}
		if err := p.keyCreationPolicy(decision, target); err != nil {
			return nil, err
		}
		return p.keysCreate(request, scope)
	default:
		return p.keysDelete(request, scope)
	}
}

// accountsCreate validates and stores one service account. The body is a
// CreateServiceAccountRequest; output-only account fields are ignored.
func (p *Provider) accountsCreate(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var body adminpb.CreateServiceAccountRequest
	if err := provider.DecodeProto(request.JSON, "", &body, gap); err != nil {
		return nil, err
	}
	id := body.GetAccountId()
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
			DisplayName: body.GetServiceAccount().GetDisplayName(),
			Description: body.GetServiceAccount().GetDescription(),
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
			return errUnknownAccount
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

// Recorded list refusals (serviceaccount-lifecycle-authorization).
var (
	// "list-negative-size" and "list-oversize".
	errPageSize = status.Error(codes.InvalidArgument, "The field page_size is out of range. It must be between 0 and 100.")
	// accounts is the documented default and maximum page size; "list-invalid-token".
	accounts = provider.Paging{Default: 20, Max: 100, NegativeSize: errPageSize,
		InvalidToken: status.Error(codes.InvalidArgument, "Request contains an invalid argument.")}
)

// accountsList returns one page of the project's accounts in unique-id
// order (recorded "list-paged"); unique ids grow with creation here.
func (p *Provider) accountsList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if size, err := strconv.Atoi(request.Query.Get("pageSize")); err == nil && size > accounts.Max {
		return nil, errPageSize
	}
	var body map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		records := slices.SortedFunc(maps.Values(s.accounts), func(a, b *accountRecord) int {
			return cmp.Or(cmp.Compare(accountUniqueID(a), accountUniqueID(b)), cmp.Compare(a.ID, b.ID))
		})
		page, next, err := provider.Page(records, request.Query, accounts, gap)
		if err != nil {
			return err
		}
		items := make([]any, 0, len(page))
		for _, record := range page {
			items = append(items, accountWire(record))
		}
		body = provider.ListBody("accounts", items, next)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("list service accounts: %w", err)
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// accountUpdate is a validated patch: the masked fields and their values.
type accountUpdate struct {
	displayName, description bool
	values                   *adminpb.ServiceAccount
}

// parseAccountUpdate validates the whole body, a PatchServiceAccountRequest,
// and its mask before any field changes.
func parseAccountUpdate(request *gateway.RESTRequest) (accountUpdate, error) {
	var update accountUpdate
	var body adminpb.PatchServiceAccountRequest
	if err := provider.DecodeProto(request.JSON, "", &body, gap); err != nil {
		return update, err
	}
	update.values = body.GetServiceAccount()
	for _, field := range body.GetUpdateMask().GetPaths() {
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
			record.DisplayName = update.values.GetDisplayName()
			response["displayName"] = record.DisplayName
		}
		if update.description {
			record.Description = update.values.GetDescription()
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

// Key types, private key file types, and key algorithms of the IAM API.
const (
	keyTypeUser   = "USER_MANAGED"
	keyTypeSystem = "SYSTEM_MANAGED"
	keyAlgorithm  = "KEY_ALG_RSA_2048"
	// systemKeyLifetime bounds a system-managed key's validity (not recorded).
	systemKeyLifetime = 2 * 365 * 24 * time.Hour
)

// keyTypes reads a keys.list keyTypes filter, by name or number; none lists
// both types, as KEY_TYPE_UNSPECIFIED does.
func keyTypes(values []string) (map[string]bool, error) {
	selected := map[string]bool{}
	for i, value := range values {
		switch value {
		case keyTypeUser, "1":
			selected[keyTypeUser] = true
		case keyTypeSystem, "2":
			selected[keyTypeSystem] = true
		case "KEY_TYPE_UNSPECIFIED", "0":
		default:
			field := fmt.Sprintf("key_types[%d]", i)
			return nil, provider.FieldViolation(field, fmt.Sprintf(
				"Invalid value at '%s' (type.googleapis.com/google.iam.admin.v1.ListServiceAccountKeysRequest.KeyType), %q", field, value))
		}
	}
	if len(selected) == 0 {
		selected = map[string]bool{keyTypeUser: true, keyTypeSystem: true}
	}
	return selected, nil
}

// keysList returns the account's keys of the requested types in name order.
// Every account has a system-managed key (recorded
// serviceaccount-lifecycle-authorization "keys-listed"); it is the key
// signBlob signs with, generated on first use. The method has no paging.
func (p *Provider) keysList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	types, err := keyTypes(request.Query["keyTypes"])
	if err != nil {
		return nil, err
	}
	account := request.Path["serviceAccount"]
	email := accountEmail(scope.Project, account)
	if types[keyTypeSystem] {
		if err := p.withAccount(scope, account, func(*scopeState, *accountRecord) error { return nil }); err != nil {
			return nil, err
		}
		if _, err := p.systemKey(scope, email); err != nil {
			return nil, err
		}
	}
	var items []map[string]any
	if err := p.withAccount(scope, account, func(s *scopeState, record *accountRecord) error {
		if system := s.systemKeys[email]; types[keyTypeSystem] && system != nil {
			items = append(items, systemKeyWire(record, system))
		}
		if types[keyTypeUser] {
			for _, key := range record.Keys {
				items = append(items, keyWire(record, key))
			}
		}
		return nil
	}); err != nil {
		return nil, err
	}
	slices.SortFunc(items, func(a, b map[string]any) int {
		return cmp.Compare(provider.Text(a[keyName]), provider.Text(b[keyName]))
	})
	return &gateway.RESTResponse{JSON: provider.ListBody("keys", items, "")}, nil
}

// keyCreationConstraints refuse user-managed key creation; the managed one
// is checked first (the sandbox enforces it).
var keyCreationConstraints = []string{orgpolicy.ManagedKeyCreation, orgpolicy.KeyCreation}

// keyCreationPolicy refuses a key creation an enforced constraint blocks,
// after IAM allowed it: an IAM denial comes first (recorded:
// orgpolicy-constraint-enforcement, which also records the managed
// constraint's refusal; the legacy constraint's is modeled).
func (p *Provider) keyCreationPolicy(decision string, target iam.Target) error {
	name := p.uniqueName(target)
	for _, constraint := range keyCreationConstraints {
		err := p.engine.CheckOrgPolicy(decision, target.Name, constraint)
		if _, violated := errors.AsType[*iam.OrgPolicyError](err); !violated {
			if err != nil {
				return fmt.Errorf("check %s: %w", constraint, err)
			}
			continue
		}
		reference := "constraints/" + constraint
		if constraint == orgpolicy.ManagedKeyCreation {
			definition, _ := orgpolicy.Lookup(constraint)
			message := fmt.Sprintf("Operation denied by org policy on resource '%s': [%q: %q].", name, reference, definition.Description())
			return provider.ErrorInfo(codes.FailedPrecondition, message, "CUSTOM_ORG_POLICY_VIOLATION", "iam.googleapis.com",
				map[string]string{"customConstraints": reference, "resource": name})
		}
		const message = "Key creation is not allowed on this service account."
		detailed, _ := status.New(codes.FailedPrecondition, message).WithDetails(&errdetails.PreconditionFailure{
			Violations: []*errdetails.PreconditionFailure_Violation{{Type: reference, Subject: "orgpolicy:" + name, Description: message}}})
		return detailed.Err()
	}
	return nil
}

// uniqueName is an account's name by unique ID, as its refusals name it:
// projects/-/serviceAccounts/UNIQUE_ID.
func (p *Provider) uniqueName(target iam.Target) string {
	resource, _ := p.index.Get(target.Name)
	return "projects/-/serviceAccounts/" + resource.Number
}

// keysCreate generates one user-managed RSA key, publishes its public half for
// signature verification, and returns the private key file once. The key is
// generated outside the lock from a copy of the account's identity fields.
// Only the default file type and algorithm are modeled.
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
	now := p.clock()
	certificate, err := selfSigned(private, &identity, now, userKeyExpiry)
	if err != nil {
		return nil, err
	}
	key := &keyRecord{ID: userKeyID(&identity, id), CreatedAt: now, Public: &private.PublicKey, Certificate: certificate}
	// The account may have been deleted, or deleted and re-created, meanwhile.
	if err := p.withAccount(scope, account, func(_ *scopeState, record *accountRecord) error {
		if !record.CreatedAt.Equal(identity.CreatedAt) {
			return errUnknownAccount
		}
		record.Keys[key.ID] = key
		p.engine.PublishSigningKey(serviceName, scope, accountEmail(record.Project, record.ID), key.ID, &private.PublicKey)
		return nil
	}); err != nil {
		return nil, err
	}
	response := keyWire(&identity, key)
	response["privateKeyType"] = "TYPE_GOOGLE_CREDENTIALS_FILE"
	response["privateKeyData"] = base64.StdEncoding.EncodeToString(document)
	return &gateway.RESTResponse{JSON: response}, nil
}

// checkKeyRequest strictly decodes a key create body; only the default file
// type and algorithm are modeled.
func checkKeyRequest(raw []byte) error {
	var body adminpb.CreateServiceAccountKeyRequest
	if err := provider.DecodeProto(raw, "", &body, gap); err != nil {
		return err
	}
	if kind := body.GetPrivateKeyType(); kind != adminpb.ServiceAccountPrivateKeyType_TYPE_UNSPECIFIED &&
		kind != adminpb.ServiceAccountPrivateKeyType_TYPE_GOOGLE_CREDENTIALS_FILE {
		return gap("privateKeyType " + kind.String() + " is not modeled")
	}
	if algorithm := body.GetKeyAlgorithm(); algorithm != adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_UNSPECIFIED &&
		algorithm != adminpb.ServiceAccountKeyAlgorithm_KEY_ALG_RSA_2048 {
		return gap("keyAlgorithm " + algorithm.String() + " is not modeled")
	}
	return nil
}

// userKeyExpiry is a user-managed key's validBeforeTime: it never expires.
var userKeyExpiry = time.Date(3999, 12, 31, 23, 59, 59, 999999000, time.UTC)

// keyWireName names one key by its account's email, as GCP does.
func keyWireName(record *accountRecord, keyID string) string {
	return accountName(record.Project, record.ID) + "/keys/" + keyID
}

// keyWire renders one user-managed key; key records are immutable once stored.
func keyWire(record *accountRecord, key *keyRecord) map[string]any {
	return map[string]any{
		keyName:           keyWireName(record, provider.LastSegment(key.ID)),
		"keyAlgorithm":    keyAlgorithm,
		"keyOrigin":       "GOOGLE_PROVIDED",
		"keyType":         keyTypeUser,
		"validAfterTime":  provider.Stamp(key.CreatedAt),
		"validBeforeTime": provider.Stamp(userKeyExpiry),
	}
}

// systemKeyWire renders the account's system-managed key.
func systemKeyWire(record *accountRecord, key *systemKey) map[string]any {
	return map[string]any{
		keyName:           keyWireName(record, key.ID),
		"keyAlgorithm":    keyAlgorithm,
		"keyOrigin":       "GOOGLE_PROVIDED",
		"keyType":         keyTypeSystem,
		"validAfterTime":  provider.Stamp(record.CreatedAt),
		"validBeforeTime": provider.Stamp(record.CreatedAt.Add(systemKeyLifetime)),
	}
}

// Public key formats of a keys.get (ServiceAccountPublicKeyType).
const (
	formatNone = ""
	formatPEM  = "TYPE_X509_PEM_FILE"
	formatRaw  = "TYPE_RAW_PUBLIC_KEY"
)

// publicKeyFormat reads publicKeyType by name or number.
func publicKeyFormat(value string) (string, error) {
	switch value {
	case "", "TYPE_NONE", "0":
		return formatNone, nil
	case formatPEM, "1":
		return formatPEM, nil
	case formatRaw, "2":
		return formatRaw, nil
	default:
		return "", provider.FieldViolation("public_key_type", fmt.Sprintf(
			"Invalid value at 'public_key_type' (type.googleapis.com/google.iam.admin.v1.ServiceAccountPublicKeyType), %q", value))
	}
}

// publicKeyData renders a key's public half in a format: its self-signed
// certificate as PEM, or its DER public key.
func publicKeyData(format string, public *rsa.PublicKey, certificate []byte) (string, error) {
	if format == formatPEM {
		return base64.StdEncoding.EncodeToString(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificate})), nil
	}
	der, err := x509.MarshalPKIXPublicKey(public)
	if err != nil {
		return "", status.Errorf(codes.Internal, "encode the public key: %v", err)
	}
	return base64.StdEncoding.EncodeToString(der), nil
}

// keysGet returns one key of the account. publicKeyType TYPE_X509_PEM_FILE
// adds the key's self-signed certificate, TYPE_RAW_PUBLIC_KEY its DER public
// key, and TYPE_NONE (the default) nothing.
func (p *Provider) keysGet(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	format, err := publicKeyFormat(request.Query.Get("publicKeyType"))
	if err != nil {
		return nil, err
	}
	keyID := request.Path["keyId"]
	var document map[string]any
	var public *rsa.PublicKey
	var certificate []byte
	err = p.withAccount(scope, request.Path["serviceAccount"], func(s *scopeState, record *accountRecord) error {
		if key, ok := record.Keys[userKeyID(record, keyID)]; ok {
			document, public, certificate = keyWire(record, key), key.Public, key.Certificate
			return nil
		}
		system := s.systemKeys[accountEmail(record.Project, record.ID)]
		if system == nil || system.ID != keyID {
			return provider.NotFound(kindKey, keyID)
		}
		document, public = systemKeyWire(record, system), &system.Private.PublicKey
		if format != formatPEM {
			return nil
		}
		var err error
		certificate, err = selfSigned(system.Private, record, record.CreatedAt, record.CreatedAt.Add(systemKeyLifetime))
		return err
	})
	if err != nil {
		return nil, err
	}
	if format != formatNone {
		if document["publicKeyData"], err = publicKeyData(format, public, certificate); err != nil {
			return nil, err
		}
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

// selfSigned issues the certificate keys.get returns for a key: subject and
// issuer the account's unique id, valid from notBefore to notAfter.
func selfSigned(key *rsa.PrivateKey, record *accountRecord, notBefore, notAfter time.Time) ([]byte, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 64))
	if err != nil {
		return nil, status.Errorf(codes.Internal, "generate the certificate serial: %v", err)
	}
	name := pkix.Name{CommonName: accountUniqueID(record)}
	template := &x509.Certificate{SerialNumber: serial, Subject: name, Issuer: name, NotBefore: notBefore, NotAfter: notAfter,
		KeyUsage: x509.KeyUsageDigitalSignature, BasicConstraintsValid: true}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "issue the key certificate: %v", err)
	}
	return der, nil
}

// userKeyID is the stored name of one of the account's user-managed keys.
func userKeyID(record *accountRecord, keyID string) string {
	return "projects/" + record.Project + "/serviceAccounts/" + record.ID + "/keys/" + keyID
}

// getIam returns the stored policy of the account. The body is a
// GetIamPolicyRequest; every requested policy version reads the stored policy.
func (p *Provider) getIam(request *gateway.RESTRequest, target iam.Target) (*gateway.RESTResponse, error) {
	if err := provider.DecodeProto(request.JSON, "", &iampb.GetIamPolicyRequest{}, gap); err != nil {
		return nil, err
	}
	policy, err := p.engine.GetPolicy(target.Name)
	if err != nil {
		return nil, status.Errorf(codes.Internal, "read the IAM policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy.Wire()}, nil
}

// setIam stores the policy of the account. A member outside the permitted
// customers is refused naming the account by unique ID (recorded:
// orgpolicy-constraint-enforcement).
func (p *Provider) setIam(request *gateway.RESTRequest, target iam.Target, decision string) (*gateway.RESTResponse, error) {
	decoded, err := provider.DecodePolicy(request.JSON, gap)
	if err != nil {
		return nil, err
	}
	policy, err := p.engine.SetPolicyFor(decision, target.Name, decoded)
	if violation, ok := errors.AsType[*iam.OrgPolicyError](err); ok {
		violation.Resource = "//iam.googleapis.com/" + p.uniqueName(target)
		return nil, violation
	}
	if err != nil {
		// The engine's own statuses (a stale etag is ABORTED) reach the caller.
		if _, ok := status.FromError(err); ok {
			return nil, err //nolint:wrapcheck // already a wire status
		}
		return nil, status.Errorf(codes.Internal, "write the IAM policy: %v", err)
	}
	return &gateway.RESTResponse{JSON: policy}, nil
}
