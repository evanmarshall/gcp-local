package iam

import (
	"crypto"
	"crypto/rsa"
	"crypto/sha256"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// ServiceAccount is an explicit local identity resource, not a cloud import or
// a credential. Private bootstrap owns it until an IAM account provider exists.
type ServiceAccount struct {
	Project  string `json:"project"`
	Email    string `json:"email"`
	UniqueID string `json:"unique_id"`
}

func AccountResource(project, email string) string {
	return "//iam.googleapis.com/projects/" + project + "/serviceAccounts/" + email
}

// errAccountShape rejects account declarations that cannot address a resource.
var errAccountShape = errors.New("local service accounts require a matching project email and numeric unique id")

func (a ServiceAccount) resource() (state.Resource, error) {
	scope := state.Scope{Project: a.Project, Location: state.Global}
	if err := scope.Validate(); err != nil {
		return state.Resource{}, fmt.Errorf("service account scope: %w", err)
	}
	pattern := `^[a-z][a-z0-9-]{4,28}[a-z0-9]@` + regexp.QuoteMeta(a.Project) + `\.iam\.gserviceaccount\.com$`
	if !regexp.MustCompile(pattern).MatchString(a.Email) || !regexp.MustCompile(`^[1-9][0-9]{5,20}$`).MatchString(a.UniqueID) {
		return state.Resource{}, errAccountShape
	}
	return state.Resource{Name: AccountResource(a.Project, a.Email), Service: "iam", Kind: "serviceAccount", Scope: scope,
		Parent: "//cloudresourcemanager.googleapis.com/projects/" + a.Project, Number: a.UniqueID}, nil
}

// Directory is the local stand-in for Cloud Identity: the groups and domains
// GCP looks up when a policy names group:, principalSet://goog/group/, or
// domain:. It is declared once at startup and never fetched from Google.
type Directory struct {
	// Domains are the Workspace or Cloud Identity domains whose users a
	// domain:NAME member covers.
	Domains []string `json:"domains,omitempty"`
	// Groups maps a group email to its direct members: user:, serviceAccount:,
	// or group: naming another declared group.
	Groups map[string][]string `json:"groups,omitempty"`
	// CustomerID is the Workspace or Cloud Identity customer ("C0...") that
	// owns the domains and groups. iam.allowedPolicyMemberDomains admits its
	// members: users of the domains, the groups, and the service accounts of
	// local projects. Without it no member belongs to any customer.
	CustomerID string `json:"customer_id,omitempty"`
}

// errDirectoryShape rejects a directory the engine could not resolve exactly.
var errDirectoryShape = errors.New("directory groups need email names and user:, serviceAccount:, or declared group: members; domains need DNS names; a customer ID starts with C")

// customerID is a Workspace or Cloud Identity customer ID.
var customerID = regexp.MustCompile(`^C[0-9A-Za-z]{5,}$`)

// errDirectoryLate refuses a directory after policies exist: a group a stored
// policy names must never stop resolving.
var errDirectoryLate = errors.New("declare the directory before any policy")

// groupSetPrefix is GCP's principal identifier for a group, accepted in allow
// and deny policies alike.
const groupSetPrefix = "principalSet://goog/group/"

// directory is a compiled Directory. Names are lower-cased, since Google
// compares emails and domains without case. memberOf maps each user: and
// serviceAccount: member to every group containing it, directly or nested.
type directory struct {
	domains  map[string]bool
	groups   map[string]bool
	memberOf map[string][]string
	customer string
}

// compileDirectory validates a declared directory and resolves transitive
// membership once, so evaluation never walks the group graph. Nesting cycles
// are allowed; each group is visited once.
func compileDirectory(declared Directory) (directory, error) {
	compiled := directory{domains: map[string]bool{}, groups: map[string]bool{}, memberOf: map[string][]string{}, customer: declared.CustomerID}
	for _, domain := range declared.Domains {
		if domainNameError(domain) != "" {
			return directory{}, errDirectoryShape
		}
		compiled.domains[strings.ToLower(domain)] = true
	}
	for group := range declared.Groups {
		if !emailShaped(group) || serviceAccountEmail(group) || compiled.groups[strings.ToLower(group)] {
			return directory{}, errDirectoryShape
		}
		compiled.groups[strings.ToLower(group)] = true
	}
	parents, err := compiled.parentEdges(declared.Groups)
	if err != nil {
		return directory{}, err
	}
	for key := range parents {
		if !strings.HasPrefix(key, "group:") {
			compiled.memberOf[key] = containingGroups(parents, key)
		}
	}
	return compiled, nil
}

// parentEdges maps each lower-cased member to the groups that list it
// directly, refusing members that aren't users, service accounts, or declared
// groups.
func (d directory) parentEdges(groups map[string][]string) (map[string][]string, error) {
	parents := map[string][]string{}
	for group, members := range groups {
		for _, member := range members {
			kind, email, _ := strings.Cut(member, ":")
			known := kind == "user" || kind == "serviceAccount" || (kind == "group" && d.groups[strings.ToLower(email)])
			if !known || !emailShaped(email) {
				return nil, errDirectoryShape
			}
			key := kind + ":" + strings.ToLower(email)
			parents[key] = append(parents[key], strings.ToLower(group))
		}
	}
	return parents, nil
}

// containingGroups walks the parent edges from one member to every group
// that contains it.
func containingGroups(parents map[string][]string, member string) []string {
	seen := map[string]bool{}
	queue := append([]string(nil), parents[member]...)
	for len(queue) > 0 {
		group := queue[0]
		queue = queue[1:]
		if !seen[group] {
			seen[group] = true
			queue = append(queue, parents["group:"+group]...)
		}
	}
	groups := make([]string, 0, len(seen))
	for group := range seen {
		groups = append(groups, group)
	}
	return groups
}

// SetDirectory declares the local group directory. It must run before any
// policy is stored, so no stored member or deny principal can stop resolving.
// There is no reload: changing the directory needs a restart.
func (e *Engine) SetDirectory(declared Directory) error {
	if declared.CustomerID != "" && !customerID.MatchString(declared.CustomerID) {
		return errDirectoryShape
	}
	compiled, err := compileDirectory(declared)
	if err != nil {
		return err
	}
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.policies) > 0 {
		return errDirectoryLate
	}
	e.directory = compiled
	return nil
}

// memberships returns the lower-cased directory members that name an
// authenticated principal: group:G and principalSet://goog/group/G for each
// group containing it, and domain:D when it is a user of a declared domain.
func (d directory) memberships(principal gateway.Principal) map[string]bool {
	names := map[string]bool{}
	if principal.State != gateway.Authenticated {
		return names
	}
	kind, email, _ := strings.Cut(principal.Subject, ":")
	email = strings.ToLower(email)
	for _, group := range d.memberOf[kind+":"+email] {
		names["group:"+group] = true
		names[strings.ToLower(groupSetPrefix)+group] = true
	}
	if _, domain, ok := strings.Cut(email, "@"); ok && kind == "user" && d.domains[domain] {
		names["domain:"+domain] = true
	}
	return names
}

// checkMember validates a group or domain member against the directory, with
// the errors GCP returns for a directory it cannot resolve (recorded in
// storage-iam-directory). handled is false for every other member form.
func (d directory) checkMember(member string) (bool, error) {
	if domain, ok := strings.CutPrefix(member, "domain:"); ok {
		if reason := domainNameError(domain); reason != "" {
			return true, status.Error(codes.InvalidArgument, reason)
		}
		if !d.domains[strings.ToLower(domain)] {
			return true, status.Errorf(codes.InvalidArgument, "Domain %s does not exist.", domain)
		}
		return true, nil
	}
	group, ok := strings.CutPrefix(member, "group:")
	if !ok {
		group, ok = strings.CutPrefix(member, groupSetPrefix)
	}
	if !ok {
		return false, nil
	}
	switch {
	case group == "":
		return true, status.Error(codes.InvalidArgument, "The reference to a group identity in the IAM policy is empty.")
	case serviceAccountEmail(group):
		return true, status.Errorf(codes.InvalidArgument, `Principal %s is of type "serviceAccount". The principal should appear as "serviceAccount:%s". `+
			"See https://cloud.google.com/iam/help/members/types for additional documentation.", group, group)
	case !d.groups[strings.ToLower(group)]:
		return true, status.Errorf(codes.InvalidArgument, "Group %s does not exist.", group)
	}
	return true, nil
}

// domainLabel is the pattern GCP names when it rejects a domain member.
var domainLabel = regexp.MustCompile(`^[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?$`)

// domainNameError returns GCP's message for a malformed domain, or "".
func domainNameError(domain string) string {
	for piece := range strings.SplitSeq(domain, ".") {
		if !domainLabel.MatchString(piece) {
			return fmt.Sprintf(`%s is an invalid domain name because the piece "%s" does not match the pattern %s.`, domain, piece,
				"[a-zA-Z0-9]([a-zA-Z0-9-]{0,61}[a-zA-Z0-9])?")
		}
	}
	return ""
}

// emailShaped reports whether a directory name is one email address.
func emailShaped(email string) bool {
	local, domain, ok := strings.Cut(email, "@")
	return ok && local != "" && !strings.ContainsAny(local, " \r\n\t:/") && domainNameError(domain) == ""
}

// serviceAccountEmail reports Google's service account domains, including
// service agents (gcp-sa-*.iam) and default accounts (developer, appspot).
func serviceAccountEmail(email string) bool {
	return strings.HasSuffix(strings.ToLower(email), ".gserviceaccount.com")
}

// signingKey is the public half of one service-account key. The provider that
// created the key owns its private half and its lifecycle; the engine only
// verifies, so a service such as Storage can check a signature without
// depending on that provider.
type signingKey struct {
	public *rsa.PublicKey
	owner  string
	scope  state.Scope
}

// PublishSigningKey records the public key a service account signs with.
// ClearScope with the same owner and scope removes it.
func (e *Engine) PublishSigningKey(owner string, scope state.Scope, account, keyID string, public *rsa.PublicKey) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.signingKeys[account] == nil {
		e.signingKeys[account] = map[string]signingKey{}
	}
	e.signingKeys[account][keyID] = signingKey{public: public, owner: owner, scope: scope}
}

// ForgetSigningKeys removes one key of an account, or every key when keyID is empty.
func (e *Engine) ForgetSigningKeys(account, keyID string) {
	e.mu.Lock()
	defer e.mu.Unlock()
	if keyID == "" {
		delete(e.signingKeys, account)
		return
	}
	delete(e.signingKeys[account], keyID)
}

// VerifySignature reports whether one of the account's published keys made an
// RSASSA-PKCS1-v1_5 SHA-256 signature over message, and the account's scope.
func (e *Engine) VerifySignature(account string, message, signature []byte) (state.Scope, bool) {
	digest := sha256.Sum256(message)
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, key := range e.signingKeys[account] {
		if rsa.VerifyPKCS1v15(key.public, crypto.SHA256, digest[:], signature) == nil {
			return key.scope, true
		}
	}
	return state.Scope{}, false
}

// clearSigningKeys drops the keys one owner published in one scope.
func (e *Engine) clearSigningKeys(owner string, scope state.Scope) {
	for account, keys := range e.signingKeys {
		for id, key := range keys {
			if key.owner == owner && key.scope == scope {
				delete(keys, id)
			}
		}
		if len(keys) == 0 {
			delete(e.signingKeys, account)
		}
	}
}
