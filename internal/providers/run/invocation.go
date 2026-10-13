package run

import (
	"bytes"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/evanmarshall/gcp-local/internal/identity"
)

// Config carries the invocation-time dependencies of the run provider.
type Config struct {
	// Tokens verifies local ID tokens minted for service audiences.
	Tokens *identity.IDTokens
	// BaseURL is the loopback base of the serving listener, for service URIs.
	BaseURL string
	// Metadata builds the per-subject workload metadata handler for one
	// enrolled service account. nil disables workload identity.
	Metadata func(subject string) (http.Handler, error)
	// MetadataBind binds one workload metadata handler and returns the
	// host:port the container must dial. Called only when Metadata is set.
	MetadataBind func(handler http.Handler) (string, func(), error)
}

// invocationPath is the local serving prefix, one level below the local
// administrative namespace so it never collides with a service contract.
const invocationPath = "/_run/"

// Invocation serves the local traffic surface of deployed services. It is the
// local stand-in for the run.app HTTPS endpoint: audience-bound ID-token
// verification and the run.invoker grant are checked before any forwarding.
func (p *Provider) Invocation() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		host, path, refused := splitInvocation(r.URL.Path)
		if refused != nil {
			writeInvocationError(w, refused)
			return
		}
		serving, err := p.lookupServing(host)
		if err != nil {
			writeInvocationError(w, err)
			return
		}
		if serving.address == "" {
			// The reaper stopped the container: start it again, or wait for
			// the cold start another invocation began.
			p.coldStart(serving) //nolint:contextcheck // the restarted revision outlives the invocation by design
			if serving, err = p.lookupServing(host); err == nil && serving.address == "" {
				err = status.Error(codes.Unavailable, "The revision container failed to cold-start")
			}
			if err != nil {
				writeInvocationError(w, err)
				return
			}
		}
		p.touchActivity(serving)
		if err := p.authorizeInvocation(r, serving); err != nil {
			writeInvocationError(w, err)
			return
		}
		p.forward(w, r, serving, path)
	})
}

// splitInvocation separates the local service host from the request path.
func splitInvocation(path string) (string, string, error) {
	if !strings.HasPrefix(path, invocationPath) {
		return "", "", status.Error(codes.NotFound, "No service is bound to this path")
	}
	// A push delivery may target the service root without a trailing path:
	// the fragment defaults to "/" (the real surface treats the bare host
	// URL as the service root).
	rest := strings.TrimPrefix(path, invocationPath)
	host, fragment, found := strings.Cut(rest, "/")
	if !found && rest != "" {
		return rest, "/", nil
	}
	if !found || host == "" {
		return "", "", status.Error(codes.NotFound, "No service is bound to this path")
	}
	if fragment == "" {
		fragment = "/"
	}
	return host, fragment, nil
}

// servingRef is one resolved, running service for invocation.
type servingRef struct {
	project  string
	location string
	id       string
	revision string
	address  string
}

// lookupServing resolves the host to its serving revision: the newest ready
// one, since all traffic goes to the latest revision. A reaped revision
// resolves with an empty address, for the caller to cold-start.
func (p *Provider) lookupServing(host string) (servingRef, error) {
	p.hostMu.RLock()
	entry, ok := p.hosts[host]
	p.hostMu.RUnlock()
	unbound := status.Errorf(codes.NotFound, "No service is bound to host %q", host)
	if !ok {
		return servingRef{}, unbound
	}
	scope := state.Scope{Project: entry.project, Location: entry.location}
	serving := servingRef{project: scope.Project, location: scope.Location, id: entry.id}
	var unavailable bool
	if err := p.withService(scope, entry.id, func(_ *scopeState, record *serviceRecord) error {
		revision := servingRevision(record)
		switch {
		case revision == nil || (revision.Address == "" && !revision.reaped):
			unavailable = true
		default:
			serving.revision, serving.address = revision.ID, revision.Address
		}
		return nil
	}); err != nil {
		return servingRef{}, unbound
	}
	if unavailable {
		return servingRef{}, status.Error(codes.Unavailable, "No ready revision is serving traffic")
	}
	return serving, nil
}

// forward proxies the request to the revision container. Only the method,
// path, and headers the application needs are forwarded; the local
// authorization header never reaches the container.
func (p *Provider) forward(w http.ResponseWriter, r *http.Request, serving servingRef, path string) {
	if !revisionAddress(serving.address) {
		writeInvocationError(w, status.Error(codes.Internal, "The revision address is not a loopback or private-network endpoint"))
		return
	}
	// The incoming body may be chunked (an unknown-length reader makes
	// NewRequest re-chunk it and drop Content-Length), and plain HTTP
	// servers that size their read by Content-Length then see an empty
	// body. Buffer the bounded invocation body and forward it fixed-length.
	body, bodyErr := io.ReadAll(io.LimitReader(r.Body, invocationBodyLimit+1))
	if bodyErr != nil {
		writeInvocationError(w, status.Error(codes.Internal, "Read the invocation body"))
		return
	}
	if int64(len(body)) > invocationBodyLimit {
		writeInvocationError(w, status.Error(codes.InvalidArgument, "The invocation body exceeds the supported size"))
		return
	}
	target, err := http.NewRequestWithContext(r.Context(), r.Method, "http://"+serving.address+path, bytes.NewReader(body)) //nolint:gosec // the address is the provider-owned revision port and the path is a single local URL path

	if err != nil {
		writeInvocationError(w, status.Error(codes.Internal, "Build the container request"))
		return
	}
	for _, key := range []string{"Content-Type", "Accept", "X-Forwarded-For"} {
		if value := r.Header.Get(key); value != "" {
			target.Header.Set(key, value)
		}
	}
	response, err := http.DefaultClient.Do(target) //nolint:gosec // the target URL is validated above as a local revision port
	if err != nil {
		writeInvocationError(w, status.Error(codes.Unavailable, "The revision container is unreachable"))
		return
	}
	defer func() { _ = response.Body.Close() }()
	headers := w.Header()
	for key, values := range response.Header {
		for _, value := range values {
			headers.Add(key, value)
		}
	}
	w.WriteHeader(response.StatusCode)
	// The container's bytes are proxied verbatim, mirroring the HTTPS
	// terminator semantics of the real service endpoint.
	_, _ = io.Copy(w, response.Body)
}

// authorizeInvocation enforces the Cloud Run invocation contract: a local ID
// token bound to the service URI, or an unauthenticated caller only when the
// service policy grants run.invoker to allUsers.
func (p *Provider) authorizeInvocation(r *http.Request, serving servingRef) error {
	bearer := strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer ")
	if bearer != "" && bearer != r.Header.Get("Authorization") {
		return p.authorizeToken(r, bearer, serving)
	}
	return p.requirePublic(serving)
}

// invocationStatus maps the gRPC codes the invocation surface can return to
// their HTTP status.
var invocationStatus = map[codes.Code]int{
	codes.NotFound:         404,
	codes.Unavailable:      503,
	codes.Unauthenticated:  401,
	codes.PermissionDenied: 403,
	codes.InvalidArgument:  400,
}

// writeInvocationError renders a Cloud Run-style invocation error.
func writeInvocationError(w http.ResponseWriter, err error) {
	code := 500
	if mapped, ok := invocationStatus[status.Code(err)]; ok {
		code = mapped
	}
	body := provider.WireError(code, status.Code(err).String(), status.Convert(err).Message())
	body.Headers = http.Header{}
	for _, header := range body.Headers.Values("Content-Type") {
		w.Header().Set("Content-Type", header)
	}
	w.WriteHeader(code)
	// The client observes the status first; a body write race cannot happen.
	// strconv.Quote escapes every control character before the JSON template.
	fmt.Fprintf(w, `{"error":{"code":%d,"message":%s}}`, code, strconv.Quote(status.Convert(err).Message())) //nolint:gosec // the quoted message is fixed-shape before interpolation
}

// authorizeToken verifies a locally minted ID token and the invoker grant.
func (p *Provider) authorizeToken(r *http.Request, bearer string, serving servingRef) error {
	if p.tokens == nil {
		return status.Error(codes.Unauthenticated, "Local ID-token verification is not configured")
	}
	claims, verifyErr := p.tokens.Verify(bearer, p.audienceOf(serving))
	if verifyErr != nil {
		return status.Error(codes.Unauthenticated, "Invalid local ID token")
	}
	email := strings.TrimSuffix(claims.Email, "@"+serving.project+".iam.gserviceaccount.com")
	if email == "" {
		return status.Error(codes.Unauthenticated, "The ID token names no service account")
	}
	// The verified token mints from this runtime's key for cloud-platform
	// workloads; the acting principal inherits that verified scope set.
	call := gateway.Call{
		Context: r.Context(),
		Method:  methodServicesInvoke,
		Project: serving.project,
		Principal: gateway.Principal{
			State: gateway.Authenticated, Subject: "serviceAccount:" + claims.Email,
			Scopes: []string{identity.CloudPlatform},
		},
	}
	target := iam.Target{Name: serviceResource(serving.project, serving.location, serving.id)}
	if _, authorizeErr := p.engine.Authorize(call, target, nil); authorizeErr != nil {
		if status.Code(authorizeErr) == codes.PermissionDenied || status.Code(authorizeErr) == codes.Unauthenticated {
			return status.Error(codes.PermissionDenied, "The caller does not have the run.invoker permission")
		}
		return fmt.Errorf("authorize the invocation: %w", authorizeErr)
	}
	return nil
}

// requirePublic refuses anonymous traffic with 403, as GCP does (recording
// run-service-lifecycle), unless the service policy grants run.invoker to
// allUsers. An invalid token is 401.
func (p *Provider) requirePublic(serving servingRef) error {
	target := serviceResource(serving.project, serving.location, serving.id)
	refused := status.Error(codes.PermissionDenied, "Anonymous callers must be granted the run.invoker role")
	policy, err := p.engine.GetPolicy(target)
	if err != nil {
		return refused
	}
	for _, binding := range policy.Bindings {
		if binding.Role != "roles/run.invoker" {
			continue
		}
		for _, member := range binding.Members {
			if member == "allUsers" || member == "allAuthenticatedUsers" {
				return nil
			}
		}
	}
	return refused
}

// hostEntry maps one local service host to its scope and identifier.
type hostEntry struct {
	project  string
	location string
	id       string
}

// hostOf builds the local service host from the Cloud Run hostname shape.
func hostOf(scope state.Scope, id string) string {
	return id + "-" + scope.Project + "." + scope.Location + ".run.app"
}

// indexHost publishes the local host mapping after a service launch.
func (p *Provider) indexHost(scope state.Scope, id string) {
	p.hostMu.Lock()
	p.hosts[hostOf(scope, id)] = hostEntry{project: scope.Project, location: scope.Location, id: id}
	p.hostMu.Unlock()
}

// forgetHost removes the local host mapping when a service goes away.
func (p *Provider) forgetHost(scope state.Scope, id string) {
	p.hostMu.Lock()
	delete(p.hosts, hostOf(scope, id))
	p.hostMu.Unlock()
}

// audienceOf returns the verified invocation audience of one service.
func (p *Provider) audienceOf(serving servingRef) string {
	return "https://" + hostOf(state.Scope{Project: serving.project, Location: serving.location}, serving.id)
}

// revisionAddress reports whether the address dials a loopback host (a
// process or published port) or a private address (a container on the
// environment's Docker network). Runtimes produce every revision address.
func revisionAddress(address string) bool {
	host, _, err := net.SplitHostPort(address)
	if err != nil {
		return false
	}
	ip := net.ParseIP(host)
	return ip.IsLoopback() || ip.IsPrivate()
}

// touchActivity records one invocation against the revision's idle clock.
func (p *Provider) touchActivity(serving servingRef) {
	scope := state.Scope{Project: serving.project, Location: serving.location}
	_ = p.withService(scope, serving.id, func(_ *scopeState, record *serviceRecord) error {
		if revision := record.Revisions[serving.revision]; revision != nil {
			revision.lastActivity = p.clock()
		}
		return nil
	})
}

// coldStart restarts a reaped revision's container. The launch runs outside
// the store lock; concurrent invocations wait for the one in flight.
func (p *Provider) coldStart(serving servingRef) {
	scope := state.Scope{Project: serving.project, Location: serving.location}
	spec, done, wait := p.claimColdStart(scope, serving)
	if wait != nil {
		<-wait
	}
	if done == nil {
		return
	}
	defer close(done)
	started, err := p.launch(spec)
	if err != nil {
		p.endColdStart(scope, serving, done, nil)
		return
	}
	if !p.endColdStart(scope, serving, done, &started) {
		p.discard(started)
		return
	}
	// The reaper ended when it stopped the container; the restarted one needs
	// its own.
	p.startReaper(scope, serving.id)
}

// claimColdStart marks a reaped revision as starting and returns its launch
// inputs, or the channel of a cold start already in flight.
func (p *Provider) claimColdStart(scope state.Scope, serving servingRef) (launchSpec, chan struct{}, chan struct{}) {
	var spec launchSpec
	var done, wait chan struct{}
	_ = p.withService(scope, serving.id, func(_ *scopeState, record *serviceRecord) error {
		revision := record.Revisions[serving.revision]
		switch {
		case revision == nil || !revision.reaped:
		case revision.starting != nil:
			wait = revision.starting
		default:
			done = make(chan struct{})
			revision.starting = done
			spec = launchSpecOf(record, revision)
		}
		return nil
	})
	return spec, done, wait
}

// endColdStart clears the claim and attaches the started container, when
// there is one, if the revision is still there. It reports the attach.
func (p *Provider) endColdStart(scope state.Scope, serving servingRef, done chan struct{}, started *launched) bool {
	attached := false
	_ = p.withService(scope, serving.id, func(_ *scopeState, record *serviceRecord) error {
		if revision := record.Revisions[serving.revision]; revision != nil && revision.starting == done {
			revision.starting = nil
			if started != nil {
				p.attach(revision, *started)
				attached = true
			}
		}
		return nil
	})
	return attached
}

// invocationBodyLimit bounds the buffered invocation request body.
const invocationBodyLimit = 32 << 20
