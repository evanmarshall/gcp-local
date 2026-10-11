package pubsub

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type pushSubscription struct {
	scope  state.Scope
	config *pb.PushConfig
	cancel context.CancelFunc
	done   chan struct{}
}

func validatePush(config *pb.PushConfig) error {
	if err := fields(config, "push_endpoint", "attributes", "oidc_token"); err != nil {
		return err
	}
	if config.GetOidcToken() == nil {
		return gap("push requires an explicit OIDC service account")
	}
	if err := fields(config.GetOidcToken(), "service_account_email", "audience"); err != nil {
		return err
	}
	if err := validatePushEndpoint(config.GetPushEndpoint()); err != nil {
		return err
	}
	token := config.GetOidcToken()
	if token.GetServiceAccountEmail() == "" || len(token.GetAudience()) > 2048 || strings.ContainsAny(token.GetAudience(), "\r\n") {
		return invalid("Invalid push account or audience")
	}
	return nil
}

// validatePushEndpoint requires a numeric loopback HTTP URL with an explicit
// port: the worker must not dial arbitrary hosts or leak the loopback worker.
func validatePushEndpoint(raw string) error {
	endpoint, err := url.Parse(raw)
	if err != nil {
		return invalid("Invalid push endpoint")
	}
	ip := net.ParseIP(endpoint.Hostname())
	if endpoint.Scheme != "http" || ip == nil || !ip.IsLoopback() || endpoint.Port() == "" || endpoint.User != nil || endpoint.Fragment != "" {
		return gap("push endpoints currently require numeric loopback HTTP and an explicit port")
	}
	return nil
}

func (p *Provider) authorizePush(call gateway.Call, config *pb.PushConfig) error {
	if p.tokens == nil {
		return gap("local ID-token issuer is not configured")
	}
	account := iam.AccountResource(call.Project, config.GetOidcToken().GetServiceAccountEmail())
	resource, found := p.index.Get(account)
	if !found || resource.Service != "iam" || resource.Kind != "serviceAccount" || resource.Scope != scopeOf(call) {
		return gap("configure a same-project push service account")
	}
	project, _ := p.index.Get(projectResource(call.Project))
	if !regexp.MustCompile(`^[1-9][0-9]{0,19}$`).MatchString(project.Number) {
		return gap("push requires the numeric resource project number")
	}
	call.Method, call.Resource = "pubsub.push.actAs", account
	_, err := p.iam.Authorize(call, iam.Target{Name: account}, nil)
	if err != nil {
		return fmt.Errorf("authorize push identity: %w", err)
	}
	return nil
}

func (p *Provider) pushCount(scope state.Scope) int {
	count := 0
	p.pushes.Range(func(_, value any) bool {
		if worker, ok := pushWorker(value); ok && worker.scope == scope {
			count++
		}
		return true
	})
	return count
}
func (p *Provider) startPush(parent context.Context, call gateway.Call, config *pb.PushConfig) error {
	cloned, err := cloneMessage(config)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(context.WithoutCancel(parent))
	worker := &pushSubscription{scope: scopeOf(call), config: cloned, cancel: cancel, done: make(chan struct{})}
	p.pushes.Store(canonical(call.Resource), worker)
	go func() {
		defer close(worker.done)
		defer cancel()
		// The subscription was authorized before creation. The worker owns its
		// queue; it mints under the service agent, never the creator or an admin.
		// A session failure ends this worker, so the error is not actionable here.
		_ = p.manager.Session(ctx, keyOf(call), func(ctx context.Context, active backend.Backend) error {
			b, err := driverOf(active)
			if err != nil {
				return err
			}
			p.deliver(ctx, b, call, worker.config)
			return ctx.Err()
		})
	}()
	return nil
}
func (p *Provider) stopPush(name string) error {
	value, ok := p.pushes.Load(name)
	if !ok {
		return nil
	}
	worker, ok := pushWorker(value)
	if !ok {
		p.pushes.Delete(name)
		return nil
	}
	worker.cancel()
	select {
	case <-worker.done:
		p.pushes.Delete(name)
		return nil
	case <-time.After(3 * time.Second):
		return status.Error(codes.FailedPrecondition, "Push worker did not stop; reset this scope")
	}
}
func (p *Provider) resetPush(scope state.Scope) error {
	var result error
	p.pushes.Range(func(key, value any) bool {
		if worker, ok := pushWorker(value); ok && worker.scope == scope {
			name, ok := key.(string)
			if !ok {
				result = errMessageType
				return false
			}
			result = p.stopPush(name)
		}
		return result == nil
	})
	return result
}
func (p *Provider) restorePush(response proto.Message) {
	decorate := func(subscription *pb.Subscription) {
		if subscription == nil {
			return
		}
		if value, ok := p.pushes.Load(canonical(subscription.GetName())); ok {
			if worker, ok := pushWorker(value); ok {
				subscription.PushConfig, _ = cloneMessage(worker.config)
			}
		}
	}
	switch response := response.(type) {
	case *pb.Subscription:
		decorate(response)
	case *pb.ListSubscriptionsResponse:
		for _, subscription := range response.GetSubscriptions() {
			decorate(subscription)
		}
	}
}

func (p *Provider) pushToken(ctx context.Context, call gateway.Call, config *pb.PushConfig) (string, error) {
	if _, failed := p.poisoned.Load(scopeOf(call)); failed {
		return "", status.Error(codes.FailedPrecondition, "Pub/Sub scope needs reset")
	}
	project, ok := p.index.Get(projectResource(call.Project))
	if !ok {
		return "", gap("push project disappeared")
	}
	call.Context = ctx
	call.Principal = gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:service-" + project.Number + "@gcp-sa-pubsub.iam.gserviceaccount.com", Project: call.Project, Scopes: []string{identity.CloudPlatform}}
	audience := config.GetOidcToken().GetAudience()
	if audience == "" {
		audience = config.GetPushEndpoint()
	}
	token, err := p.tokens.Mint(call, iam.AccountResource(call.Project, config.GetOidcToken().GetServiceAccountEmail()), audience)
	if err != nil {
		return "", fmt.Errorf("mint push identity token: %w", err)
	}
	return token, nil
}

func (p *Provider) deliver(ctx context.Context, b driver, call gateway.Call, config *pb.PushConfig) {
	client := pushClient(config.GetPushEndpoint())
	for ctx.Err() == nil {
		messages, retry := p.pullForDelivery(ctx, b, call, config)
		if retry && !pauseFor(ctx, time.Second) {
			return
		}
		for _, message := range messages {
			if ctx.Err() != nil {
				return
			}
			accepted := p.pushMessage(ctx, call, config, client, message.GetMessage())
			if !p.settleDelivery(ctx, b, call, message.GetAckId(), accepted) {
				return
			}
		}
	}
}

// pushClient builds a client that dials only the configured endpoint and never
// follows redirects, so a push target cannot pivot the worker elsewhere.
func pushClient(endpoint string) *http.Client {
	parsed, _ := url.Parse(endpoint)
	transport := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, _, address string) (net.Conn, error) {
		if address != parsed.Host {
			return nil, gap("push dial target changed")
		}
		return (&net.Dialer{Timeout: time.Second}).DialContext(ctx, "tcp", parsed.Host)
	}}
	return &http.Client{Transport: transport, Timeout: 3 * time.Second, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
}

// pauseFor waits one interval or until the context ends, and reports whether the
// loop may continue.
func pauseFor(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return true
	case <-ctx.Done():
		return false
	}
}

// pullForDelivery draws one message batch, or reports whether the failure is
// worth retrying after a pause. An empty subscription just idles the worker.
func (p *Provider) pullForDelivery(ctx context.Context, b driver, call gateway.Call, config *pb.PushConfig) ([]*pb.ReceivedMessage, bool) {
	if _, err := p.pushToken(ctx, call, config); err != nil {
		return nil, true
	}
	pullCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
	result, err := invoke(pullCtx, b, subscriber+"Pull", &pb.PullRequest{Subscription: call.Resource, MaxMessages: 1})
	cancel()
	if err != nil {
		if status.Code(err) == codes.NotFound {
			return nil, false
		}
		return nil, true
	}
	pulled, ok := result.(*pb.PullResponse)
	if !ok {
		return nil, true
	}
	return pulled.GetReceivedMessages(), true
}

// pushBody encodes the Pub/Sub push envelope or reports why it cannot.
func pushBody(subscription string, message *pb.PubsubMessage) ([]byte, bool) {
	encoded, err := protojson.Marshal(message)
	if err != nil {
		return nil, false
	}
	body, err := json.Marshal(map[string]any{"message": json.RawMessage(encoded), "subscription": subscription})
	if err != nil {
		return nil, false
	}
	return body, true
}

// pushMessage posts one message to the endpoint and reports whether the
// endpoint accepted it. Failures are retried by nacking, not logged.
func (p *Provider) pushMessage(ctx context.Context, call gateway.Call, config *pb.PushConfig, client *http.Client, message *pb.PubsubMessage) bool {
	token, err := p.pushToken(ctx, call, config)
	if err != nil {
		return false
	}
	body, ok := pushBody(call.Resource, message)
	if !ok {
		return false
	}
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, config.GetPushEndpoint(), bytes.NewReader(body))
	if err != nil {
		return false
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Authorization", "Bearer "+token)
	response, err := client.Do(request)
	if err != nil {
		return false
	}
	_, _ = io.Copy(io.Discard, io.LimitReader(response.Body, 64<<10))
	_ = response.Body.Close()

	switch response.StatusCode {
	case http.StatusOK, http.StatusCreated, http.StatusAccepted, http.StatusNoContent:
		return true
	default:
		return false
	}
}

// settleDelivery acknowledges an accepted delivery, or resets the ack deadline
// so the message is redelivered. A failed settle without a pause ends the worker.
func (p *Provider) settleDelivery(ctx context.Context, b driver, call gateway.Call, ackID string, accepted bool) bool {
	ackCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	defer cancel()
	if accepted {
		_, err := invoke(ackCtx, b, subscriber+"Acknowledge", &pb.AcknowledgeRequest{Subscription: call.Resource, AckIds: []string{ackID}})
		if err == nil {
			return true
		}
	} else {
		_, err := invoke(ackCtx, b, subscriber+"ModifyAckDeadline", &pb.ModifyAckDeadlineRequest{Subscription: call.Resource, AckIds: []string{ackID}, AckDeadlineSeconds: 0})
		if err == nil {
			return true
		}
	}
	return pauseFor(ctx, time.Second)
}
