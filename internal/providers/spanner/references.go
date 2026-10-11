package spanner

import (
	"context"
	"fmt"
	"regexp"
	"strings"
	"time"

	"cloud.google.com/go/spanner/admin/database/apiv1/databasepb"
	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
)

// mutation is the index reference one create or delete may change. The
// emulator owns the resource; the provider mirrors only its name and parent so
// the IAM engine can resolve instance and database policy inheritance.
type mutation struct {
	kind, name, parent string
	create             bool
	children           []string // databases of an instance being deleted
	scope              state.Scope
}

// createStatement extracts the database id from CREATE DATABASE.
var createStatement = regexp.MustCompile("(?is)^\\s*CREATE\\s+DATABASE\\s+`?([a-z][a-z0-9_-]*)`?\\s*$")

// reconcileTimeout bounds the backend read that resolves an uncertain outcome.
const reconcileTimeout = 2 * time.Second

// mutationOf classifies a request; non-mutating requests return the zero value.
func mutationOf(ctx context.Context, b driver, message proto.Message, scope state.Scope) mutation {
	switch request := message.(type) {
	case *instancepb.CreateInstanceRequest:
		return mutation{kind: kindInstance, name: request.GetParent() + "/instances/" + request.GetInstanceId(),
			parent: projectResource(scope.Project), create: true, scope: scope}
	case *instancepb.DeleteInstanceRequest:
		return mutation{kind: kindInstance, name: request.GetName(), parent: projectResource(scope.Project),
			children: databasesOf(ctx, b, request.GetName()), scope: scope}
	case *databasepb.CreateDatabaseRequest:
		match := createStatement.FindStringSubmatch(request.GetCreateStatement())
		if match == nil {
			return mutation{} // the emulator rejects the statement; nothing to publish
		}
		return mutation{kind: kindDatabase, name: request.GetParent() + "/databases/" + match[1],
			parent: fullName(request.GetParent()), create: true, scope: scope}
	case *databasepb.DropDatabaseRequest:
		name := request.GetDatabase()
		return mutation{kind: kindDatabase, name: name, parent: fullName(name[:strings.LastIndex(name, "/databases/")]), scope: scope}
	}
	return mutation{}
}

// databasesOf lists an instance's databases before it is deleted; GCP
// deletes them with the instance, so their references must go too.
func databasesOf(ctx context.Context, b driver, instance string) []string {
	var names []string
	request := &databasepb.ListDatabasesRequest{Parent: instance}
	for {
		response := &databasepb.ListDatabasesResponse{}
		if b.client().Invoke(ctx, databaseAdmin+"ListDatabases", request, response) != nil {
			return names
		}
		for _, database := range response.GetDatabases() {
			names = append(names, database.GetName())
		}
		if response.GetNextPageToken() == "" {
			return names
		}
		request.PageToken = response.GetNextPageToken()
	}
}

// ambiguous reports a failure whose effect on the backend is unknown.
func ambiguous(err error) bool {
	switch status.Code(err) {
	case codes.Unknown, codes.DeadlineExceeded, codes.Canceled, codes.Unavailable, codes.Internal, codes.Aborted:
		return true
	default:
		return false
	}
}

// settle publishes index changes only after the backend confirms them. An
// uncertain outcome is resolved by reading the backend; an unresolvable one
// blocks the scope until reset.
func (p *Provider) settle(ctx context.Context, b driver, m mutation, callErr error) error {
	if m.kind == "" || (callErr != nil && !ambiguous(callErr)) {
		return callErr
	}
	present := m.create
	if callErr != nil {
		var err error
		if present, err = exists(ctx, b, m); err != nil {
			p.poisoned.Store(m.scope, true)
			return status.Errorf(codes.Unavailable, "Spanner %s outcome is unresolved: %v", m.kind, callErr)
		}
	}
	if err := p.publish(m, present); err != nil {
		p.poisoned.Store(m.scope, true)
		return status.Errorf(codes.Internal, "publish the Spanner %s reference: %v", m.kind, err)
	}
	return callErr
}

// exists reads the resource from the backend.
func exists(ctx context.Context, b driver, m mutation) (bool, error) {
	ctx, cancel := context.WithTimeout(context.WithoutCancel(ctx), reconcileTimeout)
	defer cancel()
	var err error
	if m.kind == kindInstance {
		err = b.client().Invoke(ctx, instanceAdmin+"GetInstance", &instancepb.GetInstanceRequest{Name: m.name}, &instancepb.Instance{})
	} else {
		err = b.client().Invoke(ctx, databaseAdmin+"GetDatabase", &databasepb.GetDatabaseRequest{Name: m.name}, &databasepb.Database{})
	}
	switch status.Code(err) {
	case codes.OK:
		return true, nil
	case codes.NotFound:
		return false, nil
	default:
		return false, fmt.Errorf("read the Spanner %s: %w", m.kind, err)
	}
}

// publish adds or removes the confirmed references and forgets the policies
// of deleted resources.
func (p *Provider) publish(m mutation, present bool) error {
	if present {
		if m.kind == kindDatabase && m.create {
			now := p.clock()
			_ = p.created.With(m.scope, func(times *createdDatabases) error { (*times)[m.name] = now; return nil })
		}
		return p.index.Put(serviceName, state.Resource{ //nolint:wrapcheck // wrapped by settle
			Name: fullName(m.name), Service: serviceName, Kind: m.kind, Parent: m.parent, Scope: m.scope,
		})
	}
	for _, name := range append(m.children, m.name) {
		if err := p.index.Remove(serviceName, m.scope, fullName(name)); err != nil {
			return err //nolint:wrapcheck // wrapped by settle
		}
		p.engine.Forget(fullName(name))
		_ = p.created.With(m.scope, func(times *createdDatabases) error { delete(*times, name); return nil })
	}
	return nil
}
