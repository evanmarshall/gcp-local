package memorystore

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"slices"
	"sort"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/dataplane"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// create validates the request, launches the instance's server, then
// publishes metadata and the index reference. A failed publication stops the
// new server, so no running backend outlives a rejected create.
func (p *Provider) create(ctx context.Context, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeInstance(request.JSON)
	if err != nil {
		return nil, err
	}
	record, err := newInstance(scope, request.Query.Get("instanceId"), body)
	if err != nil {
		return nil, err
	}
	if body.AuthorizedNetwork != "" {
		if err := p.networkExists(record.Network); err != nil {
			return nil, err
		}
	}
	if record.AuthEnabled {
		if record.AuthString, err = newAuthString(); err != nil {
			return nil, err
		}
	}
	return p.operate(scope, instanceName(scope, record.ID), verbCreate, func() (map[string]any, error) {
		var wire map[string]any
		err := p.use(ctx, scope, func(ctx context.Context, current driver) error {
			var err error
			wire, err = p.launch(ctx, scope, current, record)
			return err
		})
		return wire, err
	})
}

// launch starts a new instance's server and publishes it, returning the
// instance as an operation response; call it inside use.
func (p *Provider) launch(ctx context.Context, scope state.Scope, current driver, record instance) (map[string]any, error) {
	if _, found := p.lookup(scope, record.ID); found {
		return nil, status.Errorf(codes.AlreadyExists, "Resource '%s' already exists", instanceName(scope, record.ID))
	}
	port, err := current.launch(ctx, record.ID, launchSpec{AuthString: record.AuthString, MemoryGB: record.MemorySizeGB})
	if errors.Is(err, dataplane.ErrExhausted) {
		return nil, status.Error(codes.ResourceExhausted, "gcp-local: every published data-plane port is in use; delete an instance or widen the range")
	}
	if err != nil {
		return nil, fmt.Errorf("launch the Redis server: %w", err)
	}
	record.Port, record.Created = port, p.clock().UTC()
	if err := p.publish(scope, record); err != nil {
		_ = current.remove(context.WithoutCancel(ctx), record.ID)
		return nil, err
	}
	return instanceResponse(record.wire(scope)), nil
}

// networkExists refuses an authorized network the Compute provider doesn't
// hold, with GCP's error (recorded, memorystore-redis-instance-admin). An
// omitted network is the project's default network, which isn't checked.
// Without the Compute service nothing can be checked, so a named network is
// a gap rather than accepted unverified.
func (p *Provider) networkExists(network string) error {
	if !slices.Contains(p.registry.Providers(), "compute") {
		return gap("authorizedNetwork needs the compute service to check the network; add compute to -services")
	}
	// The Compute provider indexes a network as
	// //compute.googleapis.com/projects/P/global/network/N.
	if _, found := p.index.Get("//compute.googleapis.com/" + strings.Replace(network, "/global/networks/", "/global/network/", 1)); found {
		return nil
	}
	return provider.BadRequest("com.google.apps.framework.request.StatusException: <eye3 title='INVALID_ARGUMENT'/> generic::INVALID_ARGUMENT: "+ //nolint:wrapcheck // GCP's recorded refusal
		"Invalid authorized network '"+provider.LastSegment(network)+"'.", "instance.authorized_network", "Invalid value: "+network)
}

// paging is the list methods' page sizes and recorded refusals
// (memorystore-redis-instance-admin records instances.list).
var paging = provider.Paging{Default: maxPageSize, Max: maxPageSize,
	InvalidToken: provider.BadRequest("The request was invalid: invalid page token", "pageToken", "invalid page token"),
	NegativeSize: provider.Invalid("Invalid argument: 'page_size'")}

// operate runs a mutation as a long-running operation: the operation starts
// before the work and finishes with its response. A refused mutation leaves
// no operation and returns its error.
func (p *Provider) operate(scope state.Scope, target, verb string, work func() (map[string]any, error)) (*gateway.RESTResponse, error) {
	id, err := p.operations.Start(scope, provider.Launch{Target: target, Verb: verb})
	if err != nil {
		return nil, err
	}
	response, err := work()
	if err != nil {
		_ = p.operations.Delete(scope, id)
		return nil, err
	}
	p.operations.Finish(scope, id, response, nil)
	return p.operation(scope, id)
}

// publish stores a new instance and its index reference under the scope
// lock authorization holds, so a racing request never sees one without the
// other.
func (p *Provider) publish(scope state.Scope, record instance) error {
	name := instanceName(scope, record.ID)
	return p.catalog.With(scope, func(value *catalog) error { //nolint:wrapcheck // the store returns scope validation or the wrapped index error
		if err := p.index.Put(serviceName, state.Resource{Name: fullName(name), Service: serviceName, Kind: "instance",
			Parent: provider.ProjectResource(scope.Project), Scope: scope}); err != nil {
			return fmt.Errorf("index the instance: %w", err)
		}
		value.instances[record.ID] = record.clone()
		return nil
	})
}

// lookup copies one instance record.
func (p *Provider) lookup(scope state.Scope, id string) (instance, bool) {
	var record instance
	var found bool
	_ = p.catalog.With(scope, func(value *catalog) error {
		var current instance
		current, found = value.instances[id]
		record = current.clone()
		return nil
	})
	return record, found
}

// read returns one instance after confirming the scope's servers are ready.
func (p *Provider) read(ctx context.Context, id string, scope state.Scope) (instance, error) {
	var record instance
	err := p.use(ctx, scope, func(context.Context, driver) error {
		var found bool
		if record, found = p.lookup(scope, id); !found {
			return notFound(instanceName(scope, id))
		}
		return nil
	})
	return record, err
}

func (p *Provider) get(ctx context.Context, id string, scope state.Scope) (*gateway.RESTResponse, error) {
	record, err := p.read(ctx, id, scope)
	if err != nil {
		return nil, err
	}
	return ok(record.wire(scope)), nil
}

// authString serves the AUTH string; an instance without AUTH returns an empty message.
func (p *Provider) authString(ctx context.Context, id string, scope state.Scope) (*gateway.RESTResponse, error) {
	record, err := p.read(ctx, id, scope)
	if err != nil {
		return nil, err
	}
	if !record.AuthEnabled {
		return ok(map[string]any{}), nil
	}
	return ok(map[string]any{"authString": record.AuthString}), nil
}

func (p *Provider) list(ctx context.Context, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var records []instance
	err := p.use(ctx, scope, func(context.Context, driver) error {
		return p.catalog.With(scope, func(value *catalog) error {
			for _, record := range value.instances {
				records = append(records, record.clone())
			}
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	page, next, err := provider.Page(records, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, len(page))
	for _, record := range page {
		items = append(items, record.wire(scope))
	}
	return ok(provider.ListBody("instances", items, next)), nil
}

// patch applies the masked fields; a memory change resizes the running server.
func (p *Provider) patch(ctx context.Context, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	paths, err := parseMask(request.Query.Get("updateMask"))
	if err != nil {
		return nil, err
	}
	body, err := decodeInstance(request.JSON)
	if err != nil {
		return nil, err
	}
	id := request.Path[pathInstance]
	return p.operate(scope, instanceName(scope, id), verbUpdate, func() (map[string]any, error) {
		var wire map[string]any
		err := p.use(ctx, scope, func(ctx context.Context, current driver) error {
			record, found := p.lookup(scope, id)
			if !found {
				return notFound(instanceName(scope, id))
			}
			updated, err := applyPatch(record, body, paths)
			if err != nil {
				return err
			}
			if updated.MemorySizeGB != record.MemorySizeGB {
				if err := current.resize(ctx, id, updated.MemorySizeGB); err != nil {
					return fmt.Errorf("resize the Redis server: %w", err)
				}
			}
			wire = instanceResponse(updated.wire(scope))
			return p.catalog.With(scope, func(value *catalog) error {
				value.instances[id] = updated.clone()
				return nil
			})
		})
		return wire, err
	})
}

// remove stops the instance's server, then drops its metadata and reference.
func (p *Provider) remove(ctx context.Context, id string, scope state.Scope) (*gateway.RESTResponse, error) {
	name := instanceName(scope, id)
	return p.operate(scope, name, verbDelete, func() (map[string]any, error) {
		err := p.use(ctx, scope, func(ctx context.Context, current driver) error {
			if _, found := p.lookup(scope, id); !found {
				return notFound(name)
			}
			if err := current.remove(ctx, id); err != nil {
				return fmt.Errorf("stop the Redis server: %w", err)
			}
			return p.catalog.With(scope, func(value *catalog) error {
				delete(value.instances, id)
				if err := p.index.Remove(serviceName, scope, fullName(name)); err != nil {
					return fmt.Errorf("unindex the instance: %w", err)
				}
				return nil
			})
		})
		return map[string]any{"@type": emptyType}, err
	})
}

// operation renders one operation of the scope.
func (p *Provider) operation(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	body, err := p.operations.Get(scope, id)
	if err != nil {
		return nil, err
	}
	return ok(body), nil
}

// cancel refuses a finished operation as GCP does (recorded,
// memorystore-redis-instance-admin) and otherwise requests cancellation.
func (p *Provider) cancel(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	current, err := p.operations.Get(scope, id)
	if err != nil {
		return nil, err
	}
	if current["done"] == true {
		return nil, provider.PreconditionFailure("Invalid resource state '" + provider.OperationName(scope, id) + "': operation already completed")
	}
	return empty(p.operations.Cancel(scope, id))
}

// listOperations renders one page of the scope's operations in creation order.
func (p *Provider) listOperations(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := p.operations.List(scope, request.Query, paging, gap)
	if err != nil {
		return nil, err
	}
	return ok(body), nil
}

// newAuthString returns a fresh UUID-shaped AUTH string.
func newAuthString() (string, error) {
	raw := make([]byte, 16)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate the AUTH string: %w", err)
	}
	raw[6] = raw[6]&0x0f | 0x40
	raw[8] = raw[8]&0x3f | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", raw[0:4], raw[4:6], raw[6:8], raw[8:10], raw[10:16]), nil
}
