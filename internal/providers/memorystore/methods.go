package memorystore

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"sort"
	"strconv"

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
	if record.AuthEnabled {
		if record.AuthString, err = newAuthString(); err != nil {
			return nil, err
		}
	}
	var result operation
	err = p.use(ctx, scope, func(ctx context.Context, current driver) error {
		if _, found := p.lookup(scope, record.ID); found {
			return status.Errorf(codes.AlreadyExists, "Resource '%s' already exists", instanceName(scope, record.ID))
		}
		port, err := current.launch(ctx, record.ID, launchSpec{AuthString: record.AuthString, MemoryGB: record.MemorySizeGB})
		if errors.Is(err, dataplane.ErrExhausted) {
			return status.Error(codes.ResourceExhausted, "gcp-local: every published data-plane port is in use; delete an instance or widen the range")
		}
		if err != nil {
			return fmt.Errorf("launch the Redis server: %w", err)
		}
		record.Port, record.Created = port, p.clock().UTC()
		result, err = p.publish(scope, record)
		if err != nil {
			_ = current.remove(context.WithoutCancel(ctx), record.ID)
			return err
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ok(result.wire(scope)), nil
}

// publish stores a new instance and its create operation, then its index reference.
func (p *Provider) publish(scope state.Scope, record instance) (operation, error) {
	name := instanceName(scope, record.ID)
	result := p.newOperation(name, verbCreate, record.wire(scope))
	if err := p.catalog.With(scope, func(value *catalog) error {
		value.instances[record.ID] = record.clone()
		value.remember(result)
		return nil
	}); err != nil {
		return operation{}, fmt.Errorf("store the instance: %w", err)
	}
	if err := p.index.Put(serviceName, state.Resource{Name: fullName(name), Service: serviceName, Kind: "instance",
		Parent: provider.ProjectResource(scope.Project), Scope: scope}); err != nil {
		_ = p.catalog.With(scope, func(value *catalog) error {
			delete(value.instances, record.ID)
			value.operations = value.operations[:len(value.operations)-1]
			return nil
		})
		return operation{}, fmt.Errorf("index the instance: %w", err)
	}
	return result, nil
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
	start, end, next, err := page(request.Query, len(records))
	if err != nil {
		return nil, err
	}
	items := make([]map[string]any, 0, end-start)
	for _, record := range records[start:end] {
		items = append(items, record.wire(scope))
	}
	return ok(listBody("instances", items, next)), nil
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
	var result operation
	err = p.use(ctx, scope, func(ctx context.Context, current driver) error {
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
		result = p.newOperation(instanceName(scope, id), verbUpdate, updated.wire(scope))
		return p.catalog.With(scope, func(value *catalog) error {
			value.instances[id] = updated.clone()
			value.remember(result)
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return ok(result.wire(scope)), nil
}

// remove stops the instance's server, then drops its metadata and reference.
func (p *Provider) remove(ctx context.Context, id string, scope state.Scope) (*gateway.RESTResponse, error) {
	var result operation
	err := p.use(ctx, scope, func(ctx context.Context, current driver) error {
		if _, found := p.lookup(scope, id); !found {
			return notFound(instanceName(scope, id))
		}
		if err := current.remove(ctx, id); err != nil {
			return fmt.Errorf("stop the Redis server: %w", err)
		}
		name := instanceName(scope, id)
		result = p.newOperation(name, verbDelete, nil)
		if err := p.catalog.With(scope, func(value *catalog) error {
			delete(value.instances, id)
			value.remember(result)
			return nil
		}); err != nil {
			return fmt.Errorf("remove the instance: %w", err)
		}
		if err := p.index.Remove(serviceName, scope, fullName(name)); err != nil {
			return fmt.Errorf("unindex the instance: %w", err)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	return ok(result.wire(scope)), nil
}

func (p *Provider) operation(ctx context.Context, id string, scope state.Scope) (*gateway.RESTResponse, error) {
	var body map[string]any
	err := p.use(ctx, scope, func(context.Context, driver) error {
		return p.catalog.With(scope, func(value *catalog) error {
			for _, record := range value.operations {
				if record.ID == id {
					body = record.wire(scope)
					return nil
				}
			}
			return notFound(provider.OperationName(scope, id))
		})
	})
	if err != nil {
		return nil, err
	}
	return ok(body), nil
}

func (p *Provider) operations(ctx context.Context, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if request.Query.Get("filter") != "" {
		return nil, gap("operations.list filter")
	}
	var items []map[string]any
	var next string
	err := p.use(ctx, scope, func(context.Context, driver) error {
		return p.catalog.With(scope, func(value *catalog) error {
			start, end, token, err := page(request.Query, len(value.operations))
			if err != nil {
				return err
			}
			for _, record := range value.operations[start:end] {
				items = append(items, record.wire(scope))
			}
			next = token
			return nil
		})
	})
	if err != nil {
		return nil, err
	}
	return ok(listBody("operations", items, next)), nil
}

// newOperation records a completed operation at the provider clock.
func (p *Provider) newOperation(target, verb string, response map[string]any) operation {
	now := p.clock().UTC()
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	id := "operation-" + strconv.FormatInt(now.UnixMilli(), 10) + "-" + hex.EncodeToString(raw)
	return operation{ID: id, Target: target, Verb: verb, Created: now, Ended: now, Response: response}
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
