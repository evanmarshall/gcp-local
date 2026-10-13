package bigtable

import (
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// createLatency is the least time an instance create stays running, so a
// client sees done=false and polls, as it does against GCP (recorded). GCP
// answers partial instance and table updates with operations already done.
const createLatency = 100 * time.Millisecond

// Operation verbs; each selects its metadata type.
const (
	verbCreateInstance = "createInstance"
	verbUpdateInstance = "updateInstance"
	verbUpdateTable    = "updateTable"
)

// operationTarget is an operation's resource and zone,
// "projects/P/instances/I[/tables/T]/locations/Z", which its name embeds.
func operationTarget(resource, zone string) string { return resource + "/locations/" + zone }

// operationName is Bigtable's operation name:
// operations/projects/P/instances/I[/tables/T]/locations/Z/operations/ID.
func operationName(_ state.Scope, target, id string) string {
	return "operations/" + target + "/operations/" + id
}

// operationMetadata renders the metadata GCP returns for each verb; only the
// @type is recorded. Create and update metadata carry request and finish
// times, table updates the table name and start and end times.
func operationMetadata(operation provider.Operation) map[string]any {
	resource, _, _ := strings.Cut(operation.Target, "/locations/")
	switch operation.Verb {
	case verbUpdateTable:
		metadata := map[string]any{"@type": typeUpdateTableMetadata, keyName: resource, "startTime": provider.Stamp(operation.Created)}
		if operation.Done {
			metadata["endTime"] = provider.Stamp(operation.Ended)
		}
		return metadata
	default:
		kind := typeCreateInstanceMetadata
		if operation.Verb == verbUpdateInstance {
			kind = typeUpdateInstanceMetadata
		}
		metadata := map[string]any{"@type": kind, "requestTime": provider.Stamp(operation.Created)}
		if operation.Done {
			metadata["finishTime"] = provider.Stamp(operation.Ended)
		}
		return metadata
	}
}

// operationWire renders an operation as GCP's REST API does: proto3 JSON
// omits done while it is false (recorded: a create's first response has only
// name and metadata).
func operationWire(body map[string]any) *gateway.RESTResponse {
	if body["done"] == false {
		delete(body, "done")
	}
	return &gateway.RESTResponse{JSON: body}
}

// started renders the operation Start just recorded, or forgets it when the
// work failed.
func (p *Provider) started(scope state.Scope, id string, response map[string]any, err error) (*gateway.RESTResponse, error) {
	if err != nil {
		_ = p.operations.Delete(scope, id)
		return nil, err
	}
	p.operations.Finish(scope, id, response, nil)
	body, err := p.operations.Get(scope, id)
	if err != nil {
		return nil, err
	}
	return operationWire(body), nil
}

// requestedOperation is the full name an operation route addresses.
func requestedOperation(request *gateway.RESTRequest) string {
	resource := provider.InstanceName(request.Path["project"], request.Path["instance"])
	if table := request.Path["table"]; table != "" {
		resource += "/tables/" + table
	}
	return operationName(state.Scope{}, operationTarget(resource, request.Path["location"]), request.Path["operation"])
}

// operationMethod serves the operations methods.
func (p *Provider) operationMethod(method string, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	if method == methodOperationsList {
		return p.operationsList(request, scope)
	}
	return p.operation(method, request, scope)
}

// operation serves get, cancel, and delete of one named operation; a name
// whose resource or zone differs from the stored operation's is not found.
func (p *Provider) operation(method string, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	name, id := requestedOperation(request), request.Path["operation"]
	body, err := p.operations.Get(scope, id)
	if err != nil || body[keyName] != name {
		return nil, status.Errorf(codes.NotFound, "Operation %s not found.", name)
	}
	switch method {
	case methodOperationCancel, methodTableOpCancel:
		if err := p.operations.Cancel(scope, id); err != nil {
			return nil, err
		}
		return emptyResponse(), nil
	case methodOperationDelete, methodTableOpDelete:
		if err := p.operations.Delete(scope, id); err != nil {
			return nil, err
		}
		return emptyResponse(), nil
	default:
		return operationWire(body), nil
	}
}

// operationsList lists the project's operations in creation order.
func (p *Provider) operationsList(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := p.operations.List(scope, request.Query, provider.Paging{Default: 100, Max: 1000}, gap)
	if err != nil {
		return nil, err
	}
	if items, ok := body["operations"].([]map[string]any); ok {
		for _, item := range items {
			operationWire(item)
		}
	}
	return &gateway.RESTResponse{JSON: body}, nil
}
