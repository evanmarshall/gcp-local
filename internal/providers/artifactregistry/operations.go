package artifactregistry

import (
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// operationPrefix starts every Artifact Registry operation ID. GCP's IDs are
// UUIDs; the prefix routes the shared operations path (see New).
const operationPrefix = "artifactregistry-"

// pathOperation is the operations route variable: the ID after the prefix.
const pathOperation = "operation"

// Operation and response type URLs of the Artifact Registry LRO envelope.
const (
	repositoryType = "type.googleapis.com/google.devtools.artifactregistry.v1.Repository"
	emptyType      = "type.googleapis.com/google.protobuf.Empty"
	metadataType   = "type.googleapis.com/google.devtools.artifactregistry.v1.OperationMetadata"
)

// operationMetadata renders the OperationMetadata, which carries only its
// type (recorded).
func operationMetadata(provider.Operation) map[string]any {
	return map[string]any{"@type": metadataType}
}

// operationDenied is GCP's 403 for an operation that is missing or that the
// caller can't read; the recording shows it for a missing one.
func operationDenied(scope state.Scope, id string) error {
	return status.Errorf(codes.PermissionDenied, "Permission denied on operation %s (or it may not exist)",
		provider.OperationName(scope, id))
}

// operation renders one stored operation. GCP omits done while it is false
// (recorded), as proto3 JSON omits a default.
func (p *Provider) operation(scope state.Scope, id string) (*gateway.RESTResponse, error) {
	body, err := p.operations.Get(scope, id)
	if status.Code(err) == codes.NotFound {
		return nil, operationDenied(scope, id)
	}
	if err != nil {
		return nil, err
	}
	if body["done"] == false {
		delete(body, "done")
	}
	return &gateway.RESTResponse{JSON: body}, nil
}

// operate runs a mutation as an operation and renders it; a refused
// mutation leaves no operation and returns its error.
func (p *Provider) operate(scope state.Scope, target, verb string, work func() (map[string]any, error)) (*gateway.RESTResponse, error) {
	id, err := p.operations.Run(scope, target, verb, work)
	if err != nil {
		return nil, err //nolint:wrapcheck // the work's wire statuses
	}
	return p.operation(scope, id)
}
