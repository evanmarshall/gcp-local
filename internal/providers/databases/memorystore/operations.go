package memorystore

import (
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// catalog is the provider-owned instance state of one project/region scope.
// Operations live in the provider's shared operation store.
type catalog struct {
	instances map[string]instance
}

func newCatalog() catalog { return catalog{instances: map[string]instance{}} }

// operationLatency is the minimum time from a request to its operation
// reporting done. Local mutations finish before the response; the latency
// makes clients poll the operation, as they do against GCP.
const operationLatency = 500 * time.Millisecond

// operationMetadata renders the Redis v1 OperationMetadata; endTime appears
// once the operation is done.
func operationMetadata(op provider.Operation) map[string]any {
	metadata := map[string]any{
		"@type":         operationType,
		fieldCreateTime: op.Created.Format(time.RFC3339Nano),
		"target":        op.Target,
		"verb":          op.Verb,
		"apiVersion":    "v1",
	}
	if op.Done {
		metadata["endTime"] = op.Ended.Format(time.RFC3339Nano)
	}
	if op.CancelRequested {
		metadata["cancelRequested"] = true
	}
	return metadata
}

// instanceResponse types an instance as an operation response.
func instanceResponse(wire map[string]any) map[string]any {
	wire["@type"] = instanceType
	return wire
}

// empty answers a method that returns google.protobuf.Empty.
func empty(err error) (*gateway.RESTResponse, error) {
	if err != nil {
		return nil, err
	}
	return ok(map[string]any{}), nil
}
