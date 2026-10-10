package storage

import (
	"errors"

	"github.com/evanmarshall/gcp-local/internal/backend"
)

// errBackendType reports a backend that this provider did not register.
var errBackendType = errors.New("backend is not the storage emulator")

// errPayloadShape reports a backend response that is not a JSON object, which
// means the pinned backend replied in a shape this provider cannot read.
var errPayloadShape = errors.New("backend response is not a JSON object")

// backendOf returns the in-process emulator behind a manager-owned backend. The
// assertion is checked so a foreign backend is an error, not a panic.
func backendOf(driver backend.Backend) (*memoryBackend, error) {
	value, ok := driver.(*memoryBackend)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}

// jsonObject narrows a decoded backend payload to a JSON object.
func jsonObject(value any) (map[string]any, error) {
	object, ok := value.(map[string]any)
	if !ok {
		return nil, errPayloadShape
	}
	return object, nil
}

// itemList narrows a decoded JSON array of objects, skipping foreign entries.
func itemList(value any) []map[string]any {
	raw, ok := value.([]any)
	if !ok {
		return nil
	}
	items := make([]map[string]any, 0, len(raw))
	for _, entry := range raw {
		if object, ok := entry.(map[string]any); ok {
			items = append(items, object)
		}
	}
	return items
}
