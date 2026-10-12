package storage

import (
	"context"
	"encoding/json"
	"net/http"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// patchedObjectFields are the object fields a patch may set: the ones the Go
// client's ObjectHandle.Update sends for ordinary metadata
// (storage/docs/json_api/v1/objects/patch). Every other writable field (acl,
// customTime, holds, retention, contexts, storageClass) is a gap.
var patchedObjectFields = map[string]bool{
	keyContentType: true, keyCacheControl: true, keyDisposition: true,
	"contentEncoding": true, keyLanguage: true, keyMetadata: true,
}

// objectPatch is one decoded objects.patch body. strings holds the string
// fields to set; metadata is the custom metadata change, where a nil value
// removes that key, and clearMetadata removes every key first.
type objectPatch struct {
	strings       map[string]string
	metadata      map[string]*string
	setMetadata   bool
	clearMetadata bool
}

// decodeObjectPatch reads an objects.patch body and refuses each field and
// value the emulator does not apply.
func decodeObjectPatch(body []byte, target map[string]string) (objectPatch, error) {
	var fields map[string]json.RawMessage
	if json.Unmarshal(body, &fields) != nil || fields == nil {
		return objectPatch{}, provider.Invalid("Expected an object patch")
	}
	patch := &objectPatch{strings: map[string]string{}}
	for key, raw := range fields {
		if err := patch.decodeField(key, raw, target); err != nil {
			return objectPatch{}, err
		}
	}
	return *patch, nil
}

// decodeField records one patch field.
func (patch *objectPatch) decodeField(key string, raw json.RawMessage, target map[string]string) error {
	// The Go client sends the bucket in every patch body. The bucket and
	// name identify the target and must agree with the path.
	if identity, ok := target[key]; ok {
		var value string
		if json.Unmarshal(raw, &value) != nil || value != identity {
			return provider.Invalid("Object " + key + " does not match the patch target")
		}
		return nil
	}
	if !patchedObjectFields[key] {
		return gap("unsupported object patch field " + key)
	}
	if key == keyMetadata {
		// null clears every key, and a null value removes one key.
		if json.Unmarshal(raw, &patch.metadata) != nil {
			return provider.Invalid("metadata values must be strings or null")
		}
		patch.setMetadata, patch.clearMetadata = true, patch.metadata == nil
		return nil
	}
	var value *string
	if json.Unmarshal(raw, &value) != nil {
		return provider.Invalid(key + " must be a string")
	}
	// The backend keeps a field it is sent empty, so clearing one (null,
	// which the Go client sends for "") is not applied here.
	if value == nil || *value == "" {
		return gap("clearing object field " + key)
	}
	patch.strings[key] = *value
	return nil
}

// mergedMetadata applies the patch to an object's current custom metadata.
func (patch *objectPatch) mergedMetadata(current map[string]any) map[string]string {
	merged := map[string]string{}
	if existing, ok := current[keyMetadata].(map[string]any); ok && !patch.clearMetadata {
		for key, value := range existing {
			if text, ok := value.(string); ok {
				merged[key] = text
			}
		}
	}
	for key, value := range patch.metadata {
		if value == nil {
			delete(merged, key)
		} else {
			merged[key] = *value
		}
	}
	return merged
}

// patchObject updates one object's metadata in place. It runs after complete
// authorization: preconditions and the generation are checked against the
// object read under the backend gate, the backend writes, and only then does
// the metageneration advance.
func patchObject(ctx context.Context, b *memoryBackend, req request) (*gateway.RESTResponse, error) {
	patch, err := decodeObjectPatch(req.rest.JSON, map[string]string{keyBucket: req.bucket, keyName: req.object})
	if err != nil {
		return nil, err
	}
	current, refused, err := currentObject(ctx, b, req.bucket, req.object)
	if refused != nil || err != nil {
		return refused, err
	}
	if response, err := preconditions(req.rest, current, MethodObjectsPatch); response != nil || err != nil {
		return response, err
	}
	generation, _ := current[keyGeneration].(string)
	if wanted := req.rest.Query.Get(keyGeneration); wanted != "" && wanted != generation {
		return notFound(req.bucket, req.object), nil
	}
	if response, err := writeObjectPatch(ctx, b, req, patch, current); response != nil || err != nil {
		return response, err
	}
	b.advanceMetageneration(req.bucket, req.object, generation)
	return b.forward(ctx, http.MethodGet, objectPath(req.bucket, req.object), nil, nil, nil)
}

// writeObjectPatch sends a patch to the backend. Upstream's PATCH sets the
// string fields; its PUT replaces the custom metadata, which is how a key is
// removed. Manager.Use serializes the scope, so no request sees one write
// without the other. A non-200 backend answer is returned as is.
func writeObjectPatch(ctx context.Context, b *memoryBackend, req request, patch objectPatch, current map[string]any) (*gateway.RESTResponse, error) {
	type backendWrite struct {
		verb string
		body any
	}
	var writes []backendWrite
	if len(patch.strings) > 0 {
		writes = append(writes, backendWrite{http.MethodPatch, patch.strings})
	}
	if patch.setMetadata {
		writes = append(writes, backendWrite{http.MethodPut, map[string]any{keyMetadata: patch.mergedMetadata(current)}})
	}
	for _, write := range writes {
		payload, err := json.Marshal(write.body)
		if err != nil {
			return nil, provider.Invalid("Unable to encode the object patch")
		}
		response, err := b.forward(ctx, write.verb, objectPath(req.bucket, req.object), nil, http.Header{headerContentType: {"application/json"}}, payload)
		if err != nil || response.Status != http.StatusOK {
			return response, err
		}
	}
	return nil, nil //nolint:nilnil // a nil response with a nil error means every write succeeded
}
