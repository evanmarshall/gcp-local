package spanner

import (
	"crypto/rand"
	"encoding/hex"
	"maps"
)

// Long-running-operation type URLs observed in the Spanner recording.
const (
	typeCreateInstanceMetadata = "type.googleapis.com/google.spanner.admin.instance.v1.CreateInstanceMetadata"
	typeInstance               = "type.googleapis.com/google.spanner.admin.instance.v1.Instance"
	typeCreateDatabaseMetadata = "type.googleapis.com/google.spanner.admin.database.v1.CreateDatabaseMetadata"
	typeDatabase               = "type.googleapis.com/google.spanner.admin.database.v1.Database"
	typeUpdateDdlMetadata      = "type.googleapis.com/google.spanner.admin.database.v1.UpdateDatabaseDdlMetadata"
	typeEmpty                  = "type.googleapis.com/google.protobuf.Empty"
	keyType                    = "@type"
)

// operationID mints one opaque operation id. Local operations complete
// synchronously, so they are returned done and are not stored for polling.
func operationID() string {
	var raw [8]byte
	_, _ = rand.Read(raw[:])
	return hex.EncodeToString(raw[:])
}

// doneOperation renders one completed LRO under parent with typed metadata
// and response payloads.
func doneOperation(parent, metadataType string, metadata map[string]any, responseType string, response map[string]any) map[string]any {
	typedMetadata := map[string]any{keyType: metadataType}
	maps.Copy(typedMetadata, metadata)
	typedResponse := map[string]any{keyType: responseType}
	maps.Copy(typedResponse, response)
	return map[string]any{
		keyName:    parent + "/operations/" + operationID(),
		"metadata": typedMetadata,
		"done":     true,
		"response": typedResponse,
	}
}
