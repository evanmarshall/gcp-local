package memorystore

import (
	"maps"
	"net/url"
	"strconv"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
)

// catalog is the provider-owned state of one project/region scope.
type catalog struct {
	instances  map[string]instance
	operations []operation
}

func newCatalog() catalog { return catalog{instances: map[string]instance{}} }

// operation is one completed google.longrunning.Operation. Local mutations
// finish before the response, so every recorded operation is done.
type operation struct {
	ID, Target, Verb string
	Created, Ended   time.Time
	Response         map[string]any // the Instance at completion, or nil for Empty
}

// operationName renders the canonical operation name.
func operationName(scope state.Scope, id string) string {
	return "projects/" + scope.Project + "/locations/" + scope.Location + "/operations/" + id
}

// wire renders the operation with the Redis v1 metadata and response types.
func (o operation) wire(scope state.Scope) map[string]any {
	response := map[string]any{"@type": emptyType}
	if o.Response != nil {
		response = maps.Clone(o.Response)
		response["@type"] = instanceType
	}
	return map[string]any{
		"name": operationName(scope, o.ID),
		"metadata": map[string]any{
			"@type":      operationType,
			"createTime": o.Created.Format(time.RFC3339Nano),
			"endTime":    o.Ended.Format(time.RFC3339Nano),
			"target":     o.Target,
			"verb":       o.Verb,
			"apiVersion": "v1",
		},
		"done":     true,
		"response": response,
	}
}

// remember appends an operation, keeping the newest maxOperations.
func (c *catalog) remember(record operation) {
	c.operations = append(c.operations, record)
	if extra := len(c.operations) - maxOperations; extra > 0 {
		c.operations = append([]operation(nil), c.operations[extra:]...)
	}
}

// page resolves pageSize/pageToken into a window over total ordered items.
// The token is the opaque decimal offset this provider returned.
func page(query url.Values, total int) (int, int, string, error) {
	size, err := pageSize(query.Get("pageSize"))
	if err != nil {
		return 0, 0, "", err
	}
	start, err := pageStart(query.Get("pageToken"), total)
	if err != nil {
		return 0, 0, "", err
	}
	end := min(start+size, total)
	next := ""
	if end < total {
		next = strconv.Itoa(end)
	}
	return start, end, next, nil
}

// pageSize applies the documented default and maximum of 1000.
func pageSize(text string) (int, error) {
	if text == "" {
		return maxPageSize, nil
	}
	value, err := strconv.Atoi(text)
	if err != nil || value < 0 {
		return 0, invalid("pageSize must be a non-negative integer")
	}
	if value == 0 || value > maxPageSize {
		return maxPageSize, nil
	}
	return value, nil
}

// pageStart validates a page token this provider issued.
func pageStart(token string, total int) (int, error) {
	if token == "" {
		return 0, nil
	}
	value, err := strconv.Atoi(token)
	if err != nil || value < 0 || value > total || strconv.Itoa(value) != token {
		return 0, invalid("Invalid page token")
	}
	return value, nil
}

// listBody renders one page of a collection, omitting empty fields as proto3 JSON does.
func listBody(field string, items []map[string]any, next string) map[string]any {
	body := map[string]any{}
	if len(items) > 0 {
		body[field] = items
	}
	if next != "" {
		body["nextPageToken"] = next
	}
	return body
}
