package pubsub

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"strings"
	"testing"
)

const restSchemas = "/v1/projects/project-a/schemas"

// TestRESTSchemaLifecycle drives projects.schemas.* and a typed topic over REST.
func TestRESTSchemaLifecycle(t *testing.T) {
	_, registry, b := memoryFixture(t)
	c := newRESTClient(t, registry)
	body := `{"type":"PROTOCOL_BUFFER","definition":` + quoteJSON(protoEvent) + `}`
	created := c.do(owner, http.MethodPost, restSchemas+"?schemaId=events", body, 200)
	failIf(t, created["name"] != protoSchema1 || created["type"] != "PROTOCOL_BUFFER" || created["revisionCreateTime"] == nil, "created", created)
	first := typed[string](t, created["revisionId"])
	committed := c.do(owner, http.MethodPost, restSchemas+"/events:commit", `{"schema":{"name":"`+protoSchema1+`","type":"PROTOCOL_BUFFER","definition":`+quoteJSON(protoEventV2)+`}}`, 200)
	failIf(t, !strings.HasPrefix(typed[string](t, committed["name"]), protoSchema1+"@"), "committed", committed)
	pinned := c.do(owner, http.MethodGet, restSchemas+"/events@"+first+"?view=BASIC", "", 200)
	failIf(t, pinned["name"] != protoSchema1+"@"+first || pinned["definition"] != nil, "pinned", pinned)
	revisions := c.do(owner, http.MethodGet, restSchemas+"/events:listRevisions?pageSize=1", "", 200)
	list, _ := revisions["schemas"].([]any)
	failIf(t, len(list) != 1 || revisions["nextPageToken"] != first, "revisions", revisions)
	c.do(owner, http.MethodGet, restSchemas+"?view=FULL", "", 200)
	message := base64.StdEncoding.EncodeToString([]byte(`{"id":"a"}`))
	c.do(owner, http.MethodPost, restSchemas+":validateMessage", `{"name":"`+protoSchema1+`","message":"`+message+`","encoding":"JSON"}`, 200)
	c.do(owner, http.MethodPost, restSchemas+":validate", `{"schema":{"type":"PROTOCOL_BUFFER","definition":"message {"}}`, http.StatusBadRequest)
	c.do(owner, http.MethodPost, restSchemas+":validate", `{"schema":{"type":"PROTOCOL_BUFFER","definition":"x"},"unknown":1}`, http.StatusBadRequest)
	c.do(other, http.MethodGet, restSchemas+"/events", "", http.StatusForbidden)
	c.do(owner, http.MethodPost, restSchemas+"/events:setIamPolicy", `{"policy":{"bindings":[{"role":"roles/pubsub.viewer","members":["`+reader+`"]}]}}`, 200)
	c.do(reader, http.MethodGet, restSchemas+"/events", "", 200)
	policy := c.do(owner, http.MethodGet, restSchemas+"/events:getIamPolicy", "", 200)
	bindings, _ := policy["bindings"].([]any)
	failIf(t, len(bindings) != 1, "policy", policy)
	topic := c.do(owner, http.MethodPut, "/v1/projects/project-a/topics/typed", `{"schemaSettings":{"schema":"`+protoSchema1+`","encoding":"JSON","firstRevisionId":"`+first+`"}}`, 200)
	settings, _ := topic["schemaSettings"].(map[string]any)
	failIf(t, settings["schema"] != protoSchema1 || settings["firstRevisionId"] != first, "topic", topic)
	publish := "/v1/projects/project-a/topics/typed:publish"
	c.do(owner, http.MethodPost, publish, `{"messages":[{"data":"`+message+`"}]}`, 200)
	invalid := base64.StdEncoding.EncodeToString([]byte(`{"id":5}`))
	c.do(owner, http.MethodPost, publish, `{"messages":[{"data":"`+invalid+`"}]}`, http.StatusBadRequest)
	failIf(t, b.called(publisher+"Publish") != 1, "an invalid REST publish reached the emulator", nil)
	refused := c.do(owner, http.MethodDelete, restSchemas+"/events:deleteRevision?revisionId="+first, "", http.StatusNotImplemented)
	failIf(t, !strings.Contains(fmt.Sprint(refused), "deleteRevision revisionId"), "revisionId refusal", refused)
	c.do(owner, http.MethodDelete, restSchemas+"/events@"+first+":deleteRevision", "", 200)
	c.do(owner, http.MethodDelete, restSchemas+"/events", "", 200)
	c.do(owner, http.MethodGet, restSchemas+"/events", "", http.StatusNotFound)
	failIf(t, b.called(schemaService+"CreateSchema") != 0, "a Protocol Buffer schema reached the emulator over REST", nil)
}

// quoteJSON renders a string as a JSON literal.
func quoteJSON(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)
	return `"` + replacer.Replace(value) + `"`
}
