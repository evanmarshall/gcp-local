package main

import (
	"context"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// Pub/Sub push and Cloud Tasks both mint ID tokens. The minting permission
// plan registers once per engine, so both providers must share one key set;
// a second key set on the same engine is refused.
func TestCloudTasksSharesTheIDTokenKeys(t *testing.T) {
	index := state.NewIndex()
	engine, err := iam.New(index)
	if err != nil {
		t.Fatal(err)
	}
	identities := &identity.Service{}
	selected := map[string]bool{"pubsub": true, "cloudtasks": true}
	tokens, err := assembleIDTokens(selected, index, engine, identities)
	if err != nil || tokens == nil || identities.IDTokens != tokens {
		t.Fatalf("shared keys: %v", err)
	}
	registry := gateway.NewRegistry()
	server := core.WithGateway("test", time.Second, gateway.New(registry, nil))
	if err := assembleCloudTasks(context.Background(), selected, registry, server, index, engine, tokens); err != nil {
		t.Fatalf("Cloud Tasks with the shared keys: %v", err)
	}
	if _, err := identity.NewIDTokens(engine, index); err == nil {
		t.Fatal("a second key set registered the minting plan twice")
	}
	if tokens, err := assembleIDTokens(map[string]bool{"storage": true}, index, engine, nil); err != nil || tokens != nil {
		t.Fatalf("no minting provider: %v %v", tokens, err)
	}
}
