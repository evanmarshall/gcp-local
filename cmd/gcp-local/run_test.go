package main

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
)

func TestRunRuntimeRefusesIncompleteDockerFlags(t *testing.T) {
	server := core.WithGateway("test", time.Second, gateway.New(gateway.NewRegistry(), nil))
	for _, options := range []serveOptions{
		{runNetwork: "env_backend"},
		{runEnv: "env"},
		{runSocket: "/var/run/docker.sock", runProcess: map[string]string{"app:1": "python3 app.py"}},
	} {
		if _, err := runRuntime(context.Background(), options, server); !errors.Is(err, errRunDockerFlags) && !errors.Is(err, errRunBothRuntimes) {
			t.Errorf("%+v: %v", options, err)
		}
	}
	if _, err := runRuntime(context.Background(), serveOptions{runSocket: "/var/run/docker.sock", runNetwork: "env_backend"}, server); err == nil {
		t.Error("a network without an environment label was accepted")
	}
	if server.AfterShutdown != nil {
		t.Error("a refused runtime registered a shutdown sweep")
	}
}
