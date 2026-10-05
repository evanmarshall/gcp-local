//go:build conformance

// Command storage-conformance is only a test credential adapter. All API,
// authorization, state, and backend behavior uses the production implementation.
// Fixed fixture tokens are deliberately absent from cmd/gcp-local.
package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
	"github.com/evanmarshall/gcp-local/internal/state"
)

func main() {
	interfaces, err := net.Interfaces()
	if err != nil || len(interfaces) != 1 || interfaces[0].Name != "lo" || os.Getenv("GCP_LOCAL_ISOLATED") != "1" {
		log.Fatal("Run the conformance fixture inside scripts/run_isolated.py")
	}
	index := state.NewIndex()
	engine, err := iam.New(index)
	must(err)
	for i, project := range []string{"gcp-local-test", "synthetic-peer"} {
		name := "//cloudresourcemanager.googleapis.com/projects/" + project
		number := "000000000001"
		if i == 1 {
			number = "000000000002"
		}
		must(index.Put("resourcemanager", state.Resource{Name: name, Service: "resourcemanager", Kind: "project", Number: number, Scope: state.Scope{Project: project, Location: state.Global}}))
		_, err := engine.SetPolicy(name, iam.Policy{Bindings: []iam.Binding{{Role: "roles/storage.admin", Members: []string{"serviceAccount:fixture@" + project + ".iam.gserviceaccount.com"}}}})
		must(err)
	}
	resolver := func(ctx context.Context, token string) (gateway.Principal, error) {
		for _, actor := range []string{"fixture", "allowed", "denied", "peer"} {
			if token == "gcp-local-test:"+actor {
				project := "gcp-local-test"
				if actor == "peer" {
					project = "synthetic-peer"
				}
				return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + actor + "@" + project + ".iam.gserviceaccount.com", Project: project, Scopes: []string{"https://www.googleapis.com/auth/cloud-platform"}}, nil
			}
		}
		return gateway.Principal{State: gateway.Unknown}, nil
	}
	registry := gateway.NewRegistry()
	server := core.WithGateway("conformance", 5*time.Second, gateway.New(registry, resolver))
	server.IAM = engine
	_, err = storage.New(registry, server.Backends, index, engine)
	must(err)
	http, err := net.Listen("tcp", "127.0.0.1:4566")
	must(err)
	grpc, err := net.Listen("tcp", "127.0.0.1:0")
	must(err)
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	must(server.Serve(ctx, core.Listeners{HTTP: http, GRPC: grpc}))
}
func must(err error) {
	if err != nil {
		log.Fatal(err)
	}
}
