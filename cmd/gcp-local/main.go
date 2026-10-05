package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"

	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/iam"
	"github.com/evanmarshall/gcp-local/internal/identity"
	"github.com/evanmarshall/gcp-local/internal/providers/storage"
	core "github.com/evanmarshall/gcp-local/internal/runtime"
)

var version = "dev"

func run(ctx context.Context, args []string, output, errors io.Writer) int {
	if len(args) > 0 && args[0] == "why-denied" {
		return whyDenied(args[1:], output, errors)
	}
	flags := flag.NewFlagSet("gcp-local", flag.ContinueOnError)
	flags.SetOutput(errors)
	address := flags.String("listen", "127.0.0.1:4566", "HTTP listen address")
	grpcAddress := flags.String("grpc-listen", "127.0.0.1:4567", "gRPC listen address")
	shutdown := flags.Duration("shutdown-timeout", 10*time.Second, "maximum duration to drain requests")
	iamPath := flags.String("iam-config", "", "owner-only local hierarchy and IAM policy configuration")
	services := flags.String("services", "", "explicit service selection: storage")
	identityPath := flags.String("identity-config", "", "owner-only local identity configuration file")
	metadataAddress := flags.String("metadata-listen", "", "optional per-workload loopback metadata listener")
	metadataPrincipal := flags.String("metadata-principal", "", "enrolled service account bound to the metadata listener")
	showVersion := flags.Bool("version", false, "print the build version")
	if err := flags.Parse(args); err != nil {
		return 2
	}
	if flags.NArg() != 0 || *shutdown <= 0 {
		fmt.Fprintln(errors, "Supply valid flags and a positive shutdown timeout")
		return 2
	}
	if *showVersion {
		fmt.Fprintln(output, version)
		return 0
	}
	var resolver gateway.Resolver
	var identities *identity.Service
	var metadataHandler http.Handler
	if *identityPath != "" {
		config, err := identity.Load(*identityPath)
		if err != nil {
			fmt.Fprintln(errors, err)
			return 2
		}
		identities, err = identity.New(config, nil)
		if err != nil {
			fmt.Fprintln(errors, err)
			return 2
		}
		resolver = identities.Resolve
	}
	if *metadataAddress != "" || *metadataPrincipal != "" {
		host, _, err := net.SplitHostPort(*metadataAddress)
		ip := net.ParseIP(host)
		if err != nil || ip == nil || !ip.IsLoopback() || identities == nil || *metadataPrincipal == "" {
			fmt.Fprintln(errors, "Metadata requires a loopback address, identity config, and an enrolled service account")
			return 2
		}
		metadataHandler, err = identities.Metadata(*metadataPrincipal)
		if err != nil {
			fmt.Fprintln(errors, err)
			return 2
		}
	}
	registry := gateway.NewRegistry()
	server := core.WithGateway(version, *shutdown, gateway.New(registry, resolver))
	if identities != nil {
		server.Identity = identities
	}
	if *iamPath != "" {
		engine, index, err := iam.Load(*iamPath)
		if err != nil {
			fmt.Fprintln(errors, err)
			return 2
		}
		if *services == "storage" {
			if identities == nil {
				fmt.Fprintln(errors, "Storage requires explicit local identity configuration")
				return 2
			}
			if _, err := storage.New(registry, server.Backends, index, engine); err != nil {
				fmt.Fprintln(errors, err)
				return 2
			}
		}
		server.IAM = engine
	}
	if (*services != "" && *services != "storage") || (*services != "" && *iamPath == "") {
		fmt.Fprintln(errors, "Select services=storage with explicit IAM and identity configuration")
		return 2
	}
	listener, err := net.Listen("tcp", *address)
	if err != nil {
		fmt.Fprintln(errors, "Unable to bind the requested HTTP listener:", err)
		return 1
	}
	grpcListener, err := net.Listen("tcp", *grpcAddress)
	if err != nil {
		listener.Close()
		fmt.Fprintln(errors, "Unable to bind the requested gRPC listener:", err)
		return 1
	}
	listeners := core.Listeners{HTTP: listener, GRPC: grpcListener}
	if metadataHandler != nil {
		listeners.Metadata, err = net.Listen("tcp", *metadataAddress)
		if err != nil {
			listener.Close()
			grpcListener.Close()
			fmt.Fprintln(errors, "Unable to bind the requested metadata listener:", err)
			return 1
		}
		server.Metadata = &http.Server{Handler: metadataHandler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 10 * time.Second, WriteTimeout: 10 * time.Second, IdleTimeout: 30 * time.Second}
	}
	logger := slog.New(slog.NewJSONHandler(output, nil))
	logger.Info("gcp-local listening", "address", listener.Addr().String(), "grpc_address", grpcListener.Addr().String(), "version", version)
	if listeners.Metadata != nil {
		logger.Info("workload metadata listening", "address", listeners.Metadata.Addr().String())
	}
	if err := server.Serve(ctx, listeners); err != nil {
		logger.Error("gcp-local stopped with an error", "error", err.Error())
		return 1
	}
	logger.Info("gcp-local stopped")
	return 0
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	os.Exit(run(ctx, os.Args[1:], os.Stdout, os.Stderr))
}
