package main

import (
	"context"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"os/signal"
	"syscall"
	"time"

	core "github.com/evanmarshall/gcp-local/internal/runtime"
)

var version = "dev"

func run(ctx context.Context, args []string, output, errors io.Writer) int {
	flags := flag.NewFlagSet("gcp-local", flag.ContinueOnError)
	flags.SetOutput(errors)
	address := flags.String("listen", "127.0.0.1:4566", "HTTP listen address")
	grpcAddress := flags.String("grpc-listen", "127.0.0.1:4567", "gRPC listen address")
	shutdown := flags.Duration("shutdown-timeout", 10*time.Second, "maximum duration to drain requests")
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
	logger := slog.New(slog.NewJSONHandler(output, nil))
	logger.Info("gcp-local listening", "address", listener.Addr().String(), "grpc_address", grpcListener.Addr().String(), "version", version)
	if err := core.New(version, *shutdown).Serve(ctx, listener, grpcListener); err != nil {
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
