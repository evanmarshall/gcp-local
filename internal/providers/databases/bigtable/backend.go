package bigtable

import (
	"context"
	"errors"
	"fmt"
	"net"

	"cloud.google.com/go/bigtable/admin/apiv2/adminpb"
	"cloud.google.com/go/bigtable/bttest"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/test/bufconn"
)

// bufferSize is the in-memory connection buffer between provider and emulator.
const bufferSize = 1 << 20

// emulatorBackend runs Google's Bigtable emulator (bttest, the cbtemulator code)
// in-process for one project scope. It listens on an in-memory bufconn
// listener, so it opens no network port, loads no credentials, and is reachable
// only through the provider's admin client after authorization.
type emulatorBackend struct {
	// options are extra emulator server options; tests use them to observe calls.
	options  []grpc.ServerOption
	listener *bufconn.Listener
	server   *bttest.Server
	conn     *grpc.ClientConn
	admin    adminpb.BigtableTableAdminClient
}

// errBackendStopped reports use of a stopped emulator.
var errBackendStopped = errors.New("bigtable backend is stopped")

func (b *emulatorBackend) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	listener := bufconn.Listen(bufferSize)
	server, err := bttest.NewServerWithListener(listener, b.options...)
	if err != nil {
		_ = listener.Close()
		return fmt.Errorf("start the bigtable emulator: %w", err)
	}
	conn, err := grpc.NewClient("passthrough:///bttest",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return listener.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		server.Close()
		_ = listener.Close()
		return fmt.Errorf("connect to the bigtable emulator: %w", err)
	}
	b.listener, b.server, b.conn = listener, server, conn
	b.admin = adminpb.NewBigtableTableAdminClient(conn)
	return nil
}

func (b *emulatorBackend) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.admin == nil {
		return errBackendStopped
	}
	return nil
}

// Stop discards the emulator and every table it held.
func (b *emulatorBackend) Stop(ctx context.Context) error {
	if b.conn != nil {
		_ = b.conn.Close()
	}
	// bttest does not close a caller-owned listener, and grpc.Server.Stop waits
	// for Serve to return, so the listener must close first to end Accept.
	if b.listener != nil {
		_ = b.listener.Close()
	}
	if b.server != nil {
		b.server.Close()
	}
	b.listener, b.server, b.conn, b.admin = nil, nil, nil, nil
	return ctx.Err()
}

// Reset has no assets beyond the in-memory emulator that Stop discards.
func (b *emulatorBackend) Reset(ctx context.Context) error { return b.Stop(ctx) }

// emulatorOf returns the running emulator behind one manager backend.
func emulatorOf(driver backend.Backend) (*emulatorBackend, error) {
	b, ok := driver.(*emulatorBackend)
	if !ok || b.admin == nil {
		return nil, errBackendStopped
	}
	return b, nil
}
