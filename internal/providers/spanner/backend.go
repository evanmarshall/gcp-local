package spanner

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"runtime"
	"time"

	"cloud.google.com/go/spanner/admin/instance/apiv1/instancepb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// EmulatorVersion is the pinned official emulator release (toolchains/spanner.json).
const EmulatorVersion = "1.5.58"

// pinnedFiles are the checksums of the installed emulator binary and the
// libstdc++ it requires, per Linux architecture. They mirror toolchains/spanner.json.
var pinnedFiles = map[string]struct{ emulator, libstdcxx string }{
	"amd64": {
		emulator:  "eb82a863bf80b43637ce04066b934c7b23ad3d0a3b9a73201fc6ac1895219604",
		libstdcxx: "7d03c9686574bad9ee3590450fc441481b2ee01c97ee13647a72cad0637143c8",
	},
	"arm64": {
		emulator:  "7a2457726dae20c0ef08efc50db4b8fc0974cd7e758e2c532f0de51fd1866551",
		libstdcxx: "11bba08278c07d15ac13a0e469cd84667c707bb480445bf370564c70a09e5357",
	},
}

// Backend configuration sentinels.
var (
	errPlatform        = errors.New("the Spanner emulator backend runs only on linux/amd64 and linux/arm64")
	errBackendPaths    = errors.New("spanner requires an absolute emulator path and library directory")
	errEmulatorMissing = errors.New("spanner emulator binary is not an executable file")
	errPinMismatch     = errors.New("spanner emulator file does not match the committed pin")
)

// maxPinnedFile bounds hashing of the pinned files.
const maxPinnedFile = 256 << 20

// Config locates the installed official emulator: the emulator_main binary and
// the directory holding the libstdc++.so.6 extracted from the same image.
type Config struct {
	Emulator, Libraries string
}

func (c Config) validate() error {
	pins, ok := pinnedFiles[runtime.GOARCH]
	if runtime.GOOS != "linux" || !ok {
		return errPlatform
	}
	if !filepath.IsAbs(c.Emulator) || !filepath.IsAbs(c.Libraries) {
		return errBackendPaths
	}
	info, err := os.Stat(c.Emulator)
	if err != nil || !info.Mode().IsRegular() || info.Mode().Perm()&0o111 == 0 {
		return errEmulatorMissing
	}
	if err := verifyPin(c.Emulator, pins.emulator); err != nil {
		return err
	}
	return verifyPin(filepath.Join(c.Libraries, "libstdc++.so.6"), pins.libstdcxx)
}

// verifyPin compares one file's SHA-256 with its pin.
func verifyPin(path, want string) error {
	file, err := os.Open(path) //nolint:gosec // operator-configured absolute path
	if err != nil {
		return fmt.Errorf("open %s: %w", filepath.Base(path), err)
	}
	defer file.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(file, maxPinnedFile)); err != nil {
		return fmt.Errorf("hash %s: %w", filepath.Base(path), err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != want {
		return fmt.Errorf("%w: %s", errPinMismatch, filepath.Base(path))
	}
	return nil
}

// emulator is one official emulator process restricted to one project scope.
// It owns instances, databases, schemas, and operations. Only the provider
// holds this private loopback connection; no caller credentials, proxies, or
// ambient ADC reach the child process.
type emulator struct {
	config     Config
	scope      state.Scope
	process    *backend.Process
	connection *grpc.ClientConn
	directory  string
}

func (b *emulator) Start(ctx context.Context) error {
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for the emulator port: %w", err)
	}
	endpoint := listener.Addr().String()
	_ = listener.Close()
	b.directory, err = os.MkdirTemp("", "gcp-local-spanner-")
	if err != nil {
		return fmt.Errorf("create the emulator directory: %w", err)
	}
	b.connection, err = grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(4<<20)))
	if err != nil {
		return fmt.Errorf("connect to the emulator: %w", err)
	}
	b.process, err = backend.NewProcess(backend.ProcessConfig{
		Command:   []string{b.config.Emulator, "--host_port=" + endpoint},
		Directory: b.directory,
		Environment: []string{"PATH=/usr/bin:/bin", "HOME=" + b.directory, "TMPDIR=" + b.directory,
			"LD_LIBRARY_PATH=" + b.config.Libraries},
		Probe:       b.probe,
		ResetAssets: func(context.Context) error { return nil },
	})
	if err != nil {
		return fmt.Errorf("configure the emulator process: %w", err)
	}
	if err := b.process.Start(ctx); err != nil {
		return fmt.Errorf("start the emulator process: %w", err)
	}
	return nil
}

// probe lists instance configs, which succeeds once the gRPC server serves.
func (b *emulator) probe(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
	defer cancel()
	request := &instancepb.ListInstanceConfigsRequest{Parent: "projects/" + b.scope.Project}
	if err := b.connection.Invoke(ctx, rpcListInstanceConfigs, request, &instancepb.ListInstanceConfigsResponse{}); err != nil {
		return fmt.Errorf("query emulator readiness: %w", err)
	}
	return nil
}

func (b *emulator) Ready(ctx context.Context) error {
	if b.process == nil {
		return backend.ErrProcessExited
	}
	if err := b.process.Ready(ctx); err != nil {
		return fmt.Errorf("emulator readiness: %w", err)
	}
	return nil
}

func (b *emulator) Stop(ctx context.Context) error {
	if b.connection != nil {
		_ = b.connection.Close()
	}
	if b.process != nil {
		if err := b.process.Stop(ctx); err != nil {
			return fmt.Errorf("stop the emulator process: %w", err)
		}
	}
	if b.directory != "" {
		if err := os.RemoveAll(b.directory); err != nil {
			return fmt.Errorf("remove the emulator directory: %w", err)
		}
		b.directory = ""
	}
	return nil
}

// Reset stops the process: the emulator keeps all state in memory.
func (b *emulator) Reset(ctx context.Context) error { return b.Stop(ctx) }

func (b *emulator) client() grpc.ClientConnInterface { return b.connection }

// driver is the backend contract the provider forwards through.
type driver interface {
	backend.Backend
	client() grpc.ClientConnInterface
}
