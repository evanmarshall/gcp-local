package pubsub

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"time"

	pb "cloud.google.com/go/pubsub/v2/apiv1/pubsubpb"
	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

const EmulatorVersion = "0.8.36"
const EmulatorSHA256 = "51f87210e624d28d7473f3cfa6eb2d4538a7362c8c4948ef08498cdd7595d25a"

type Config struct{ Java, JAR string }

func (c Config) validate() error {
	if !filepath.IsAbs(c.Java) || !filepath.IsAbs(c.JAR) {
		return errors.New("Pub/Sub requires absolute Java and pinned emulator JAR paths")
	}
	java, err := os.Stat(c.Java)
	if err != nil || !java.Mode().IsRegular() || java.Mode().Perm()&0111 == 0 {
		return errors.New("Pub/Sub Java executable is unavailable")
	}
	jar, err := os.Open(c.JAR)
	if err != nil {
		return err
	}
	defer jar.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(jar, 100<<20)); err != nil {
		return err
	}
	if hex.EncodeToString(hash.Sum(nil)) != EmulatorSHA256 {
		return errors.New("Pub/Sub emulator JAR does not match the committed pin")
	}
	return nil
}

// The official JVM owns topic/subscription configuration and message queues.
// Only the provider holds this private connection; no caller credentials,
// proxy configuration, push URL, or ambient ADC enter the child process.
type emulator struct {
	config     Config
	scope      state.Scope
	process    *backend.Process
	connection *grpc.ClientConn
	directory  string
}

func (b *emulator) Start(ctx context.Context) error {
	if err := b.config.validate(); err != nil {
		return err
	}
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return err
	}
	endpoint := listener.Addr().String()
	_, port, _ := net.SplitHostPort(endpoint)
	listener.Close()
	b.directory, err = os.MkdirTemp("", "gcp-local-pubsub-")
	if err != nil {
		return err
	}
	b.connection, err = grpc.NewClient("passthrough:///"+endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "tcp", endpoint)
		}), grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(2<<20)))
	if err != nil {
		return err
	}
	b.process, err = backend.NewProcess(backend.ProcessConfig{
		Command: []string{b.config.Java, "-Xms32m", "-Xmx256m", "-XX:MaxDirectMemorySize=128m", "-XX:ActiveProcessorCount=2", "-Djava.io.tmpdir=" + b.directory,
			"-jar", b.config.JAR, "--host=127.0.0.1", "--port=" + port, "--projects=" + b.scope.Project},
		Directory: b.directory, Environment: []string{"PATH=/usr/bin:/bin", "TMPDIR=" + b.directory},
		Probe: func(ctx context.Context) error {
			ctx, cancel := context.WithTimeout(ctx, 500*time.Millisecond)
			defer cancel()
			_, err := pb.NewPublisherClient(b.connection).GetTopic(ctx, &pb.GetTopicRequest{Topic: "projects/" + b.scope.Project + "/topics/gcp-local-readiness"})
			if status.Code(err) == codes.NotFound || err == nil {
				return nil
			}
			return err
		},
		ResetAssets: func(context.Context) error { return nil },
	})
	if err != nil {
		return err
	}
	return b.process.Start(ctx)
}
func (b *emulator) Ready(ctx context.Context) error {
	if b.process == nil {
		return backend.ErrProcessExited
	}
	return b.process.Ready(ctx)
}
func (b *emulator) Stop(ctx context.Context) error {
	if b.connection != nil {
		_ = b.connection.Close()
	}
	if b.process != nil {
		if err := b.process.Stop(ctx); err != nil {
			return err
		}
	}
	if b.directory != "" {
		if err := os.RemoveAll(b.directory); err != nil {
			return err
		}
		b.directory = ""
	}
	return nil
}
func (b *emulator) Reset(ctx context.Context) error { return b.Stop(ctx) }

func (b *emulator) client() grpc.ClientConnInterface { return b.connection }

type driver interface {
	backend.Backend
	client() grpc.ClientConnInterface
}
