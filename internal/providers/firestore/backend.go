package firestore

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// The pinned official emulator; toolchains/firestore.json records its source.
const (
	EmulatorVersion = "1.19.9"
	EmulatorSHA256  = "c8c8a63a5ab87cc1b677642c16b794dd9be241fe2d50dc52242b353d27e189fe"
	emulatorMain    = "com.google.cloud.datastore.emulator.firestore.CloudFirestore"
)

// Backend configuration sentinels.
var (
	errBackendPaths = errors.New("firestore requires absolute java and pinned emulator jar paths")
	errJavaMissing  = errors.New("firestore java executable is unavailable")
	errJarMismatch  = errors.New("firestore emulator jar does not match the committed pin")
)

// Config locates the Java runtime and the checksum-pinned emulator JAR.
type Config struct {
	Java, JAR string
}

func (c Config) validate() error {
	if !filepath.IsAbs(c.Java) || !filepath.IsAbs(c.JAR) {
		return errBackendPaths
	}
	java, err := os.Stat(c.Java)
	if err != nil || !java.Mode().IsRegular() || java.Mode().Perm()&0111 == 0 {
		return errJavaMissing
	}
	jar, err := os.Open(c.JAR)
	if err != nil {
		return fmt.Errorf("open emulator jar: %w", err)
	}
	defer jar.Close()
	hash := sha256.New()
	if _, err := io.Copy(hash, io.LimitReader(jar, 128<<20)); err != nil {
		return fmt.Errorf("hash emulator jar: %w", err)
	}
	if hex.EncodeToString(hash.Sum(nil)) != EmulatorSHA256 {
		return errJarMismatch
	}
	return nil
}

// driver is the provider's view of one scoped document backend: a private
// loopback base URL that serves the Firestore v1 REST documents surface.
type driver interface {
	backend.Backend
	baseURL() string
}

// emulator runs the official JVM for one project. It owns document state; the
// provider holds the only connection. No caller credentials, proxy settings,
// or ambient ADC enter the child, and single-project mode rejects any other
// project ID as a second isolation check.
type emulator struct {
	config    Config
	scope     state.Scope
	process   *backend.Process
	endpoint  string
	directory string
}

func (b *emulator) Start(ctx context.Context) error {
	if err := b.config.validate(); err != nil {
		return err
	}
	var listen net.ListenConfig
	listener, err := listen.Listen(ctx, "tcp", "127.0.0.1:0")
	if err != nil {
		return fmt.Errorf("listen for the emulator port: %w", err)
	}
	b.endpoint = "http://" + listener.Addr().String()
	_, port, _ := net.SplitHostPort(listener.Addr().String())
	_ = listener.Close()
	b.directory, err = os.MkdirTemp("", "gcp-local-firestore-")
	if err != nil {
		return fmt.Errorf("create the emulator data directory: %w", err)
	}
	probe, err := backend.HTTPProbe(b.endpoint + "/")
	if err != nil {
		return fmt.Errorf("configure the emulator probe: %w", err)
	}
	b.process, err = backend.NewProcess(backend.ProcessConfig{
		Command: []string{b.config.Java, "-Duser.language=en", "-Xms32m", "-Xmx256m", "-XX:MaxDirectMemorySize=128m", "-XX:ActiveProcessorCount=2",
			"-Djava.io.tmpdir=" + b.directory, "-cp", b.config.JAR, emulatorMain,
			"--host=127.0.0.1", "--port=" + port, "--project_id=" + b.scope.Project,
			"--single_project_mode=true", "--single_project_mode_error=true"},
		Directory: b.directory, Environment: []string{"PATH=/usr/bin:/bin", "TMPDIR=" + b.directory, "HOME=" + b.directory},
		Probe:       probe,
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
	if b.process != nil {
		if err := b.process.Stop(ctx); err != nil {
			return fmt.Errorf("stop the emulator process: %w", err)
		}
	}
	if b.directory != "" {
		if err := os.RemoveAll(b.directory); err != nil {
			return fmt.Errorf("remove the emulator data directory: %w", err)
		}
		b.directory = ""
	}
	return nil
}

// Reset stops the JVM; the next request starts an empty one.
func (b *emulator) Reset(ctx context.Context) error { return b.Stop(ctx) }

func (b *emulator) baseURL() string { return b.endpoint }

// backendClient reaches only the private loopback emulator: no proxy, no
// redirects, and a bounded wait.
var backendClient = &http.Client{
	Timeout:       30 * time.Second,
	Transport:     &http.Transport{Proxy: nil, MaxIdleConnsPerHost: 4},
	CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
}
