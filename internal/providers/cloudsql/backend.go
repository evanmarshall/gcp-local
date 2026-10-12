package cloudsql

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"syscall"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"github.com/evanmarshall/gcp-local/internal/dataplane"
)

// pinned are the SHA-256 checksums of the installed server binaries per
// architecture and Cloud SQL databaseVersion. They mirror toolchains/postgres.json.
var pinned = map[string]map[string]struct{ initdb, postgres, release string }{
	"arm64": {
		"POSTGRES_16": {"d370000e13da111995726b463ffb64b4ebfc8713c10e5214d2a95f771d78db8d", "a3fc636a0a25fef762baa67ffd3f8f05b526ba2cd14236555523c368eeaeb0f3", "16_15"},
		"POSTGRES_15": {"d9e3af7ba8cbda3c201e9963861b39a2c4dedded5996eeefb5ee3d7c48939690", "bea46c1a96f495267a3bedbae03b2d3a1825f28fdbf184b78cd2016f7567d8e7", "15_19"},
	},
	"amd64": {
		"POSTGRES_16": {"b49dde31f6b284e39369dc4c8e4f55e83296da9721a05773a539deaccbb2e982", "ea144a02d075d44f28ef2247ec42f8817b45bc89da6e5b5e54313270982b61f6", "16_15"},
		"POSTGRES_15": {"b446b26a90fd3599d03efa401529073e5cf7a352af6e8d2b960598489e966ff7", "f7480af9888937ad8f4c4c9be62d61e84cb7062a907e016f81226229aa66e77d", "15_19"},
	},
}

// Backend configuration sentinels.
var (
	errPlatform      = errors.New("the Cloud SQL PostgreSQL backend runs only on linux/amd64 and linux/arm64")
	errPostgresRoot  = errors.New("cloud SQL requires an absolute PostgreSQL install root")
	errPostgresNone  = errors.New("no pinned PostgreSQL version is installed under the install root")
	errPinMismatch   = errors.New("PostgreSQL binary does not match the committed pin")
	errRunAsRequired = errors.New("PostgreSQL refuses to run as root; configure an unprivileged uid:gid")
)

// maxPinnedFile bounds hashing of the pinned binaries.
const maxPinnedFile = 64 << 20

// Config locates the installed binaries: Root holds one directory per major
// version (Root/16, Root/15), as scripts/install_postgres.py lays them out.
// RunAs is the unprivileged user the servers run as when gcp-local runs as root.
// Publish, when set, gives every public-IP instance a port from the
// data-plane range.
type Config struct {
	Root    string
	RunAs   *syscall.Credential
	Publish *dataplane.Pool
}

// distribution is one verified major version.
type distribution struct {
	root, release string
}

// validate verifies the installed binaries and returns the usable versions.
func (c Config) validate() (map[string]distribution, error) {
	pins, ok := pinned[runtime.GOARCH]
	if runtime.GOOS != "linux" || !ok {
		return nil, errPlatform
	}
	if !filepath.IsAbs(c.Root) {
		return nil, errPostgresRoot
	}
	if os.Geteuid() == 0 && (c.RunAs == nil || c.RunAs.Uid == 0) {
		return nil, errRunAsRequired
	}
	return installed(c.Root, pins)
}

// installed verifies every installed major version under root against its pins.
func installed(root string, pins map[string]struct{ initdb, postgres, release string }) (map[string]distribution, error) {
	versions := map[string]distribution{}
	for version, pin := range pins {
		directory := filepath.Join(root, version[len("POSTGRES_"):])
		if _, err := os.Stat(filepath.Join(directory, "bin", "postgres")); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := verifyPin(filepath.Join(directory, "bin", "postgres"), pin.postgres); err != nil {
			return nil, err
		}
		if err := verifyPin(filepath.Join(directory, "bin", "initdb"), pin.initdb); err != nil {
			return nil, err
		}
		versions[version] = distribution{root: directory, release: pin.release}
	}
	if len(versions) == 0 {
		return nil, errPostgresNone
	}
	return versions, nil
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
		return fmt.Errorf("%w: %s", errPinMismatch, path)
	}
	return nil
}

// serverSpec describes one instance's server.
type serverSpec struct {
	Instance, Version, RootPassword string
}

// driver is the backend contract the provider uses: a project's fleet of
// PostgreSQL servers, one per Cloud SQL instance.
type driver interface {
	backend.Backend
	create(ctx context.Context, spec serverSpec) error
	remove(ctx context.Context, instance string) error
	// exec runs SQL as the private administrative role in one database.
	exec(ctx context.Context, instance, database, sql string) ([][]string, error)
	// endpoint returns the server's private socket and a channel closed when
	// the server is removed or stopped.
	endpoint(instance string) (string, <-chan struct{}, error)
}

// errBackendType reports a backend that is not a Cloud SQL driver.
var errBackendType = errors.New("cloud SQL backend is not a PostgreSQL fleet")

// driverOf narrows the manager's backend to the fleet contract.
func driverOf(active backend.Backend) (driver, error) {
	value, ok := active.(driver)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}

// sortedKeys returns a map's keys in order.
func sortedKeys[V any](values map[string]V) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
