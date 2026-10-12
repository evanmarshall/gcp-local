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
	"strings"
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

// mysqlPinned are the SHA-256 checksums of the installed (section-stripped)
// mysqld binaries. They mirror the binaries entries of toolchains/mysql.json.
var mysqlPinned = map[string]map[string]struct{ mysqld, release string }{
	"arm64": {
		"MYSQL_8_4": {"c572ff8332cd2f9600fb59cdcd472958f7cd8d755e7c030e5c7647769bcdf7ee", "8_4_11"},
		"MYSQL_8_0": {"c90066c4c334253e2a3bd87063388a40259602a9b26d36e78f7ce5e5f5b3120b", "8_0_46"},
	},
	"amd64": {
		"MYSQL_8_4": {"b4f8367f8edb7ddc68db48803982aa800562d0f3e7009eb3ee17abd92948c31d", "8_4_11"},
		"MYSQL_8_0": {"eb80259f78723db02f2bfeab0a39a9b10464a33176da4319c562297bbaae2d0a", "8_0_46"},
	},
}

// Backend configuration sentinels.
var (
	errPlatform      = errors.New("the Cloud SQL backends run only on linux/amd64 and linux/arm64")
	errInstallRoot   = errors.New("cloud SQL requires an absolute PostgreSQL or MySQL install root")
	errVersionsNone  = errors.New("no pinned PostgreSQL or MySQL version is installed under the install roots")
	errPinMismatch   = errors.New("database server binary does not match the committed pin")
	errRunAsRequired = errors.New("PostgreSQL and MySQL refuse to run as root; configure an unprivileged uid:gid")
)

// maxPinnedFile bounds hashing of the pinned binaries.
const maxPinnedFile = 128 << 20

// Config locates the installed binaries. Root holds one directory per
// PostgreSQL major version (Root/16, Root/15), as scripts/install_postgres.py
// lays them out; MySQLRoot holds Root/8.4 and Root/8.0 from
// scripts/install_mysql.py. Either may be empty. RunAs is the unprivileged
// user the servers run as when gcp-local runs as root. Publish, when set,
// gives every public-IP instance a port from the data-plane range.
type Config struct {
	Root, MySQLRoot string
	RunAs           *syscall.Credential
	Publish         *dataplane.Pool
}

// distribution is one verified server version.
type distribution struct {
	root, release string
	mysql         bool
}

// validate verifies the installed binaries and returns the usable versions.
func (c Config) validate() (map[string]distribution, error) {
	pins, ok := pinned[runtime.GOARCH]
	if runtime.GOOS != "linux" || !ok {
		return nil, errPlatform
	}
	if !c.rootsValid() {
		return nil, errInstallRoot
	}
	if os.Geteuid() == 0 && (c.RunAs == nil || c.RunAs.Uid == 0) {
		return nil, errRunAsRequired
	}
	versions := map[string]distribution{}
	if err := c.installed(pins, mysqlPinned[runtime.GOARCH], versions); err != nil {
		return nil, err
	}
	if len(versions) == 0 {
		return nil, errVersionsNone
	}
	return versions, nil
}

// installed verifies the versions under every configured install root.
func (c Config) installed(pins map[string]struct{ initdb, postgres, release string }, mysql map[string]struct{ mysqld, release string }, versions map[string]distribution) error {
	if c.Root != "" {
		if err := installed(c.Root, pins, versions); err != nil {
			return err
		}
	}
	if c.MySQLRoot != "" {
		return installedMySQL(c.MySQLRoot, mysql, versions)
	}
	return nil
}

// rootsValid reports whether at least one install root is set and every set root is absolute.
func (c Config) rootsValid() bool {
	for _, root := range []string{c.Root, c.MySQLRoot} {
		if root != "" && !filepath.IsAbs(root) {
			return false
		}
	}
	return c.Root != "" || c.MySQLRoot != ""
}

// installed verifies every installed PostgreSQL major version under root against its pins.
func installed(root string, pins map[string]struct{ initdb, postgres, release string }, versions map[string]distribution) error {
	for version, pin := range pins {
		directory := filepath.Join(root, version[len("POSTGRES_"):])
		if _, err := os.Stat(filepath.Join(directory, "bin", "postgres")); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := verifyPin(filepath.Join(directory, "bin", "postgres"), pin.postgres); err != nil {
			return err
		}
		if err := verifyPin(filepath.Join(directory, "bin", "initdb"), pin.initdb); err != nil {
			return err
		}
		versions[version] = distribution{root: directory, release: pin.release}
	}
	return nil
}

// installedMySQL verifies every installed MySQL version under root against its
// pins: MYSQL_8_4 lives in root/8.4.
func installedMySQL(root string, pins map[string]struct{ mysqld, release string }, versions map[string]distribution) error {
	for version, pin := range pins {
		directory := filepath.Join(root, strings.ReplaceAll(version[len("MYSQL_"):], "_", "."))
		if _, err := os.Stat(filepath.Join(directory, "bin", "mysqld")); errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err := verifyPin(filepath.Join(directory, "bin", "mysqld"), pin.mysqld); err != nil {
			return err
		}
		versions[version] = distribution{root: directory, release: pin.release, mysql: true}
	}
	return nil
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
// PostgreSQL and MySQL servers, one per Cloud SQL instance.
type driver interface {
	backend.Backend
	create(ctx context.Context, spec serverSpec) error
	remove(ctx context.Context, instance string) error
	// exec runs SQL as the private administrative account; database selects
	// the PostgreSQL database and is ignored by MySQL.
	exec(ctx context.Context, instance, database, sql string) ([][]string, error)
	// endpoint returns the server's private socket and a channel closed when
	// the server is removed or stopped.
	endpoint(instance string) (string, <-chan struct{}, error)
}

// errBackendType reports a backend that is not a Cloud SQL driver.
var errBackendType = errors.New("cloud SQL backend is not a database server fleet")

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
