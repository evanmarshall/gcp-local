package localtls

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

var now = time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)

// read returns one file from dir through an os.Root.
func read(t *testing.T, dir, name string) []byte {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	data, err := root.ReadFile(name)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func parse(t *testing.T, dir, name string) *x509.Certificate {
	t.Helper()
	block, _ := pem.Decode(read(t, dir, name))
	if block == nil {
		t.Fatalf("%s holds no PEM block", name)
	}
	certificate, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatal(err)
	}
	return certificate
}

// material creates TLS material in a new directory and returns its path.
func material(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "tls")
	if _, err := Load(dir, now); err != nil {
		t.Fatal(err)
	}
	return dir
}

// checkPrivateFiles requires a 0700 directory holding three 0600 files, with
// the server key as the only private key.
func checkPrivateFiles(t *testing.T, dir string) {
	t.Helper()
	if info, err := os.Stat(dir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("directory mode: %v %v", info, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != len(files) {
		t.Fatalf("files: %v %v", entries, err)
	}
	for _, entry := range entries {
		info, err := entry.Info()
		if err != nil || info.Mode().Perm() != 0o600 {
			t.Fatalf("%s mode: %v %v", entry.Name(), info.Mode(), err)
		}
		if bytes.Contains(read(t, dir, entry.Name()), []byte("PRIVATE KEY")) != (entry.Name() == KeyFile) {
			t.Fatalf("%s: unexpected private key presence", entry.Name())
		}
	}
}

func TestCreatesPrivateNameConstrainedMaterial(t *testing.T) {
	dir := material(t)
	checkPrivateFiles(t, dir)
	authority, leaf := parse(t, dir, CAFile), parse(t, dir, CertFile)
	checkCertificates(t, authority, leaf)
	roots := x509.NewCertPool()
	roots.AddCert(authority)
	for name, valid := range map[string]bool{"localhost": true, "gcp-local": true, "127.0.0.1": true, "::1": true, "storage.googleapis.com": false, "10.0.0.1": false} {
		if _, err := leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, DNSName: name}); (err == nil) != valid {
			t.Fatalf("verify %s: %v", name, err)
		}
	}
}

// checkCertificates requires a constrained CA and a server certificate for
// the loopback names and the gateway host, valid now.
func checkCertificates(t *testing.T, authority, leaf *x509.Certificate) {
	t.Helper()
	type shape struct {
		CA, PathLenZero, Critical bool
		Permitted, DNS            []string
		IPs                       []net.IP
		Usage                     []x509.ExtKeyUsage
	}
	got := []shape{
		{authority.IsCA, authority.MaxPathLenZero, authority.PermittedDNSDomainsCritical, authority.PermittedDNSDomains, authority.DNSNames, authority.IPAddresses, authority.ExtKeyUsage},
		{leaf.IsCA, leaf.MaxPathLenZero, leaf.PermittedDNSDomainsCritical, leaf.PermittedDNSDomains, leaf.DNSNames, leaf.IPAddresses, leaf.ExtKeyUsage},
	}
	want := []shape{
		{CA: true, PathLenZero: true, Critical: true, Permitted: []string{"localhost", "gcp-local"}},
		{DNS: []string{"localhost", "gcp-local"}, IPs: []net.IP{net.IPv4(127, 0, 0, 1).To4(), net.IPv6loopback}, Usage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("certificates:\n got %+v\nwant %+v", got, want)
	}
	if leaf.NotAfter.Sub(now) > validity || !leaf.NotBefore.Before(now) {
		t.Fatalf("validity %v-%v", leaf.NotBefore, leaf.NotAfter)
	}
}

func TestReloadKeepsTheCAAndEachDirectoryGetsItsOwn(t *testing.T) {
	first, second := material(t), material(t)
	before := read(t, first, CAFile)
	if _, err := Load(first, now.Add(time.Hour)); err != nil {
		t.Fatalf("reload: %v", err)
	}
	if !bytes.Equal(before, read(t, first, CAFile)) || bytes.Equal(before, read(t, second, CAFile)) {
		t.Fatal("reload replaced the CA, or two directories share one")
	}
}

func TestRefusesUnsafeIncompleteOrExpiredMaterial(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, dir string) time.Time
		want   error
	}{
		{"group-readable key", groupReadableKey, errFileMode},
		{"open directory", openDirectory, errDirectory},
		{"missing CA", missingCA, errIncomplete},
		{"expired", expired, errExpired},
		{"another CA", anotherCA, errMismatch},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := material(t)
			if _, err := Load(dir, tc.mutate(t, dir)); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
}

func groupReadableKey(t *testing.T, dir string) time.Time {
	t.Helper()
	if err := os.Chmod(filepath.Join(dir, KeyFile), 0o640); err != nil { //nolint:gosec // the test asserts that a shared key is refused
		t.Fatal(err)
	}
	return now
}

func openDirectory(t *testing.T, dir string) time.Time {
	t.Helper()
	if err := os.Chmod(dir, 0o755); err != nil { //nolint:gosec // the test asserts that an open directory is refused
		t.Fatal(err)
	}
	return now
}

func missingCA(t *testing.T, dir string) time.Time {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, CAFile)); err != nil {
		t.Fatal(err)
	}
	return now
}

func expired(t *testing.T, _ string) time.Time {
	t.Helper()
	return now.Add(validity + time.Hour)
}

func anotherCA(t *testing.T, dir string) time.Time {
	t.Helper()
	root, err := os.OpenRoot(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer root.Close()
	if err := root.WriteFile(CAFile, read(t, material(t), CAFile), 0o600); err != nil {
		t.Fatal(err)
	}
	return now
}
