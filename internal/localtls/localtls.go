// Package localtls loads or creates one environment's gRPC TLS material: a
// private CA certificate and the server certificate it signs.
//
// grpc-js and the handwritten Python Spanner, Bigtable, and Logging clients
// send call credentials only over TLS (#54). The CA's private key is discarded
// once it has signed the server certificate, so trusting ca.pem authenticates
// only that certificate. The CA is also name-constrained to localhost and the
// packaged gateway host. BoringSSL (gRPC C-core) refuses IP address
// constraints, so the loopback IP names are not constrained.
package localtls

import (
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"net"
	"os"
	"time"
)

// File names inside the TLS directory. Clients trust CAFile; the key never
// leaves the directory.
const (
	CAFile   = "ca.pem"
	CertFile = "server.pem"
	KeyFile  = "server-key.pem"
)

// gatewayHost is the packaged core's Compose service name, which Cloud Run
// revision containers dial.
const gatewayHost = "gcp-local"

// validity stays within the 825-day limit some platform verifiers apply to
// TLS server certificates.
const validity = 825 * 24 * time.Hour

var (
	files = []string{CAFile, CertFile, KeyFile}
	hosts = []string{"localhost", gatewayHost}
)

var (
	errDirectory  = errors.New("the gRPC TLS directory must be an owner-only (0700) directory, not a symlink")
	errFileMode   = errors.New("gRPC TLS files must be regular owner-only (0600) files")
	errIncomplete = errors.New("the gRPC TLS directory is incomplete; delete it to create new material")
	errExpired    = errors.New("the gRPC TLS server certificate is not valid now; delete its directory to create new material")
	errMismatch   = errors.New("the gRPC TLS server certificate is not signed by the directory's CA")
)

// Load returns the server TLS configuration from dir. A missing or empty dir
// gets a new CA and server certificate; a partial one is refused.
func Load(dir string, now time.Time) (*tls.Config, error) {
	if err := ensureDirectory(dir); err != nil {
		return nil, err
	}
	root, err := os.OpenRoot(dir)
	if err != nil {
		return nil, fmt.Errorf("open the gRPC TLS directory: %w", err)
	}
	defer root.Close()
	count, err := present(root)
	switch {
	case err != nil:
		return nil, err
	case count == 0:
		if err := create(root, now); err != nil {
			return nil, err
		}
	case count != len(files):
		return nil, errIncomplete
	}
	return load(root, now)
}

// ensureDirectory creates dir (mode 0700) or requires an existing owner-only directory.
func ensureDirectory(dir string) error {
	info, err := os.Lstat(dir)
	if errors.Is(err, fs.ErrNotExist) {
		if err := os.Mkdir(dir, 0o700); err != nil {
			return fmt.Errorf("create the gRPC TLS directory: %w", err)
		}
		return nil
	}
	if err != nil {
		return fmt.Errorf("inspect the gRPC TLS directory: %w", err)
	}
	if !info.IsDir() || info.Mode().Perm()&0o077 != 0 {
		return errDirectory
	}
	return nil
}

// present counts the material files in root, each a regular owner-only file.
func present(root *os.Root) (int, error) {
	count := 0
	for _, name := range files {
		info, err := root.Lstat(name)
		if errors.Is(err, fs.ErrNotExist) {
			continue
		}
		if err != nil {
			return 0, fmt.Errorf("inspect %s: %w", name, err)
		}
		if !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
			return 0, errFileMode
		}
		count++
	}
	return count, nil
}

// load reads the server key pair and requires it to chain to ca.pem now.
func load(root *os.Root, now time.Time) (*tls.Config, error) {
	var contents [3][]byte
	for index, name := range files {
		data, err := root.ReadFile(name)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", name, err)
		}
		contents[index] = data
	}
	pair, err := tls.X509KeyPair(contents[1], contents[2])
	if err != nil {
		return nil, fmt.Errorf("load the gRPC TLS key pair: %w", err)
	}
	if now.Before(pair.Leaf.NotBefore) || now.After(pair.Leaf.NotAfter) {
		return nil, errExpired
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(contents[0]) {
		return nil, errMismatch
	}
	if _, err := pair.Leaf.Verify(x509.VerifyOptions{Roots: roots, CurrentTime: now, DNSName: "localhost"}); err != nil {
		return nil, fmt.Errorf("%w: %w", errMismatch, err)
	}
	// gRPC clients require HTTP/2 negotiated by ALPN.
	return &tls.Config{Certificates: []tls.Certificate{pair}, NextProtos: []string{"h2"}, MinVersion: tls.VersionTLS12}, nil
}

// create writes a new name-constrained CA certificate and a server certificate
// it signs. The CA key exists only in memory here.
func create(root *os.Root, now time.Time) error {
	start, end := now.Add(-time.Hour), now.Add(validity)
	caDER, caKey, err := issue(&x509.Certificate{
		Subject:   pkix.Name{CommonName: "gcp-local environment CA"},
		NotBefore: start, NotAfter: end,
		KeyUsage:              x509.KeyUsageCertSign,
		BasicConstraintsValid: true, IsCA: true, MaxPathLenZero: true,
		PermittedDNSDomainsCritical: true,
		PermittedDNSDomains:         hosts,
	}, nil, nil)
	if err != nil {
		return err
	}
	authority, err := x509.ParseCertificate(caDER)
	if err != nil {
		return fmt.Errorf("parse the CA certificate: %w", err)
	}
	serverDER, serverKey, err := issue(&x509.Certificate{
		Subject:   pkix.Name{CommonName: gatewayHost},
		NotBefore: start, NotAfter: end,
		KeyUsage:    x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},
		DNSNames:    hosts,
		IPAddresses: []net.IP{net.IPv4(127, 0, 0, 1), net.IPv6loopback},
	}, authority, caKey)
	if err != nil {
		return err
	}
	keyDER, err := x509.MarshalPKCS8PrivateKey(serverKey)
	if err != nil {
		return fmt.Errorf("encode the server key: %w", err)
	}
	// The CA certificate is written last.
	for _, file := range []struct {
		name, kind string
		der        []byte
	}{{KeyFile, "PRIVATE KEY", keyDER}, {CertFile, "CERTIFICATE", serverDER}, {CAFile, "CERTIFICATE", caDER}} {
		if err := writePrivate(root, file.name, pem.EncodeToMemory(&pem.Block{Type: file.kind, Bytes: file.der})); err != nil {
			return err
		}
	}
	return nil
}

// issue generates a key and a certificate for it, signed by parent's key, or
// self-signed when parent is nil.
func issue(template, parent *x509.Certificate, parentKey *ecdsa.PrivateKey) ([]byte, *ecdsa.PrivateKey, error) {
	key, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, nil, fmt.Errorf("generate a key: %w", err)
	}
	if parent == nil {
		parent, parentKey = template, key
	}
	if template.SerialNumber, err = rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128)); err != nil {
		return nil, nil, fmt.Errorf("generate a certificate serial: %w", err)
	}
	der, err := x509.CreateCertificate(rand.Reader, template, parent, &key.PublicKey, parentKey)
	if err != nil {
		return nil, nil, fmt.Errorf("create the %q certificate: %w", template.Subject.CommonName, err)
	}
	return der, key, nil
}

// writePrivate creates one new owner-only file and refuses to overwrite.
func writePrivate(root *os.Root, name string, content []byte) error {
	file, err := root.OpenFile(name, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("create %s: %w", name, err)
	}
	_, err = file.Write(content)
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write %s: %w", name, err)
	}
	return nil
}
