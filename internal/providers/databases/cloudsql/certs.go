package cloudsql

import (
	"crypto"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/rsa"
	"crypto/sha1" //nolint:gosec // Cloud SQL names certificates by their SHA-1 fingerprint
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/pem"
	"errors"
	"fmt"
	"math/big"
	"net"
	"net/http"
	"regexp"
	"slices"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
)

// caLifetime and clientLifetime are the validity periods of the per-instance
// server CA and of client certificates (Cloud SQL: 10 years each).
const (
	caLifetime     = 10 * 365 * 24 * time.Hour
	clientLifetime = 10 * 365 * 24 * time.Hour
	kindSSLCert    = "sql#sslCert"
)

// authority is one instance's TLS material, created with the instance and
// never changed: the server CA clients verify, the server certificate the
// published listener presents, and the client CA that signs sslCerts. The
// keys stay in memory. GCP's server CA is RSA; these keys are ECDSA P-256,
// and the server certificate also names 127.0.0.1 so verify-full works locally.
type authority struct {
	ca       *x509.Certificate
	caPEM    string
	created  time.Time
	server   tls.Certificate
	clientCA *x509.Certificate
	clientKy crypto.Signer
}

// clientCert is one sslCerts entry; its private key was returned once.
type clientCert struct {
	CommonName, PEM, Serial, Fingerprint string
	Created, Expires                     time.Time
}

// newAuthority creates an instance's CA, server certificate, and client CA.
func newAuthority(project, instance string, now time.Time) (*authority, error) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate the server CA key: %w", err)
	}
	ca, caDER, err := selfSigned(caKey, "Google Cloud SQL Server CA", now)
	if err != nil {
		return nil, err
	}
	serverKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate the server key: %w", err)
	}
	leaf := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{CommonName: project + ":" + instance},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(caLifetime), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth}, IPAddresses: []net.IP{net.ParseIP(loopbackIPv4)},
		DNSNames: []string{"localhost"}}
	leafDER, err := x509.CreateCertificate(rand.Reader, leaf, ca, &serverKey.PublicKey, caKey)
	if err != nil {
		return nil, fmt.Errorf("sign the server certificate: %w", err)
	}
	clientKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("generate the client CA key: %w", err)
	}
	clientCA, _, err := selfSigned(clientKey, "Google Cloud SQL Client CA", now)
	if err != nil {
		return nil, err
	}
	return &authority{ca: ca, caPEM: pemBlock("CERTIFICATE", caDER), created: now,
		server:   tls.Certificate{Certificate: [][]byte{leafDER, caDER}, PrivateKey: serverKey},
		clientCA: clientCA, clientKy: clientKey}, nil
}

// selfSigned creates a CA certificate for key.
func selfSigned(key *ecdsa.PrivateKey, name string, now time.Time) (*x509.Certificate, []byte, error) {
	template := &x509.Certificate{SerialNumber: serial(), Subject: pkix.Name{Country: []string{"US"}, Organization: []string{"gcp-local"},
		CommonName: name}, NotBefore: now.Add(-time.Minute), NotAfter: now.Add(caLifetime), IsCA: true, BasicConstraintsValid: true,
		KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageCRLSign}
	der, err := x509.CreateCertificate(rand.Reader, template, template, &key.PublicKey, key)
	if err != nil {
		return nil, nil, fmt.Errorf("sign the %s: %w", name, err)
	}
	parsed, err := x509.ParseCertificate(der)
	if err != nil {
		return nil, nil, fmt.Errorf("parse the %s: %w", name, err)
	}
	return parsed, der, nil
}

// serial returns a random 128-bit certificate serial number.
func serial() *big.Int { return new(big.Int).SetBytes(randomBytes(16)) }

// pemBlock encodes one PEM block.
func pemBlock(kind string, der []byte) string {
	return string(pem.EncodeToMemory(&pem.Block{Type: kind, Bytes: der}))
}

// fingerprint is a certificate's SHA-1 fingerprint, the sslCerts key.
func fingerprint(der []byte) string {
	sum := sha1.Sum(der) //nolint:gosec // the API's identifier, not a security check
	return hex.EncodeToString(sum[:])
}

// issue signs a client certificate for commonName; Cloud SQL returns its
// RSA key in PKCS #1 form (observed live: "RSA PRIVATE KEY").
func (a *authority) issue(commonName string, now time.Time) (clientCert, string, error) {
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		return clientCert{}, "", fmt.Errorf("generate the client key: %w", err)
	}
	number := serial()
	template := &x509.Certificate{SerialNumber: number, Subject: pkix.Name{CommonName: commonName},
		NotBefore: now.Add(-time.Minute), NotAfter: now.Add(clientLifetime), KeyUsage: x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}}
	der, err := x509.CreateCertificate(rand.Reader, template, a.clientCA, &key.PublicKey, a.clientKy)
	if err != nil {
		return clientCert{}, "", fmt.Errorf("sign the client certificate: %w", err)
	}
	cert := clientCert{CommonName: commonName, PEM: pemBlock("CERTIFICATE", der), Serial: number.String(),
		Fingerprint: fingerprint(der), Created: now, Expires: template.NotAfter}
	return cert, pemBlock("RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(key)), nil
}

// caWire renders the server CA as an sql#sslCert. The instance and connect
// settings omit selfLink; sslCerts.insert includes it (observed live).
func (a *authority) caWire(project, instance string, link bool) map[string]any {
	value := map[string]any{keyKind: kindSSLCert, "cert": a.caPEM, "certSerialNumber": a.ca.SerialNumber.String(),
		"commonName": "C=US,O=gcp-local,CN=Google Cloud SQL Server CA", keyCreateTime: rfc3339(a.created),
		"expirationTime": rfc3339(a.ca.NotAfter), pathInstance: instance, "sha1Fingerprint": fingerprint(a.ca.Raw)}
	if link {
		value[keySelfLink] = instanceLink(project, instance) + "/sslCerts/" + fingerprint(a.ca.Raw)
	}
	return value
}

// wire renders a client certificate as an sql#sslCert.
func (c clientCert) wire(project, instance string) map[string]any {
	return map[string]any{keyKind: kindSSLCert, "cert": c.PEM, "certSerialNumber": c.Serial, "commonName": c.CommonName,
		keyCreateTime: rfc3339(c.Created), "expirationTime": rfc3339(c.Expires), pathInstance: instance,
		"sha1Fingerprint": c.Fingerprint, keySelfLink: instanceLink(project, instance) + "/sslCerts/" + c.Fingerprint}
}

// rfc3339 renders a time as the API does.
func rfc3339(value time.Time) string { return value.UTC().Format(time.RFC3339Nano) }

// commonNamePattern is the documented commonName alphabet ([a-zA-Z.-_ ]+),
// with digits, which GCP accepts (observed live: "gl-client").
var commonNamePattern = regexp.MustCompile(`^[a-zA-Z0-9.\-_ ]{1,64}$`)

// Refusals observed live (cloudsql-postgres-connectivity).
var errCertMissing = failure{code: http.StatusNotFound, reason: "sslCertificateDoesNotExist", domain: domainGlobal,
	message: "The SSL certificate does not exist."}

// sslCertBody is the SslCertsInsertRequest.
type sslCertBody struct {
	CommonName string `json:"commonName"`
}

// insertSSLCert issues a client certificate signed by the instance's client CA.
func (p *Provider) insertSSLCert(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	var body sslCertBody
	if err := provider.DecodeJSON(request.JSON, "body", &body, gap, provider.Fields{IgnoreUnknown: true}); err != nil {
		return nil, err
	}
	if !commonNamePattern.MatchString(body.CommonName) {
		return nil, invalid("Invalid request: Invalid common name: " + body.CommonName + ".")
	}
	name, started := request.Path[pathInstance], p.clock()
	var issued clientCert
	var key string
	var ca map[string]any
	err := p.mutate(call.Context, project, name, func(record *instanceRecord) error {
		if slices.ContainsFunc(record.Certs, func(c clientCert) bool { return c.CommonName == body.CommonName }) {
			return invalid("Invalid request: Common name already exists: " + body.CommonName + ".")
		}
		var err error
		issued, key, err = record.TLS.issue(body.CommonName, p.clock())
		if err != nil {
			return err
		}
		record.Certs = append(record.Certs, issued)
		ca = record.TLS.caWire(project, name, true)
		return nil
	})
	if err != nil {
		return nil, err
	}
	operation, err := p.complete(call, project, "UPDATE", name, started)
	if err != nil {
		return nil, err
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyKind: "sql#sslCertsInsert", "operation": operation.JSON,
		keyServerCaCert: ca, "clientCert": map[string]any{"certInfo": issued.wire(project, name), "certPrivateKey": key}}}, nil
}

// listSSLCerts lists the instance's client certificates.
func (p *Provider) listSSLCerts(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name := request.Path[pathInstance]
	record, ok := p.lookup(project, name)
	if !ok {
		return notFound(), nil
	}
	items := make([]any, 0, len(record.Certs))
	for _, cert := range record.Certs {
		items = append(items, cert.wire(project, name))
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyKind: "sql#sslCertsList", "items": items}}, nil
}

// getSSLCert reads one client certificate, or the server CA, by fingerprint.
func (p *Provider) getSSLCert(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name := request.Path[pathInstance]
	record, ok := p.lookup(project, name)
	if !ok {
		return notFound(), nil
	}
	wanted := request.Path["sha1Fingerprint"]
	for _, cert := range record.Certs {
		if cert.Fingerprint == wanted {
			return &gateway.RESTResponse{JSON: cert.wire(project, name)}, nil
		}
	}
	return nil, errCertMissing
}

// deleteSSLCert revokes one client certificate: the listener stops accepting it.
func (p *Provider) deleteSSLCert(call gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	name, started, wanted := request.Path[pathInstance], p.clock(), request.Path["sha1Fingerprint"]
	err := p.mutate(call.Context, project, name, func(record *instanceRecord) error {
		index := slices.IndexFunc(record.Certs, func(c clientCert) bool { return c.Fingerprint == wanted })
		if index < 0 {
			return errCertMissing
		}
		record.Certs = slices.Delete(record.Certs, index, index+1)
		return nil
	})
	if err != nil {
		return nil, err
	}
	return p.complete(call, project, "UPDATE", name, started)
}

// connectSettings returns what a connector needs: addresses and the server
// CA (fields of sql#connectSettings observed live).
func (p *Provider) connectSettings(_ gateway.Call, request *gateway.RESTRequest, project string) (*gateway.RESTResponse, error) {
	if request.Query.Get("readTime") != "" {
		return nil, gap("the connectSettings readTime parameter")
	}
	name := request.Path[pathInstance]
	record, ok := p.lookup(project, name)
	if !ok {
		return notFound(), nil
	}
	return &gateway.RESTResponse{JSON: map[string]any{keyKind: "sql#connectSettings", keyBackendType: secondGen,
		keyConnectionName: project + ":" + record.Region + ":" + name, keyDatabaseVersion: record.Version,
		keyIPAddresses: record.addresses(), keyRegion: record.Region, keyServerCaCert: record.TLS.caWire(project, name, false),
		"serverCaMode": serverCaMode}}, nil
}

// errNoAuthority reports an instance without TLS material.
var errNoAuthority = errors.New("the instance has no TLS material")

// serverConfig is the listener's TLS configuration for one instance: its
// server certificate, and client certificates verified against its client
// CA and its current sslCerts, so a deleted certificate stops working.
func (p *Provider) serverConfig(project, instance string, requireClient bool) (*tls.Config, error) {
	record, ok := p.lookup(project, instance)
	if !ok || record.TLS == nil {
		return nil, errNoAuthority
	}
	pool := x509.NewCertPool()
	pool.AddCert(record.TLS.clientCA)
	auth := tls.VerifyClientCertIfGiven
	if requireClient {
		auth = tls.RequireAndVerifyClientCert
	}
	return &tls.Config{MinVersion: tls.VersionTLS12, Certificates: []tls.Certificate{record.TLS.server}, ClientCAs: pool, ClientAuth: auth,
		// VerifyConnection also runs on resumed sessions, so a deleted
		// certificate can't come back through a session ticket.
		VerifyConnection: func(state tls.ConnectionState) error {
			if len(state.PeerCertificates) == 0 {
				return nil
			}
			current, _ := p.lookup(project, instance)
			if !slices.ContainsFunc(current.Certs, func(c clientCert) bool { return c.Fingerprint == fingerprint(state.PeerCertificates[0].Raw) }) {
				return errRevoked
			}
			return nil
		}}, nil
}

// errRevoked refuses a client certificate the instance no longer lists.
var errRevoked = errors.New("the client certificate is not one of the instance's sslCerts")
