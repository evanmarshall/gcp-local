package cloudsql

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"net/http"
	"slices"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/mysqlwire"
	"github.com/evanmarshall/gcp-local/internal/providers/databases/cloudsql/server/pgwire"
	"github.com/evanmarshall/gcp-local/internal/providers/provider/providertest"
)

// depthBody is the test insert body with backups, a flag, REGIONAL
// availability, ENCRYPTED_ONLY, and one authorized network.
func depthBody() map[string]any {
	return withSettings(map[string]any{"availabilityType": availabilityRegional,
		"backupConfiguration": map[string]any{"enabled": true, "startTime": "03:00", "pointInTimeRecoveryEnabled": true,
			"location": "us", "transactionLogRetentionDays": 3,
			"backupRetentionSettings": map[string]any{"retainedBackups": 5, "retentionUnit": "COUNT"}},
		"databaseFlags":   []any{map[string]any{"name": "max_connections", "value": "60"}},
		"ipConfiguration": map[string]any{"sslMode": sslEncrypted, "authorizedNetworks": []any{map[string]any{"value": "127.0.0.0/8", "name": "loopback"}}}})
}

// TestSettingsRoundTrip stores and returns backups, availability, TLS mode,
// networks, and flags as requested, starts the server with the flags, and
// patches them with the recorded merge and replace semantics.
func TestSettingsRoundTrip(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, depthBody()), http.StatusOK)
	instance := h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)
	subset(t, "instance", instance, map[string]any{
		"settings.availabilityType": availabilityRegional, "secondaryGceZone": "us-central1-b", "failoverReplica.available": true,
		"settings.backupConfiguration.enabled": true, "settings.backupConfiguration.startTime": "03:00",
		"settings.backupConfiguration.pointInTimeRecoveryEnabled": true, "settings.backupConfiguration.location": "us",
		"settings.backupConfiguration.transactionLogRetentionDays": 3, "settings.backupConfiguration.backupRetentionSettings.retainedBackups": 5,
		"settings.ipConfiguration.sslMode": sslEncrypted, "settings.ipConfiguration.requireSsl": false,
		"settings.ipConfiguration.serverCaMode": serverCaMode, "settings.ipConfiguration.authorizedNetworks.#": 1,
		"settings.ipConfiguration.authorizedNetworks.0.kind": "sql#aclEntry", "settings.ipConfiguration.authorizedNetworks.0.name": "loopback",
		"serverCaCert.kind": kindSSLCert, "serverCaCert.selfLink": nil,
	})
	if got := h.fleet().flags[testInstance]; !slices.Equal(got, []server.Flag{{Name: "max_connections", Value: "60"}}) {
		t.Fatalf("server flags = %v", got)
	}
}

// TestSettingsPatch merges named backup and IP fields, replaces flags and
// authorized networks whole, and restarts the server with the new flags.
func TestSettingsPatch(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, depthBody()), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"backupConfiguration": map[string]any{"transactionLogRetentionDays": 5},
		"ipConfiguration":     map[string]any{"requireSsl": true},
		"databaseFlags":       []any{map[string]any{"name": "log_min_duration_statement", "value": "500"}}})), http.StatusOK)
	subset(t, "patched settings", h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings], map[string]any{
		"backupConfiguration.transactionLogRetentionDays": 5, "backupConfiguration.location": "us",
		"ipConfiguration.requireSsl": true, "ipConfiguration.sslMode": sslTrustedCerts, "ipConfiguration.authorizedNetworks.#": 1,
		"databaseFlags.#": 1, "databaseFlags.0.name": "log_min_duration_statement",
	})
	if !slices.Equal(h.fleet().flags[testInstance], []server.Flag{{Name: "log_min_duration_statement", Value: "500"}}) {
		t.Fatalf("flags were not applied: %v", h.fleet().flags[testInstance])
	}
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"ipConfiguration": map[string]any{"authorizedNetworks": []any{
			map[string]any{"value": "203.0.113.0/24"}, map[string]any{"value": "198.51.100.17/32"}, map[string]any{"value": "192.0.2.0/28"}}}})), http.StatusOK)
	subset(t, "networks", h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings], map[string]any{
		"ipConfiguration.authorizedNetworks.#": 3, "ipConfiguration.authorizedNetworks.0.value": "203.0.113.0/24",
		"ipConfiguration.authorizedNetworks.2.value": "192.0.2.0/28",
	})
	// Entries the instance had move after the new ones (recorded).
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"ipConfiguration": map[string]any{"authorizedNetworks": []any{
			map[string]any{"value": "203.0.113.0/24"}, map[string]any{"value": "10.0.0.0/8"}}}})), http.StatusOK)
	subset(t, "reordered", h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings], map[string]any{
		"ipConfiguration.authorizedNetworks.0.value": "10.0.0.0/8", "ipConfiguration.authorizedNetworks.1.value": "203.0.113.0/24",
	})
}

// TestSettingsRefusals covers the recorded validation errors and the gaps
// that remain, and that none of them starts a backend.
func TestSettingsRefusals(t *testing.T) {
	h := newHarness(t)
	refusals := []struct {
		code     int
		settings map[string]any
		message  string
	}{
		{http.StatusBadRequest, map[string]any{"edition": "ENTERPRISE_PLUS"}, "Invalid request: Invalid Tier (db-f1-micro) for (ENTERPRISE_PLUS) Edition. " +
			"Use a predefined Tier like db-perf-optimized-N-* instead. Learn more at https://cloud.google.com/sql/docs/postgres/create-instance#machine-types."},
		{http.StatusBadRequest, map[string]any{"availabilityType": "BOGUS"},
			`Invalid value at 'body.settings.availability_type' (type.googleapis.com/google.cloud.sql.v1beta4.SqlAvailabilityType), "BOGUS"`},
		{http.StatusBadRequest, map[string]any{"ipConfiguration": map[string]any{"sslMode": "BOGUS"}},
			`Invalid value at 'body.settings.ip_configuration.ssl_mode' (type.googleapis.com/google.cloud.sql.v1beta4.IpConfiguration.SslMode), "BOGUS"`},
		{http.StatusBadRequest, map[string]any{"ipConfiguration": map[string]any{"authorizedNetworks": []any{map[string]any{"value": "not-a-cidr"}}}},
			"Invalid request: Network (not-a-cidr).."},
		{http.StatusBadRequest, map[string]any{"ipConfiguration": map[string]any{"authorizedNetworks": []any{map[string]any{"value": "203.0.113.7/24"}}}},
			"Invalid request: Network (203.0.113.7/24).."},
		{http.StatusNotImplemented, map[string]any{"databaseFlags": []any{map[string]any{"name": "cloudsql.iam_authentication", "value": "on"}}},
			"Cloud SQL coverage gap: database flag cloudsql.iam_authentication"},
		{http.StatusNotImplemented, map[string]any{"ipConfiguration": map[string]any{"serverCaMode": "GOOGLE_MANAGED_CAS_CA"}},
			"Cloud SQL coverage gap: settings.ipConfiguration.serverCaMode GOOGLE_MANAGED_CAS_CA"},
		{http.StatusNotImplemented, map[string]any{"backupConfiguration": map[string]any{"replicationLogArchivingEnabled": true}},
			"Cloud SQL coverage gap: settings.backupConfiguration.replicationLogArchivingEnabled without PostgreSQL point-in-time recovery"},
		{http.StatusNotImplemented, map[string]any{"maintenanceWindow": map[string]any{"day": 1, "hour": 3}},
			"Cloud SQL coverage gap: settings.maintenanceWindow is not modeled"},
	}
	for _, refusal := range refusals {
		h.expectError(http.MethodPost, testInstances, withSettings(refusal.settings), refusal.code, refusal.message)
	}
	if h.fleet() != nil {
		t.Fatal("a refused insert started a backend")
	}
	h.createInstance()
	h.expectError(http.MethodPost, testPath+"/failover", map[string]any{}, http.StatusNotImplemented,
		"Cloud SQL coverage gap: instances.failover (REGIONAL availability is stored, but one local server serves the instance and there is no standby)")
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"ipConfiguration": map[string]any{"ipv4Enabled": false}}),
		http.StatusNotImplemented, "Cloud SQL coverage gap: instances.patch field settings.ipConfiguration.ipv4Enabled")
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"tier": smallTier,
		"databaseFlags": []any{map[string]any{"name": "max_connections", "value": "1"}}}), http.StatusBadRequest,
		"Value requested is not valid. Failed to set max_connections: 1 is not between 14 and 262143.")
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"databaseFlags": []any{map[string]any{"name": "gl_not_a_flag", "value": "1"}}}),
		http.StatusNotFound, "The requested flag is either misspelled or unsupported by Cloud SQL.")
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"databaseFlags": []any{map[string]any{"name": "max_connections", "value": "60"},
		map[string]any{"name": "max_connections", "value": "70"}}}), http.StatusBadRequest, "Invalid request: Duplicate flags.")
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"databaseFlags": []any{map[string]any{"name": "log_min_duration_statement", "value": "soon"}}}),
		http.StatusBadRequest, "Value requested is not valid. Failed to set log_min_duration_statement: soon is not a number.")
	h.expectError(http.MethodPatch, testPath, settingsBodyOf(map[string]any{"ipConfiguration": map[string]any{"sslMode": sslEncrypted, "requireSsl": true}}),
		http.StatusBadRequest, "Invalid request: For a Postgres instance, sslMode value ENCRYPTED_ONLY and requireSsl value true are conflicting. "+
			"When sslMode=ENCRYPTED_ONLY, requireSsl must be false. When requireSsl=true, sslMode must be TRUSTED_CLIENT_CERTIFICATE_REQUIRED. "+
			"It's recommended that you only set sslMode.")
	h.createMySQL()
	for flag, message := range map[string]string{"slow_query_log": "maybe is not on/off.", "long_query_time": "-1.000000 is not between 0.000000 and 30000000.000000."} {
		value := map[string]string{"slow_query_log": "maybe", "long_query_time": "-1"}[flag]
		h.expectError(http.MethodPatch, testMySQLPath, settingsBodyOf(map[string]any{"databaseFlags": []any{map[string]any{"name": flag, "value": value}}}),
			http.StatusBadRequest, "Value requested is not valid. Failed to set "+flag+": "+message)
	}
	subset(t, "refused patch", h.expect(h.invoke(adminToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings],
		map[string]any{"tier": "db-f1-micro", keySettingsVersion: "1", "databaseFlags": nil})
}

// TestBackupRunsAndRestore takes, lists, reads, restores, and deletes
// on-demand backup runs through the fleet.
func TestBackupRunsAndRestore(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/backupRuns", map[string]any{}), http.StatusNotFound)
	h.createInstance()
	runs := testPath + "/backupRuns"
	h.expect(h.invoke(viewerToken, http.MethodPost, runs, map[string]any{}), http.StatusForbidden)
	operation := h.expect(h.invoke(adminToken, http.MethodPost, runs, map[string]any{"description": "first", "type": "AUTOMATED"}), http.StatusOK)
	subset(t, "backup operation", operation, map[string]any{"operationType": "BACKUP_VOLUME", "backupContext.kind": "sql#backupContext"})
	id, _ := object(t, operation["backupContext"])["backupId"].(string)
	second, _ := object(t, h.expect(h.invoke(adminToken, http.MethodPost, runs, map[string]any{}), http.StatusOK)["backupContext"])["backupId"].(string)
	subset(t, "backup runs", h.expect(h.invoke(viewerToken, http.MethodGet, runs, nil), http.StatusOK), map[string]any{
		keyKind: "sql#backupRunsList", "items.#": 2, "items.0.id": second, "items.1.description": "first"})
	subset(t, "backup run", h.expect(h.invoke(viewerToken, http.MethodGet, runs+"/"+id, nil), http.StatusOK), map[string]any{
		keyKind: "sql#backupRun", "type": "ON_DEMAND", "status": "SUCCESSFUL", "databaseVersion": testVersion})
	h.expectError(http.MethodGet, runs+"/1", nil, http.StatusNotFound, "The backup run does not exist.")
	h.expectError(http.MethodPost, testPath+"/restoreBackup", map[string]any{}, http.StatusBadRequest, "Invalid request: Missing parameter: Backup name , BackupDr Backup name and RestoreBackupContext, all cannot be empty..")
	h.expectError(http.MethodPost, testPath+"/restoreBackup", map[string]any{"restoreBackupContext": map[string]any{"backupRunId": id, "project": peerProject}},
		http.StatusNotImplemented, "Cloud SQL coverage gap: restoreBackup from another project")
	restore := map[string]any{"restoreBackupContext": map[string]any{"backupRunId": id, "instanceId": testInstance, "project": testProject}}
	h.expect(h.invoke(viewerToken, http.MethodPost, testPath+"/restoreBackup", restore), http.StatusForbidden)
	subset(t, "restore", h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/restoreBackup", restore), http.StatusOK),
		map[string]any{"operationType": "RESTORE_VOLUME"})
	copyBody := insertBody()
	copyBody[keyName] = "demo-copy"
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, copyBody), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances+"/demo-copy/restoreBackup", restore), http.StatusOK)
	if got := h.fleet().restores; !slices.Equal(got, []string{testInstance + "/" + id + ">" + testInstance, testInstance + "/" + id + ">demo-copy"}) {
		t.Fatalf("restores = %v", got)
	}
	subset(t, "delete", h.expect(h.invoke(adminToken, http.MethodDelete, runs+"/"+id, nil), http.StatusOK), map[string]any{"operationType": "DELETE_BACKUP"})
	h.expectError(http.MethodDelete, runs+"/"+id, nil, http.StatusNotFound, "The backup run does not exist.")
	subset(t, "deleted run", h.expect(h.invoke(viewerToken, http.MethodGet, runs+"/"+id, nil), http.StatusOK), map[string]any{"status": "DELETED", "endTime": nil})
	h.expectError(http.MethodPost, testPath+"/restoreBackup", restore, http.StatusBadRequest, "Invalid request: Backup run does not exist..")
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath, nil), http.StatusOK)
	h.createInstance()
	subset(t, "recreated", h.expect(h.invoke(viewerToken, http.MethodGet, runs, nil), http.StatusOK), map[string]any{"items": nil})
}

// TestSSLCertsAndConnectSettings issues, lists, reads, refuses a duplicate
// of, and deletes a client certificate, and returns the CA in connect settings.
func TestSSLCertsAndConnectSettings(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	certs := testPath + "/sslCerts"
	h.expect(h.invoke(viewerToken, http.MethodPost, certs, map[string]any{"commonName": "app"}), http.StatusForbidden)
	inserted := h.expect(h.invoke(adminToken, http.MethodPost, certs, map[string]any{"commonName": "app"}), http.StatusOK)
	subset(t, "insert", inserted, map[string]any{keyKind: "sql#sslCertsInsert", "operation.operationType": "UPDATE",
		"clientCert.certInfo.commonName": "app", "clientCert.certInfo.kind": kindSSLCert})
	client := object(t, inserted["clientCert"])
	if key, _ := client["certPrivateKey"].(string); !strings.HasPrefix(key, "-----BEGIN RSA PRIVATE KEY-----") || object(t, inserted["serverCaCert"])[keySelfLink] == nil {
		t.Fatalf("insert = %v", inserted)
	}
	info := object(t, client["certInfo"])
	fingerprint, _ := info["sha1Fingerprint"].(string)
	h.expectError(http.MethodPost, certs, map[string]any{"commonName": "app"}, http.StatusBadRequest, "Invalid request: Common name already exists: app.")
	subset(t, "list", h.expect(h.invoke(viewerToken, http.MethodGet, certs, nil), http.StatusOK), map[string]any{keyKind: "sql#sslCertsList", "items.#": 1})
	subset(t, "read", h.expect(h.invoke(viewerToken, http.MethodGet, certs+"/"+fingerprint, nil), http.StatusOK), map[string]any{"cert": info["cert"]})
	instance := h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)
	subset(t, "connect settings", h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/connectSettings", nil), http.StatusOK), map[string]any{
		keyKind: "sql#connectSettings", "serverCaCert.cert": object(t, instance["serverCaCert"])["cert"], "serverCaMode": serverCaMode})
	h.expect(h.invoke(adminToken, http.MethodDelete, certs+"/"+fingerprint, nil), http.StatusOK)
	h.expectError(http.MethodGet, certs+"/"+fingerprint, nil, http.StatusNotFound, "The SSL certificate does not exist.")
	h.expectError(http.MethodDelete, certs+"/"+fingerprint, nil, http.StatusNotFound, "The SSL certificate does not exist.")
}

// publishedTLS starts TLS on the published port as libpq does and returns
// the TLS connection, or the handshake error.
func publishedTLS(t *testing.T, address string, config *tls.Config) (*tls.Conn, error) {
	t.Helper()
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", address)
	must(t, err)
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, err = conn.Write(binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), pgwire.SSLRequestCode))
	must(t, err)
	answer := make([]byte, 1)
	_, err = io.ReadFull(conn, answer)
	must(t, err)
	if answer[0] != 'S' {
		t.Fatalf("SSL answer = %q", answer)
	}
	secure := tls.Client(conn, config)
	if err := secure.HandshakeContext(t.Context()); err != nil {
		_ = conn.Close()
		return nil, err //nolint:wrapcheck // the test inspects it
	}
	return secure, nil
}

// trustCA returns a TLS client configuration that trusts an instance's CA,
// with an optional client certificate.
func (h *harness) trustCA(name string, cert map[string]any) *tls.Config {
	h.t.Helper()
	instance := h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/"+name, nil), http.StatusOK)
	ca, _ := object(h.t, instance["serverCaCert"])["cert"].(string)
	return clientTLS(h.t, ca, cert)
}

// TestPublishedTLS terminates TLS on the published port with the
// instance's certificate, enforces ENCRYPTED_ONLY and trusted client
// certificates, and forwards the decrypted startup packet.
func TestPublishedTLS(t *testing.T) {
	h := newHarness(t)
	h.provider.pool = onePortPool(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, withSettings(map[string]any{
		"ipConfiguration": map[string]any{"sslMode": sslEncrypted}})), http.StatusOK)
	address, fake := h.publishedAddress(testInstance), h.fleet().server(testInstance)
	if reply := dialStartup(t, address, defaultUser, false); !bytes.Contains(reply, []byte("requires TLS")) {
		t.Fatalf("cleartext reply = %q", reply)
	}
	secure, err := publishedTLS(t, address, h.trustCA(testInstance, nil))
	must(t, err)
	_, err = secure.Write(startupPacket(defaultUser))
	must(t, err)
	select {
	case got := <-fake.received:
		if !bytes.Equal(got, startupPacket(defaultUser)) {
			t.Fatalf("forwarded = %q", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the decrypted startup packet was not forwarded")
	}
	_ = secure.Close()

	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"ipConfiguration": map[string]any{"sslMode": sslTrustedCerts}})), http.StatusOK)
	if conn, err := publishedTLS(t, address, h.trustCA(testInstance, nil)); err == nil {
		// TLS 1.3 reports the missing certificate on the first read.
		if _, err = conn.Read(make([]byte, 1)); err == nil {
			t.Fatal("a connection without a client certificate was admitted")
		}
	}
	inserted := h.expect(h.invoke(adminToken, http.MethodPost, testPath+"/sslCerts", map[string]any{"commonName": "app"}), http.StatusOK)
	secure, err = publishedTLS(t, address, h.trustCA(testInstance, object(t, inserted["clientCert"])))
	must(t, err)
	_ = secure.Close()

	pool := x509.NewCertPool()
	other := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: pool, ServerName: loopbackIPv4}
	if _, err := publishedTLS(t, address, other); err == nil {
		t.Fatal("the server certificate verified against an empty root pool")
	}
}

// TestAuthorizedNetworks refuses published-port connections from outside the
// instance's authorized networks in each engine's protocol.
func TestAuthorizedNetworks(t *testing.T) {
	h := newHarness(t)
	h.provider.pool = onePortPool(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, withSettings(map[string]any{
		"ipConfiguration": map[string]any{"authorizedNetworks": []any{map[string]any{"value": "10.0.0.0/8"}}}})), http.StatusOK)
	address := h.publishedAddress(testInstance)
	if reply := dialStartup(t, address, defaultUser, false); reply[0] != 'E' || !bytes.Contains(reply, []byte("127.0.0.1 is not in the authorized networks")) {
		t.Fatalf("refusal = %q", reply)
	}
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"ipConfiguration": map[string]any{"authorizedNetworks": []any{map[string]any{"value": "127.0.0.1"}}}})), http.StatusOK)
	dialStartup(t, address, defaultUser, false)
	select {
	case <-h.fleet().server(testInstance).received:
	case <-time.After(5 * time.Second):
		t.Fatal("an authorized source was not forwarded")
	}
	h.expect(h.invoke(adminToken, http.MethodDelete, testPath, nil), http.StatusOK)

	body := mysqlInsertBody()
	object(t, body[keySettings])["ipConfiguration"] = map[string]any{"authorizedNetworks": []any{map[string]any{"value": "192.0.2.0/24"}}}
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	refused, _ := dialMySQLHandshake(t, h.publishedAddress(testMySQL), mysqlRootUser)
	if failure := mysqlwire.ParseError(refused); failure == nil || !strings.Contains(failure.Error(), "ERROR 1130") {
		t.Fatalf("MySQL refusal = %v", failure)
	}
}

// TestMySQLPublishedTLS terminates a MySQL client's TLS on the published
// port and forwards its handshake response renumbered and without CLIENT_SSL.
func TestMySQLPublishedTLS(t *testing.T) {
	h := newHarness(t)
	h.provider.pool = onePortPool(t)
	body := mysqlInsertBody()
	object(t, body[keySettings])["ipConfiguration"] = map[string]any{"sslMode": sslEncrypted}
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	address, fake := h.publishedAddress(testMySQL), h.fleet().server(testMySQL)
	_, reply := dialMySQLHandshake(t, address, mysqlRootUser)
	if failure := mysqlwire.ParseError(reply[4:]); failure == nil || !strings.Contains(failure.Error(), "ERROR 3159") {
		t.Fatalf("cleartext reply = %v", failure)
	}
	conn, err := (&net.Dialer{Timeout: time.Second}).DialContext(t.Context(), "tcp", address)
	must(t, err)
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(5 * time.Second))
	_, _, err = mysqlwire.ReadPacket(conn)
	must(t, err)
	flags := uint32(clientFlags | mysqlwire.CapSSL)
	request := append(binary.LittleEndian.AppendUint32(binary.LittleEndian.AppendUint32(nil, flags), mysqlwire.MaxPacket), mysqlwire.CharsetUTF8MB4)
	_, err = conn.Write(mysqlwire.Packet(1, append(request, make([]byte, 23)...)))
	must(t, err)
	secure := tls.Client(conn, h.trustCA(testMySQL, nil))
	must(t, secure.HandshakeContext(t.Context()))
	_, err = secure.Write(mysqlwire.Packet(2, mysqlHandshakeResponse(mysqlRootUser, flags)))
	must(t, err)
	expectForwarded(t, fake, mysqlwire.Packet(1, mysqlHandshakeResponse(mysqlRootUser, clientFlags)))
}

// TestConcurrentDepthRequests races backup runs, certificates, and patches
// on one instance under -race.
func TestConcurrentDepthRequests(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	providertest.Hammer(t, h.gate, adminToken, 6, 10, func(int) []providertest.Request {
		return []providertest.Request{
			{Method: http.MethodPost, Path: testPath + "/backupRuns", Body: map[string]any{}, Want: []int{200}},
			{Method: http.MethodGet, Path: testPath + "/backupRuns", Want: []int{200}},
			{Method: http.MethodPost, Path: testPath + "/sslCerts", Body: map[string]any{"commonName": "app"}, Want: []int{200, 400}},
			{Method: http.MethodGet, Path: testPath + "/sslCerts", Want: []int{200}},
			{Method: http.MethodPatch, Path: testPath, Body: settingsBodyOf(map[string]any{
				"ipConfiguration": map[string]any{"authorizedNetworks": []any{map[string]any{"value": "10.0.0.0/8"}}}}), Want: []int{200}},
			{Method: http.MethodGet, Path: testPath + "/connectSettings", Want: []int{200}},
		}
	})
}

// clientTLS trusts an instance's server CA, as sslmode=verify-full does
// against 127.0.0.1, with an optional client certificate from sslCerts.
func clientTLS(t *testing.T, ca string, cert map[string]any) *tls.Config {
	t.Helper()
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM([]byte(ca)) {
		t.Fatal("the server CA is not PEM")
	}
	config := &tls.Config{MinVersion: tls.VersionTLS12, RootCAs: roots, ServerName: "127.0.0.1"}
	if cert != nil {
		info := object(t, cert["certInfo"])
		certPEM, _ := info["cert"].(string)
		keyPEM, _ := cert["certPrivateKey"].(string)
		pair, err := tls.X509KeyPair([]byte(certPEM), []byte(keyPEM))
		must(t, err)
		config.Certificates = []tls.Certificate{pair}
	}
	return config
}

// TestAutomatedBackups takes an AUTOMATED backup at each daily window on
// an injected clock and keeps only retainedBackups of them.
func TestAutomatedBackups(t *testing.T) {
	h := newHarness(t)
	now := time.Date(2026, 10, 13, 2, 30, 0, 0, time.UTC)
	ticks, waits := make(chan time.Time), make(chan time.Duration, 4)
	h.provider.clock = func() time.Time { return now }
	h.provider.after = func(d time.Duration) <-chan time.Time { waits <- d; return ticks }
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, withSettings(map[string]any{
		"backupConfiguration": map[string]any{"enabled": true, "startTime": "03:00",
			"backupRetentionSettings": map[string]any{"retainedBackups": 1}}})), http.StatusOK)
	if wait := <-waits; wait != 30*time.Minute {
		t.Fatalf("first wait = %v", wait)
	}
	ticks <- now
	if wait := <-waits; wait != 30*time.Minute {
		t.Fatalf("the worker did not wait for the next window: %v", wait)
	}
	listed := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/backupRuns", nil), http.StatusOK)
	subset(t, "automated runs", listed, map[string]any{"items.#": 1, "items.0.type": "AUTOMATED", "items.0.windowStartTime": "2026-10-13T03:00:00Z"})
	first := object(t, items(t, listed)[0])["id"]
	ticks <- now
	<-waits
	listed = h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/backupRuns", nil), http.StatusOK)
	if count(listed) != 1 || object(t, items(t, listed)[0])["id"] == first || len(h.fleet().backups[testInstance]) != 1 {
		t.Fatalf("retention kept %v (fleet %v)", listed, h.fleet().backups[testInstance])
	}
	kept := object(t, items(t, listed)[0])["id"]
	h.expect(h.invoke(adminToken, http.MethodPatch, testPath, settingsBodyOf(map[string]any{
		"backupConfiguration": map[string]any{"enabled": false}})), http.StatusOK)
	assertStopped(t, h, ticks, waits, now, kept)
}

// assertStopped checks that a disabled backup schedule takes no backup and
// waits for no further window. A stopped worker may still take a tick it was
// already selecting on (select picks among ready cases at random).
func assertStopped(t *testing.T, h *harness, ticks chan time.Time, waits chan time.Duration, now time.Time, kept any) {
	t.Helper()
	select {
	case ticks <- now:
	case <-time.After(100 * time.Millisecond):
	}
	select {
	case wait := <-waits:
		t.Fatalf("a disabled schedule still runs: waited %v", wait)
	case <-time.After(100 * time.Millisecond):
	}
	listed := h.expect(h.invoke(viewerToken, http.MethodGet, testPath+"/backupRuns", nil), http.StatusOK)
	if count(listed) != 1 || object(t, items(t, listed)[0])["id"] != kept {
		t.Fatalf("a disabled schedule took a backup: %v", listed)
	}
}

// TestInstanceUpdate replaces the lists an update (PUT) omits, which a
// patch keeps, and refuses a final backup on delete.
func TestInstanceUpdate(t *testing.T) {
	h := newHarness(t)
	h.expect(h.invoke(adminToken, http.MethodPost, testInstances, depthBody()), http.StatusOK)
	h.expect(h.invoke(adminToken, http.MethodPut, testPath, settingsBodyOf(map[string]any{"tier": smallTier})), http.StatusOK)
	subset(t, "updated settings", h.expect(h.invoke(viewerToken, http.MethodGet, testPath, nil), http.StatusOK)[keySettings], map[string]any{
		"tier": smallTier, "databaseFlags": nil, "userLabels": nil, "ipConfiguration.authorizedNetworks": []any{},
		"ipConfiguration.sslMode": sslEncrypted, "backupConfiguration.enabled": true})
	if len(h.fleet().flags[testInstance]) != 0 {
		t.Fatalf("update kept the server flags: %v", h.fleet().flags[testInstance])
	}
	h.expectError(http.MethodDelete, testPath+"?enableFinalBackup=true", nil, http.StatusNotImplemented,
		"Cloud SQL coverage gap: instances.delete enableFinalBackup (final backups are not modeled)")
}

// subset asserts that each dotted path of want (a list index, or # for a
// list's length) holds that JSON value in got.
func subset(t *testing.T, label string, got any, want map[string]any) {
	t.Helper()
	for path, value := range want {
		have, err := json.Marshal(walk(got, strings.Split(path, ".")))
		must(t, err)
		expected, err := json.Marshal(value)
		must(t, err)
		if string(have) != string(expected) {
			t.Errorf("%s: %s = %s, want %s", label, path, have, expected)
		}
	}
}

// walk follows a dotted path through decoded JSON; a missing step is nil.
func walk(value any, path []string) any {
	for _, part := range path {
		list, isList := value.([]any)
		index, err := strconv.Atoi(part)
		switch {
		case part == "#" && isList:
			value = len(list)
		case err == nil && isList && index < len(list):
			value = list[index]
		default:
			object, _ := value.(map[string]any)
			value = object[part]
		}
	}
	return value
}

// errInitdbDenied stands in for a server that can't initialize its data.
var errInitdbDenied = errors.New("initdb: permission denied")

// TestBackendFailureIsAnOperationError reports a server that fails to start
// as a finished operation with an error, never a 500 that clients retry.
func TestBackendFailureIsAnOperationError(t *testing.T) {
	h := newHarness(t)
	h.createInstance()
	h.fleet().mu.Lock()
	h.fleet().failCreate = errInitdbDenied
	h.fleet().mu.Unlock()
	body := insertBody()
	body[keyName] = "broken"
	operation := h.expect(h.invoke(adminToken, http.MethodPost, testInstances, body), http.StatusOK)
	subset(t, "failed create", operation, map[string]any{"operationType": "CREATE", "targetId": "broken"})
	name, _ := operation[keyName].(string)
	time.Sleep(operationLatency)
	subset(t, "failed operation", h.expect(h.invoke(adminToken, http.MethodGet, testOperations+"/"+name, nil), http.StatusOK), map[string]any{
		"status": statusDone, "error.errors.0.code": "INTERNAL_ERROR", "error.errors.0.message": "gcp-local: cloud SQL backend: initdb: permission denied"})
	h.expect(h.invoke(adminToken, http.MethodGet, testInstances+"/broken", nil), http.StatusNotFound)
}
