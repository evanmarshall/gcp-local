// Package pgwire is the PostgreSQL frontend/backend protocol 3.0 slice the
// Cloud SQL provider speaks: an authenticated simple-query client and the
// startup-phase reader of the connector.
package pgwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/pbkdf2"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"encoding/base64"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// PostgreSQL frontend/backend protocol 3.0 constants used by the client and
// the connector. Only the startup, SCRAM-SHA-256, and simple-query paths are
// implemented: the provider needs no other protocol feature.
const (
	ProtocolVersion   = 196608 // 3.0
	SSLRequestCode    = 80877103
	gssRequestCode    = 80877104
	cancelRequestCode = 80877102
	maxMessage        = 1 << 20
	authOK            = 0
	authCleartext     = 3
	authSASL          = 10
	authSASLContinue  = 11
	authSASLFinal     = 12
	scramMechanism    = "SCRAM-SHA-256"
)

// Wire client sentinels.
var (
	errWireMessage   = errors.New("malformed PostgreSQL protocol message")
	errWireAuth      = errors.New("unsupported PostgreSQL authentication request")
	errScramServer   = errors.New("PostgreSQL SCRAM server response failed verification")
	errWireMechanism = errors.New("PostgreSQL server does not offer SCRAM-SHA-256")
)

// Error is a server ErrorResponse; Code is the SQLSTATE.
type Error struct {
	Severity, Code, Message string
}

func (e *Error) Error() string { return e.Severity + " " + e.Code + ": " + e.Message }

// Conn is one authenticated simple-query session.
type Conn struct {
	conn   net.Conn
	reader *bufio.Reader
}

// Dial opens and authenticates one session over a dialed connection.
func Dial(ctx context.Context, network, address, user, database, password string) (*Conn, error) {
	return DialTLS(ctx, network, address, user, database, password, nil)
}

// errNoTLS reports a server that declined an SSLRequest.
var errNoTLS = errors.New("PostgreSQL server declined TLS")

// DialTLS is Dial with TLS negotiated by an SSLRequest when secure is set,
// as libpq's sslmode=verify-ca or verify-full does with secure's roots.
func DialTLS(ctx context.Context, network, address, user, database, password string, secure *tls.Config) (*Conn, error) {
	var dialer net.Dialer
	raw, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("dial PostgreSQL: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = raw.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { _ = raw.Close() })
	defer stop()
	conn := raw
	if secure != nil {
		if conn, err = negotiateTLS(ctx, raw, secure); err != nil {
			_ = raw.Close()
			return nil, err
		}
	}
	session := &Conn{conn: conn, reader: bufio.NewReader(conn)}
	if err := session.startup(user, database, password); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return session, nil
}

// negotiateTLS sends an SSLRequest and starts TLS when the server accepts.
func negotiateTLS(ctx context.Context, raw net.Conn, secure *tls.Config) (net.Conn, error) {
	request := binary.BigEndian.AppendUint32(binary.BigEndian.AppendUint32(nil, 8), SSLRequestCode)
	if _, err := raw.Write(request); err != nil {
		return nil, fmt.Errorf("write the SSLRequest: %w", err)
	}
	answer := make([]byte, 1)
	if _, err := io.ReadFull(raw, answer); err != nil {
		return nil, fmt.Errorf("read the SSLRequest answer: %w", err)
	}
	if answer[0] != 'S' {
		return nil, errNoTLS
	}
	conn := tls.Client(raw, secure)
	if err := conn.HandshakeContext(ctx); err != nil {
		return nil, fmt.Errorf("PostgreSQL TLS handshake: %w", err)
	}
	return conn, nil
}

// startup sends the startup packet and completes authentication.
func (c *Conn) startup(user, database, password string) error {
	var params bytes.Buffer
	for _, pair := range [][2]string{{"user", user}, {"database", database}, {"client_encoding", "UTF8"}} {
		params.WriteString(pair[0] + "\x00" + pair[1] + "\x00")
	}
	params.WriteByte(0)
	packet := binary.BigEndian.AppendUint32(nil, uint32(8+params.Len())) //nolint:gosec // bounded by the parameters
	packet = binary.BigEndian.AppendUint32(packet, ProtocolVersion)
	if _, err := c.conn.Write(append(packet, params.Bytes()...)); err != nil {
		return fmt.Errorf("write PostgreSQL startup: %w", err)
	}
	return c.authenticate(password)
}

// authenticate answers the server's authentication requests until ReadyForQuery.
func (c *Conn) authenticate(password string) error {
	var scram *scramClient
	for {
		kind, body, err := c.receive()
		if err != nil {
			return err
		}
		switch kind {
		case 'R':
			scram, err = c.authStep(body, password, scram)
			if err != nil {
				return err
			}
		case 'Z':
			return nil
		case 'E':
			return parseError(body)
		default: // ParameterStatus, BackendKeyData, NoticeResponse
		}
	}
}

// authStep handles one AuthenticationRequest message.
func (c *Conn) authStep(body []byte, password string, scram *scramClient) (*scramClient, error) {
	if len(body) < 4 {
		return nil, errWireMessage
	}
	payload := body[4:]
	switch binary.BigEndian.Uint32(body) {
	case authOK:
		return scram, nil
	case authCleartext:
		return scram, c.send('p', append([]byte(password), 0))
	case authSASL, authSASLContinue, authSASLFinal:
		return c.saslStep(binary.BigEndian.Uint32(body), payload, password, scram)
	default:
		return nil, errWireAuth
	}
}

// saslStep advances the SCRAM-SHA-256 exchange by one server message.
func (c *Conn) saslStep(kind uint32, payload []byte, password string, scram *scramClient) (*scramClient, error) {
	if kind == authSASL {
		if !bytes.Contains(payload, []byte(scramMechanism+"\x00")) {
			return nil, errWireMechanism
		}
		scram = newScramClient(password)
		first := []byte(scram.clientFirst())
		message := append([]byte(scramMechanism+"\x00"), binary.BigEndian.AppendUint32(nil, uint32(len(first)))...) //nolint:gosec // short SCRAM message
		return scram, c.send('p', append(message, first...))
	}
	if scram == nil {
		return nil, errWireAuth
	}
	if kind == authSASLFinal {
		if !scram.verify(string(payload)) {
			return nil, errScramServer
		}
		return scram, nil
	}
	final, err := scram.clientFinal(string(payload))
	if err != nil {
		return nil, err
	}
	return scram, c.send('p', []byte(final))
}

// send writes one typed frontend message.
func (c *Conn) send(kind byte, body []byte) error {
	message := append([]byte{kind}, binary.BigEndian.AppendUint32(nil, uint32(len(body)+4))...) //nolint:gosec // bounded messages
	if _, err := c.conn.Write(append(message, body...)); err != nil {
		return fmt.Errorf("write PostgreSQL message: %w", err)
	}
	return nil
}

// receive reads one typed backend message.
func (c *Conn) receive() (byte, []byte, error) {
	header := make([]byte, 5)
	if _, err := io.ReadFull(c.reader, header); err != nil {
		return 0, nil, fmt.Errorf("read PostgreSQL message: %w", err)
	}
	length := binary.BigEndian.Uint32(header[1:])
	if length < 4 || length > maxMessage {
		return 0, nil, errWireMessage
	}
	body := make([]byte, length-4)
	if _, err := io.ReadFull(c.reader, body); err != nil {
		return 0, nil, fmt.Errorf("read PostgreSQL message body: %w", err)
	}
	return header[0], body, nil
}

// Query runs one simple query and returns its text rows. NULL is "".
func (c *Conn) Query(ctx context.Context, sql string) ([][]string, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.Close() })
	defer stop()
	if err := c.send('Q', append([]byte(sql), 0)); err != nil {
		return nil, err
	}
	var rows [][]string
	var failure error
	for {
		kind, body, err := c.receive()
		if err != nil {
			return nil, err
		}
		switch kind {
		case 'D':
			row, err := parseRow(body)
			if err != nil {
				return nil, err
			}
			rows = append(rows, row)
		case 'E':
			failure = parseError(body)
		case 'Z':
			return rows, failure
		default: // RowDescription, CommandComplete, EmptyQueryResponse, notices
		}
	}
}

// Close terminates the session.
func (c *Conn) Close() {
	_ = c.send('X', nil)
	_ = c.conn.Close()
}

// parseRow decodes one DataRow message.
func parseRow(body []byte) ([]string, error) {
	if len(body) < 2 {
		return nil, errWireMessage
	}
	count := int(binary.BigEndian.Uint16(body))
	body = body[2:]
	row := make([]string, 0, count)
	for range count {
		if len(body) < 4 {
			return nil, errWireMessage
		}
		size := int32(binary.BigEndian.Uint32(body)) //nolint:gosec // -1 is the protocol's NULL marker
		body = body[4:]
		if size < 0 {
			row = append(row, "")
			continue
		}
		if int(size) > len(body) {
			return nil, errWireMessage
		}
		row = append(row, string(body[:size]))
		body = body[size:]
	}
	return row, nil
}

// parseError decodes an ErrorResponse message.
func parseError(body []byte) error {
	result := &Error{}
	for field := range bytes.SplitSeq(body, []byte{0}) {
		if len(field) < 2 {
			continue
		}
		value := string(field[1:])
		switch field[0] {
		case 'S':
			result.Severity = value
		case 'C':
			result.Code = value
		case 'M':
			result.Message = value
		default:
		}
	}
	return result
}

// ErrorResponse encodes a FATAL ErrorResponse for the connector.
func ErrorResponse(code, message string) []byte {
	var body bytes.Buffer
	for _, field := range [][2]string{{"S", "FATAL"}, {"V", "FATAL"}, {"C", code}, {"M", message}} {
		body.WriteString(field[0] + field[1] + "\x00")
	}
	body.WriteByte(0)
	return append(append([]byte{'E'}, binary.BigEndian.AppendUint32(nil, uint32(body.Len()+4))...), body.Bytes()...) //nolint:gosec // short message
}

// scramClient performs the client half of SCRAM-SHA-256 (RFC 5802/7677).
// PostgreSQL ignores the SCRAM user name, so the bare message carries none.
type scramClient struct {
	password, nonce, firstBare, auth string
	salted                           []byte
}

func newScramClient(password string) *scramClient {
	nonce := make([]byte, 18)
	_, _ = rand.Read(nonce) // crypto/rand.Read never returns an error
	return &scramClient{password: password, nonce: base64.RawStdEncoding.EncodeToString(nonce)}
}

func (s *scramClient) clientFirst() string {
	s.firstBare = "n=,r=" + s.nonce
	return "n,," + s.firstBare
}

// clientFinal answers the server-first message with the client proof.
func (s *scramClient) clientFinal(serverFirst string) (string, error) {
	fields := map[string]string{}
	for part := range strings.SplitSeq(serverFirst, ",") {
		if key, value, ok := strings.Cut(part, "="); ok {
			fields[key] = value
		}
	}
	salt, err := base64.StdEncoding.DecodeString(fields["s"])
	iterations, convErr := strconv.Atoi(fields["i"])
	if err != nil || convErr != nil || iterations < 1 || iterations > 1<<20 || !strings.HasPrefix(fields["r"], s.nonce) {
		return "", errScramServer
	}
	s.salted, err = pbkdf2.Key(sha256.New, s.password, salt, iterations, sha256.Size)
	if err != nil {
		return "", fmt.Errorf("derive the SCRAM key: %w", err)
	}
	withoutProof := "c=biws,r=" + fields["r"]
	s.auth = s.firstBare + "," + serverFirst + "," + withoutProof
	clientKey := mac(s.salted, "Client Key")
	stored := sha256.Sum256(clientKey)
	signature := mac(stored[:], s.auth)
	proof := make([]byte, len(clientKey))
	for i := range clientKey {
		proof[i] = clientKey[i] ^ signature[i]
	}
	return withoutProof + ",p=" + base64.StdEncoding.EncodeToString(proof), nil
}

// verify checks the server signature in the server-final message.
func (s *scramClient) verify(serverFinal string) bool {
	signature, err := base64.StdEncoding.DecodeString(strings.TrimPrefix(serverFinal, "v="))
	return err == nil && hmac.Equal(signature, mac(mac(s.salted, "Server Key"), s.auth))
}

func mac(key []byte, message string) []byte {
	h := hmac.New(sha256.New, key)
	h.Write([]byte(message))
	return h.Sum(nil)
}

// QuoteIdentifier renders a PostgreSQL identifier literal.
func QuoteIdentifier(name string) string {
	return `"` + strings.ReplaceAll(name, `"`, `""`) + `"`
}

// QuoteLiteral renders an escape-string literal, whose meaning does not
// depend on the session's standard_conforming_strings setting. Callers reject
// NUL bytes, which no PostgreSQL text value can carry.
func QuoteLiteral(value string) string {
	return "E'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", "''") + "'"
}
