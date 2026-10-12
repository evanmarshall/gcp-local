// Package mysqlwire is the MySQL client/server protocol slice the Cloud SQL
// provider speaks: the administrative client and the connector's handshake relay.
package mysqlwire

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha1" //nolint:gosec // mysql_native_password is defined over SHA-1
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
)

// MySQL client/server protocol constants used by the administrative client
// and the connector. Only the 4.1 handshake, mysql_native_password,
// caching_sha2_password over a Unix socket, and text-protocol COM_QUERY are
// implemented: the provider needs no other protocol feature.
const (
	ProtocolVersion  = 10
	MaxPacket        = 1<<24 - 1
	mysqlComQuery    = 0x03
	NativePassword   = "mysql_native_password" //nolint:gosec // an authentication plugin name, not a credential
	mysqlCachingSHA2 = "caching_sha2_password"
	CharsetUTF8MB4   = 45 // utf8mb4_general_ci
	mysqlMoreResults = 0x0008
	AccessDenied     = 1045
	HostRefused      = 1130
)

// maxMessage bounds one MySQL wire packet, like the PostgreSQL client's bound.
const maxMessage = 1 << 20

// Capability flags (lower 32 bits) the client requests and the connector inspects.
const (
	capLongPassword     = 0x00000001
	CapProtocol41       = 0x00000200
	CapSSL              = 0x00000800
	capTransactions     = 0x00002000
	CapSecureConnection = 0x00008000
	capMultiStatements  = 0x00010000
	capMultiResults     = 0x00020000
	CapPluginAuth       = 0x00080000
)

// MySQL wire sentinels.
var (
	ErrPacket = errors.New("malformed MySQL protocol packet")
	ErrAuth   = errors.New("unsupported MySQL authentication request")
)

// Error is a server ERR packet.
type Error struct {
	Code           uint16
	State, Message string
}

func (e *Error) Error() string {
	return "ERROR " + strconv.Itoa(int(e.Code)) + " (" + e.State + "): " + e.Message
}

// ReadPacket reads one packet and returns its payload and sequence id.
// Payloads that span several packets are refused: the provider never needs one.
func ReadPacket(reader io.Reader) ([]byte, byte, error) {
	header := make([]byte, 4)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, 0, fmt.Errorf("read MySQL packet: %w", err)
	}
	length := int(header[0]) | int(header[1])<<8 | int(header[2])<<16
	if length > maxMessage {
		return nil, 0, ErrPacket
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(reader, payload); err != nil {
		return nil, 0, fmt.Errorf("read MySQL packet body: %w", err)
	}
	return payload, header[3], nil
}

// Packet frames one payload with its sequence id.
func Packet(sequence byte, payload []byte) []byte {
	header := binary.LittleEndian.AppendUint32(nil, uint32(len(payload))) //nolint:gosec // payloads stay under maxMessage
	header[3] = sequence
	return append(header, payload...)
}

// ErrorPacket encodes an ERR packet for the connector.
func ErrorPacket(sequence byte, code uint16, state, message string) []byte {
	payload := binary.LittleEndian.AppendUint16([]byte{0xff}, code)
	payload = append(payload, '#')
	payload = append(payload, state...)
	return Packet(sequence, append(payload, message...))
}

// ParseError decodes an ERR packet payload.
func ParseError(payload []byte) error {
	if len(payload) < 3 {
		return ErrPacket
	}
	result := &Error{Code: binary.LittleEndian.Uint16(payload[1:])}
	rest := payload[3:]
	if len(rest) >= 6 && rest[0] == '#' {
		result.State, rest = string(rest[1:6]), rest[6:]
	}
	result.Message = string(rest)
	return result
}

// greeting is the parsed part of a server's initial handshake.
type greeting struct {
	scramble []byte
	plugin   string
}

// parseGreeting decodes a Handshake v10 payload.
func parseGreeting(payload []byte) (greeting, error) {
	if len(payload) > 0 && payload[0] == 0xff {
		return greeting{}, ParseError(payload)
	}
	version := bytes.IndexByte(payload, 0)
	if len(payload) == 0 || payload[0] != ProtocolVersion || version < 0 {
		return greeting{}, ErrPacket
	}
	rest := payload[version+1:]
	// thread id (4), scramble part 1 (8), filler (1), capabilities (2),
	// charset (1), status (2), capabilities (2), scramble length (1), reserved (10).
	if len(rest) < 31 {
		return greeting{}, ErrPacket
	}
	scramble := append([]byte(nil), rest[4:12]...)
	part, size := rest[31:], max(13, int(rest[20])-8)
	if len(part) < size {
		return greeting{}, ErrPacket
	}
	scramble = append(scramble, part[:12]...)
	plugin := part[size:]
	if name := bytes.IndexByte(plugin, 0); name >= 0 {
		plugin = plugin[:name]
	}
	return greeting{scramble: scramble, plugin: string(plugin)}, nil
}

// WithoutTLS clears the server's SSL capability in a greeting, so the client
// sends its credentials in clear over the loopback hop, as the PostgreSQL
// connector declines TLS. It reports false for a payload that is not a greeting.
func WithoutTLS(payload []byte) bool {
	version := bytes.IndexByte(payload, 0)
	if len(payload) == 0 || payload[0] != ProtocolVersion || version < 0 || len(payload) < version+1+15 {
		return false
	}
	offset := version + 1 + 13 // thread id, scramble part 1, filler
	flags := binary.LittleEndian.Uint16(payload[offset:]) &^ CapSSL
	binary.LittleEndian.PutUint16(payload[offset:], flags)
	return true
}

// HandshakeUser extracts the user of a 4.1 HandshakeResponse. It refuses an
// SSLRequest and pre-4.1 responses.
func HandshakeUser(payload []byte) (string, bool) {
	if len(payload) < 33 {
		return "", false
	}
	flags := binary.LittleEndian.Uint32(payload)
	if flags&CapProtocol41 == 0 || flags&CapSSL != 0 {
		return "", false
	}
	user, _, found := bytes.Cut(payload[32:], []byte{0})
	return string(user), found
}

// nativeScramble is the mysql_native_password response:
// SHA1(password) XOR SHA1(scramble || SHA1(SHA1(password))).
func nativeScramble(scramble []byte, password string) []byte {
	if password == "" {
		return nil
	}
	stage1 := sha1.Sum([]byte(password)) //nolint:gosec // protocol-defined
	stage2 := sha1.Sum(stage1[:])        //nolint:gosec // protocol-defined
	hash := sha1.New()                   //nolint:gosec // protocol-defined
	hash.Write(scramble[:20])
	hash.Write(stage2[:])
	mixed := hash.Sum(nil)
	for i := range mixed {
		mixed[i] ^= stage1[i]
	}
	return mixed
}

// sha2Scramble is the caching_sha2_password fast-path response:
// SHA256(password) XOR SHA256(SHA256(SHA256(password)) || scramble).
func sha2Scramble(scramble []byte, password string) []byte {
	if password == "" {
		return nil
	}
	stage1 := sha256.Sum256([]byte(password))
	stage2 := sha256.Sum256(stage1[:])
	mixed := sha256.Sum256(append(stage2[:], scramble[:20]...))
	for i := range mixed {
		mixed[i] ^= stage1[i]
	}
	return mixed[:]
}

// Conn is one authenticated text-protocol session.
type Conn struct {
	conn     net.Conn
	reader   *bufio.Reader
	sequence byte
	network  string
}

// Dial opens and authenticates one session as user.
func Dial(ctx context.Context, network, address, user, password string) (*Conn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, network, address)
	if err != nil {
		return nil, fmt.Errorf("dial MySQL: %w", err)
	}
	if deadline, ok := ctx.Deadline(); ok {
		_ = conn.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { _ = conn.Close() })
	defer stop()
	session := &Conn{conn: conn, reader: bufio.NewReader(conn), network: network}
	if err := session.handshake(user, password); err != nil {
		_ = conn.Close()
		return nil, err
	}
	return session, nil
}

// handshake answers the server greeting and completes authentication.
func (c *Conn) handshake(user, password string) error {
	payload, err := c.receive()
	if err != nil {
		return err
	}
	hello, err := parseGreeting(payload)
	if err != nil {
		return err
	}
	// Answer with mysql_native_password, which every account the provider
	// creates uses; the server switches plugins for any other account.
	plugin := NativePassword
	auth := authResponse(plugin, hello.scramble, password)
	flags := uint32(capLongPassword | CapProtocol41 | capTransactions | CapSecureConnection | capMultiStatements | capMultiResults | CapPluginAuth)
	response := binary.LittleEndian.AppendUint32(nil, flags)
	response = binary.LittleEndian.AppendUint32(response, MaxPacket)
	response = append(response, CharsetUTF8MB4)
	response = append(response, make([]byte, 23)...)
	response = append(append(response, user...), 0)
	response = append(append(response, byte(len(auth))), auth...) //nolint:gosec // a 20- or 32-byte scramble
	response = append(append(response, plugin...), 0)
	if err := c.send(response); err != nil {
		return err
	}
	return c.authenticate(password)
}

// authResponse computes the auth data for one plugin.
func authResponse(plugin string, scramble []byte, password string) []byte {
	if plugin == mysqlCachingSHA2 {
		return sha2Scramble(scramble, password)
	}
	return nativeScramble(scramble, password)
}

// authenticate follows auth-switch and caching_sha2 requests until OK.
func (c *Conn) authenticate(password string) error {
	for {
		payload, err := c.receive()
		if err != nil {
			return err
		}
		done, err := c.authStep(payload, password)
		if done || err != nil {
			return err
		}
	}
}

// authStep answers one authentication packet and reports the final OK.
func (c *Conn) authStep(payload []byte, password string) (bool, error) {
	if len(payload) == 0 {
		return false, ErrPacket
	}
	switch payload[0] {
	case 0x00:
		return true, nil
	case 0xff:
		return false, ParseError(payload)
	case 0xfe:
		return false, c.switchPlugin(payload[1:], password)
	case 0x01:
		return false, c.moreData(payload, password)
	default:
		return false, ErrAuth
	}
}

// switchPlugin answers an AuthSwitchRequest: a plugin name, then a fresh scramble.
func (c *Conn) switchPlugin(request []byte, password string) error {
	name, scramble, found := bytes.Cut(request, []byte{0})
	if !found || len(scramble) < 20 || (string(name) != NativePassword && string(name) != mysqlCachingSHA2) {
		return ErrAuth
	}
	return c.send(authResponse(string(name), scramble, password))
}

// moreData answers caching_sha2_password: 3 is fast-auth success, 4 asks for
// the password, which is sent only over a Unix socket.
func (c *Conn) moreData(payload []byte, password string) error {
	switch {
	case len(payload) == 2 && payload[1] == 3:
		return nil
	case len(payload) == 2 && payload[1] == 4 && c.network == "unix":
		return c.send(append([]byte(password), 0))
	default:
		return ErrAuth
	}
}

// send writes one packet with the next sequence id.
func (c *Conn) send(payload []byte) error {
	if _, err := c.conn.Write(Packet(c.sequence, payload)); err != nil {
		return fmt.Errorf("write MySQL packet: %w", err)
	}
	c.sequence++
	return nil
}

// receive reads one packet and tracks the sequence id.
func (c *Conn) receive() ([]byte, error) {
	payload, sequence, err := ReadPacket(c.reader)
	if err != nil {
		return nil, err
	}
	c.sequence = sequence + 1
	return payload, nil
}

// Query runs one COM_QUERY, which may hold several statements, and returns
// the text rows of every result set. NULL is "". It stops at the first error.
func (c *Conn) Query(ctx context.Context, sql string) ([][]string, error) {
	if deadline, ok := ctx.Deadline(); ok {
		_ = c.conn.SetDeadline(deadline)
	}
	stop := context.AfterFunc(ctx, func() { _ = c.conn.Close() })
	defer stop()
	c.sequence = 0
	if err := c.send(append([]byte{mysqlComQuery}, sql...)); err != nil {
		return nil, err
	}
	var rows [][]string
	for {
		more, result, err := c.result()
		if err != nil {
			return nil, err
		}
		rows = append(rows, result...)
		if !more {
			return rows, nil
		}
	}
}

// result reads one result: an OK, an ERR, or a text result set.
func (c *Conn) result() (bool, [][]string, error) {
	payload, err := c.receive()
	if err != nil {
		return false, nil, err
	}
	if len(payload) == 0 {
		return false, nil, ErrPacket
	}
	switch payload[0] {
	case 0x00:
		return okMoreResults(payload), nil, nil
	case 0xff:
		return false, nil, ParseError(payload)
	}
	// A LOCAL INFILE request (0xfb) is never expected and fails to decode here.
	columns, _, ok := lengthEncoded(payload)
	if !ok || columns == 0 || columns > 4096 {
		return false, nil, ErrPacket
	}
	for range columns + 1 { // column definitions, then EOF
		if _, err := c.receive(); err != nil {
			return false, nil, err
		}
	}
	return c.rows(int(columns))
}

// rows reads text rows until the EOF packet that ends a result set.
func (c *Conn) rows(columns int) (bool, [][]string, error) {
	var rows [][]string
	for {
		payload, err := c.receive()
		if err != nil {
			return false, nil, err
		}
		switch {
		case len(payload) > 0 && payload[0] == 0xfe && len(payload) < 9:
			more := len(payload) >= 5 && binary.LittleEndian.Uint16(payload[3:])&mysqlMoreResults != 0
			return more, rows, nil
		case len(payload) > 0 && payload[0] == 0xff:
			return false, nil, ParseError(payload)
		}
		row, err := parseTextRow(payload, columns)
		if err != nil {
			return false, nil, err
		}
		rows = append(rows, row)
	}
}

// okMoreResults reads the status flags of an OK packet.
func okMoreResults(payload []byte) bool {
	rest := payload[1:]
	for range 2 { // affected rows, last insert id
		_, size, ok := lengthEncoded(rest)
		if !ok {
			return false
		}
		rest = rest[size:]
	}
	return len(rest) >= 2 && binary.LittleEndian.Uint16(rest)&mysqlMoreResults != 0
}

// parseTextRow decodes one text-protocol row of length-encoded strings.
func parseTextRow(payload []byte, columns int) ([]string, error) {
	row := make([]string, 0, columns)
	for range columns {
		if len(payload) > 0 && payload[0] == 0xfb {
			row, payload = append(row, ""), payload[1:]
			continue
		}
		length, size, ok := lengthEncoded(payload)
		if !ok || uint64(len(payload)-size) < length { //nolint:gosec // size never exceeds len(payload)
			return nil, ErrPacket
		}
		row = append(row, string(payload[size:size+int(length)])) //nolint:gosec // bounded by the payload
		payload = payload[size+int(length):]                      //nolint:gosec // bounded by the payload
	}
	return row, nil
}

// lengthEncoded decodes a length-encoded integer and its size.
func lengthEncoded(data []byte) (uint64, int, bool) {
	if len(data) == 0 {
		return 0, 0, false
	}
	size := map[byte]int{0xfc: 3, 0xfd: 4, 0xfe: 9}[data[0]]
	switch {
	case data[0] < 0xfb:
		return uint64(data[0]), 1, true
	case size == 0 || len(data) < size:
		return 0, 0, false
	}
	var value uint64
	for i := size - 1; i >= 1; i-- {
		value = value<<8 | uint64(data[i])
	}
	return value, size, true
}

// Close sends COM_QUIT and closes the session.
func (c *Conn) Close() {
	c.sequence = 0
	_ = c.send([]byte{0x01})
	_ = c.conn.Close()
}

// QuoteIdentifier renders a backtick-quoted identifier.
func QuoteIdentifier(name string) string {
	return "`" + strings.ReplaceAll(name, "`", "``") + "`"
}

// QuoteLiteral renders a string literal for a session whose sql_mode the
// administrative client sets (backslash escapes enabled). Callers reject NUL.
func QuoteLiteral(value string) string {
	return "'" + strings.ReplaceAll(strings.ReplaceAll(value, `\`, `\\`), "'", `\'`) + "'"
}
