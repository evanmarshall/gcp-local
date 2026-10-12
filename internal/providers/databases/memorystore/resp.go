package memorystore

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"time"
)

// RESP client sentinels.
var (
	errReplyShape = errors.New("unexpected RESP reply")
	errReplySize  = errors.New("RESP reply exceeds the client bound")
)

// maxBulk bounds one bulk reply; the provider reads only short status values.
const maxBulk = 1 << 20

// respConn is a minimal RESP2 client for the provider's own admin commands:
// readiness and configuration on a private loopback endpoint. It is not a
// general Redis client and never carries caller credentials.
type respConn struct {
	conn   net.Conn
	reader *bufio.Reader
}

// dialRESP connects to one loopback Redis endpoint, bounded by ctx.
func dialRESP(ctx context.Context, address string) (*respConn, error) {
	var dialer net.Dialer
	conn, err := dialer.DialContext(ctx, "tcp", address)
	if err != nil {
		return nil, fmt.Errorf("dial redis: %w", err)
	}
	deadline, ok := ctx.Deadline()
	if !ok {
		deadline = time.Now().Add(5 * time.Second)
	}
	if err := conn.SetDeadline(deadline); err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("bound redis connection: %w", err)
	}
	return &respConn{conn: conn, reader: bufio.NewReader(conn)}, nil
}

func (c *respConn) Close() error {
	if err := c.conn.Close(); err != nil {
		return fmt.Errorf("close redis connection: %w", err)
	}
	return nil
}

// do sends one command and returns its scalar reply as text. An error reply
// is returned as an error carrying the server's message.
func (c *respConn) do(args ...string) (string, error) {
	var request strings.Builder
	fmt.Fprintf(&request, "*%d\r\n", len(args))
	for _, arg := range args {
		fmt.Fprintf(&request, "$%d\r\n%s\r\n", len(arg), arg)
	}
	if _, err := io.WriteString(c.conn, request.String()); err != nil {
		return "", fmt.Errorf("write redis command: %w", err)
	}
	return c.reply()
}

// reply reads one simple, error, integer, or bulk reply.
func (c *respConn) reply() (string, error) {
	line, err := c.reader.ReadString('\n')
	if err != nil {
		return "", fmt.Errorf("read redis reply: %w", err)
	}
	line = strings.TrimSuffix(line, "\r\n")
	if line == "" {
		return "", errReplyShape
	}
	switch line[0] {
	case '+', ':':
		return line[1:], nil
	case '-':
		return "", fmt.Errorf("%w: %s", errServerReply, line[1:])
	case '$':
		return c.bulk(line[1:])
	default:
		return "", errReplyShape
	}
}

// errServerReply wraps a Redis error reply.
var errServerReply = errors.New("redis error reply")

// bulk reads the payload of a bulk string reply.
func (c *respConn) bulk(header string) (string, error) {
	size, err := strconv.Atoi(header)
	if err != nil {
		return "", errReplyShape
	}
	if size < 0 {
		return "", nil
	}
	if size > maxBulk {
		return "", errReplySize
	}
	payload := make([]byte, size+2)
	if _, err := io.ReadFull(c.reader, payload); err != nil {
		return "", fmt.Errorf("read redis bulk reply: %w", err)
	}
	return string(payload[:size]), nil
}
