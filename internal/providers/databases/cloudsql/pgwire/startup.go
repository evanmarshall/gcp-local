package pgwire

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
)

// errStartupPacket reports a startup-phase packet the connector cannot read.
var errStartupPacket = errors.New("malformed PostgreSQL startup packet")

// ReadStartup reads the client's startup packet, declining TLS and GSS
// encryption as the Cloud SQL Auth Proxy's local listener does. It returns the
// raw packet to forward and the requested user. A cancel request is forwarded
// as-is after the same authorization.
func ReadStartup(reader *bufio.Reader, client net.Conn) ([]byte, string, error) {
	for range 3 {
		packet, code, err := readPacket(reader)
		if err != nil {
			return nil, "", err
		}
		switch code {
		case SSLRequestCode, gssRequestCode:
			if _, err := client.Write([]byte{'N'}); err != nil {
				return nil, "", fmt.Errorf("decline encryption: %w", err)
			}
		case cancelRequestCode:
			return packet, "", nil
		case ProtocolVersion:
			return packet, startupUser(packet[8:]), nil
		default:
			return nil, "", errStartupPacket
		}
	}
	return nil, "", errStartupPacket
}

// readPacket reads one untyped startup-phase packet and its request code.
func readPacket(reader *bufio.Reader) ([]byte, uint32, error) {
	header := make([]byte, 8)
	if _, err := io.ReadFull(reader, header); err != nil {
		return nil, 0, fmt.Errorf("read the startup packet: %w", err)
	}
	length := binary.BigEndian.Uint32(header)
	if length < 8 || length > 10000 {
		return nil, 0, errStartupPacket
	}
	packet := make([]byte, length)
	copy(packet, header)
	if _, err := io.ReadFull(reader, packet[8:]); err != nil {
		return nil, 0, fmt.Errorf("read the startup packet: %w", err)
	}
	return packet, binary.BigEndian.Uint32(header[4:]), nil
}

// startupUser extracts the user parameter from a startup packet body.
func startupUser(body []byte) string {
	fields := bytes.Split(body, []byte{0})
	for i := 0; i+1 < len(fields); i += 2 {
		if string(fields[i]) == "user" {
			return string(fields[i+1])
		}
	}
	return ""
}
