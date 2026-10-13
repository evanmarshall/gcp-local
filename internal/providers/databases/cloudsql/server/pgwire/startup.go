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

// errBufferedHandshake refuses bytes sent after an SSLRequest before the
// server's answer, which an attacker could inject ahead of the TLS handshake.
var errBufferedHandshake = errors.New("PostgreSQL client sent data before the TLS handshake")

// ReadStartup reads the client's startup packet. GSS encryption is declined;
// TLS is declined unless offerTLS, in which case it answers 'S' and returns
// encrypt so the caller can start TLS and read the startup packet again. It
// returns the raw packet to forward and the requested user. A cancel request
// is forwarded as-is after the same authorization.
func ReadStartup(reader *bufio.Reader, client net.Conn, offerTLS bool) ([]byte, string, bool, error) {
	for range 3 {
		packet, code, err := readPacket(reader)
		if err != nil {
			return nil, "", false, err
		}
		switch code {
		case SSLRequestCode, gssRequestCode:
			encrypt, err := answerEncryption(reader, client, offerTLS && code == SSLRequestCode)
			if encrypt || err != nil {
				return nil, "", encrypt, err
			}
		case cancelRequestCode:
			return packet, "", false, nil
		case ProtocolVersion:
			return packet, startupUser(packet[8:]), false, nil
		default:
			return nil, "", false, errStartupPacket
		}
	}
	return nil, "", false, errStartupPacket
}

// answerEncryption accepts TLS with 'S' when accept, refusing bytes the
// client sent before the answer, and declines encryption with 'N' otherwise.
func answerEncryption(reader *bufio.Reader, client net.Conn, accept bool) (bool, error) {
	if !accept {
		if _, err := client.Write([]byte{'N'}); err != nil {
			return false, fmt.Errorf("decline encryption: %w", err)
		}
		return false, nil
	}
	if reader.Buffered() > 0 {
		return false, errBufferedHandshake
	}
	if _, err := client.Write([]byte{'S'}); err != nil {
		return false, fmt.Errorf("accept encryption: %w", err)
	}
	return true, nil
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
