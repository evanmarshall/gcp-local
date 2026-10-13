package mysqlwire

import (
	"encoding/hex"
	"strings"
	"testing"
)

func TestMySQLScrambles(t *testing.T) {
	// Expected values were computed independently with Python's hashlib from
	// the protocol definitions; the integration test authenticates against mysqld.
	scramble := []byte{9, 103, 5, 73, 72, 88, 107, 99, 122, 7, 125, 16, 123, 117, 18, 79, 8, 50, 4, 96}
	if got := hex.EncodeToString(nativeScramble(scramble, "secret")); got != "de24aeb32dc1ea308b37ba2d7fb910e5c78944b4" {
		t.Fatalf("native scramble = %s", got)
	}
	if got := hex.EncodeToString(sha2Scramble(scramble, "secret")); got != "78b2547329d46aa4fecdb2432322f129dc9befc7603de8e65dc6bbde1b9579ce" {
		t.Fatalf("caching_sha2 scramble = %s", got)
	}
	if nativeScramble(scramble, "") != nil || sha2Scramble(scramble, "") != nil {
		t.Fatal("empty passwords must send no auth data")
	}
}

func TestMySQLRowsAndQuoting(t *testing.T) {
	row, err := parseTextRow([]byte{0xfb, 3, 'a', 'b', 'c', 0xfc, 1, 0, 'z'}, 3)
	if err != nil || strings.Join(row, ",") != ",abc,z" {
		t.Fatalf("row = %q, %v", row, err)
	}
	if _, err := parseTextRow([]byte{5, 'a'}, 1); err == nil {
		t.Fatal("accepted a truncated row")
	}
	if failure := ParseError(ErrorPacket(0, 1045, "28000", "denied")[4:]); failure.Error() != "ERROR 1045 (28000): denied" {
		t.Fatalf("error = %v", failure)
	}
	if QuoteIdentifier("a`b") != "`a``b`" || QuoteLiteral(`a'b\c`) != `'a\'b\\c'` {
		t.Fatal("MySQL quoting is wrong")
	}
}
