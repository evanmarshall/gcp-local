package xmlapi

import (
	"bytes"
	"io"
	"testing"
	"time"
)

// TestSignedURLWindow checks the expiry and clock-skew bounds directly.
func TestSignedURLWindow(t *testing.T) {
	now := time.Date(2026, 10, 11, 12, 0, 0, 0, time.UTC)
	for _, tc := range []struct {
		date, expires, code string
	}{
		{"20261011T113000Z", "60", "ExpiredToken"},
		{"20261011T130000Z", "60", "InvalidArgument"},
		{"20261011T115959Z", "604801", "InvalidArgument"},
		{"2026-10-11", "60", "InvalidArgument"},
	} {
		query := map[string][]string{"X-Goog-Date": {tc.date}, "X-Goog-Expires": {tc.expires}}
		if _, refused := signedWindow(query, now); refused == nil {
			t.Fatalf("%s/%s accepted", tc.date, tc.expires)
		} else if body, _ := io.ReadAll(refused.Body); !bytes.Contains(body, []byte("<Code>"+tc.code+"</Code>")) {
			t.Fatalf("%s/%s: %s", tc.date, tc.expires, body)
		}
	}
	if _, refused := signedWindow(map[string][]string{"X-Goog-Date": {"20261011T115930Z"}, "X-Goog-Expires": {"60"}}, now); refused != nil {
		t.Fatal("valid window refused")
	}
}
