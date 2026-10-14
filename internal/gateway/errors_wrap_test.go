package gateway

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var errPlain = errors.New("plain failure")

func TestNormalizeErrorDropsProviderWrapping(t *testing.T) {
	inner := status.Error(codes.AlreadyExists, "Already Exists: Dataset p:d")
	got := status.Convert(normalizeError(fmt.Errorf("create the dataset: %w", inner)))
	if got.Code() != codes.AlreadyExists || got.Message() != "Already Exists: Dataset p:d" {
		t.Fatalf("normalized = %v %q", got.Code(), got.Message())
	}
	if got := status.Convert(normalizeError(errPlain)); got.Code() != codes.Internal || got.Message() != "Internal provider error" {
		t.Fatalf("unwrapped error = %v %q", got.Code(), got.Message())
	}
}

// An unselected service names the -services fix; a selected service's
// missing method still reads as a coverage gap.
func TestMissingHandlerNamesUnselectedService(t *testing.T) {
	r := NewRegistry()
	register(t, r, "storage.objects.get", func(Call, any) (any, error) { return struct{}{}, nil })
	if got := status.Convert(r.missing("cloudkms.keyRings.create")).Message(); !strings.Contains(got, "cloudkms is not enabled") {
		t.Fatalf("unselected service = %q", got)
	}
	if got := status.Convert(r.missing("storage.objects.compose")).Message(); got != "No provider implements storage.objects.compose" {
		t.Fatalf("missing method = %q", got)
	}
}

// familyRoutes are one JSON-body route per API family the gateway renders
// refusals for.
var familyRoutes = []struct{ method, path string }{
	{"storage.buckets.insert", "/storage/v1/b?project=p"},
	{"bigquery.datasets.create", "/bigquery/v2/projects/p/datasets"},
	{"sql.instances.insert", "/sql/v1beta4/projects/p/instances"},
	{"secretmanager.secrets.create", "/v1/projects/p/secrets"},
}

// rawRequest sends one request and returns the status, Content-Type, and body.
func rawRequest(t *testing.T, client *http.Client, method, url, body string, headers http.Header) (int, string, []byte) {
	t.Helper()
	req, err := http.NewRequestWithContext(context.Background(), method, url, strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	if headers != nil {
		req.Header = headers
	}
	response, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	encoded, err := io.ReadAll(response.Body)
	if err != nil {
		t.Fatal(err)
	}
	return response.StatusCode, response.Header.Get("Content-Type"), encoded
}

// familyError is the error body fields that differ between API families,
// flattened: errors[0] as reason/location, details[0] as its reason/service.
type familyError struct {
	Code                           int
	Message, Status                string
	Reason, Location, LocationType string
	DetailReason, DetailService    string
}

func decodeFamilyError(t *testing.T, encoded []byte) familyError {
	t.Helper()
	var body struct {
		Error struct {
			Code    int    `json:"code"`
			Message string `json:"message"`
			Status  string `json:"status"`
			Errors  []struct {
				Reason       string `json:"reason"`
				Domain       string `json:"domain"`
				Location     string `json:"location"`
				LocationType string `json:"locationType"`
			} `json:"errors"`
			Details []struct {
				Reason   string            `json:"reason"`
				Metadata map[string]string `json:"metadata"`
			} `json:"details"`
		} `json:"error"`
	}
	if err := json.Unmarshal(encoded, &body); err != nil {
		t.Fatalf("%v: %s", err, encoded)
	}
	e := body.Error
	got := familyError{Code: e.Code, Message: e.Message, Status: e.Status}
	if len(e.Errors) > 0 {
		got.Reason, got.Location, got.LocationType = e.Errors[0].Reason, e.Errors[0].Location, e.Errors[0].LocationType
	}
	if len(e.Details) > 0 {
		got.DetailReason, got.DetailService = e.Details[0].Reason, e.Details[0].Metadata["service"]
	}
	return got
}

// familyServer serves every family route with a handler that fails the test
// if a refused request reaches it.
func familyServer(t *testing.T) *httptest.Server {
	t.Helper()
	r := NewRegistry()
	for _, route := range familyRoutes {
		register(t, r, route.method, func(Call, any) (any, error) {
			t.Errorf("%s: refused request reached the provider", route.method)
			return nil, errPlain
		})
	}
	register(t, r, "storage.xml.objects.get", func(Call, any) (any, error) {
		t.Error("refused XML request reached the provider")
		return nil, errPlain
	})
	server := httptest.NewServer(New(r, nil))
	t.Cleanup(server.Close)
	return server
}

// A malformed Authorization header is GCP's 401 in each API family's shape
// (probed 2026-10-13).
func TestMalformedCredentialsRenderPerFamily(t *testing.T) {
	server := familyServer(t)
	auth := func(reason, service string) familyError {
		return familyError{Code: 401, Message: invalidCredentials, Status: "UNAUTHENTICATED", Reason: reason,
			DetailReason: "CREDENTIALS_MISSING", DetailService: service}
	}
	header := func(e familyError) familyError {
		e.Location, e.LocationType = "Authorization", "header"
		return e
	}
	want := map[string]familyError{
		"storage.buckets.insert":       header(familyError{Code: 401, Message: "Invalid Credentials", Reason: "authError"}),
		"bigquery.datasets.create":     header(auth("authError", "bigquery.googleapis.com")),
		"sql.instances.insert":         header(auth("authError", "sqladmin.googleapis.com")),
		"secretmanager.secrets.create": auth("", "secretmanager.googleapis.com"),
	}
	for _, route := range familyRoutes {
		for _, value := range []string{"Basic Zm9vOmJhcg==", "Bearer a b"} {
			code, _, encoded := rawRequest(t, server.Client(), http.MethodPost, server.URL+route.path, `{}`, http.Header{authorizationHeader: {value}})
			if got := decodeFamilyError(t, encoded); code != 401 || got != want[route.method] {
				t.Fatalf("%s %q: %d %s", route.method, value, code, encoded)
			}
		}
	}
	code, contentType, encoded := rawRequest(t, server.Client(), http.MethodGet, server.URL+"/bucket/object", "", http.Header{authorizationHeader: {"Bearer"}})
	if code != 401 || !strings.HasPrefix(contentType, "application/xml") || !strings.Contains(string(encoded), "<Code>AuthenticationRequired</Code>") {
		t.Fatalf("xml: %d %s %s", code, contentType, encoded)
	}
}

// A JSON body over the cap is a non-retryable refusal: Storage's 413
// uploadTooLarge, and every other family's 400 INVALID_ARGUMENT, never 429.
func TestOversizedJSONIsNotRetryable(t *testing.T) {
	server := familyServer(t)
	tooLarge := func(reason string) familyError {
		return familyError{Code: 400, Message: "Request payload size exceeds the limit: 2097152 bytes.", Status: "INVALID_ARGUMENT", Reason: reason}
	}
	want := map[string]familyError{
		"storage.buckets.insert":       {Code: 413, Message: "Request Too Large", Reason: "uploadTooLarge"},
		"bigquery.datasets.create":     tooLarge("invalid"),
		"sql.instances.insert":         tooLarge("invalid"),
		"secretmanager.secrets.create": tooLarge(""),
	}
	oversized := `{"padding":"` + strings.Repeat("a", maxJSON) + `"}`
	for _, route := range familyRoutes {
		code, _, encoded := rawRequest(t, server.Client(), http.MethodPost, server.URL+route.path, oversized, nil)
		if got := decodeFamilyError(t, encoded); code != want[route.method].Code || got != want[route.method] {
			t.Fatalf("%s: %d %s", route.method, code, encoded)
		}
	}
}
