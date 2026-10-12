// Package xmlapi holds the Cloud Storage XML API wire: V4 signed URL
// verification and the XML documents the API answers with.
package xmlapi

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"net"
	"net/url"
	"slices"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
)

// UnsignedPayload is the payload hash a signed request may declare.
const UnsignedPayload = "UNSIGNED-PAYLOAD"

// V4 signing literals.
const (
	paramSignature     = "X-Goog-Signature"
	headerPayloadHash  = "x-goog-content-sha256"
	signatureAlgorithm = "GOOG4-RSA-SHA256"
	signedDateLayout   = "20060102T150405Z"
	maxSignedExpiry    = 7 * 24 * 60 * 60
	// signedClockSkew is how far in the future X-Goog-Date may be.
	signedClockSkew = 15 * time.Minute
	cloudPlatform   = "https://www.googleapis.com/auth/cloud-platform"
)

// signedParameters are the query options a V4 signed URL carries.
var signedParameters = []string{"X-Goog-Algorithm", "X-Goog-Credential", "X-Goog-Date", "X-Goog-Expires", "X-Goog-SignedHeaders", paramSignature}

// IsSigned reports whether a request carries V4 signed URL parameters.
func IsSigned(query url.Values) bool {
	return slices.ContainsFunc(signedParameters, query.Has)
}

// Verifier checks a signature with one of an account's published keys.
type Verifier func(account string, message, signature []byte) (state.Scope, bool)

// VerifySignedURL checks a V4 signed request: its parameters, its validity
// window, its credential scope, and its RSA signature over the canonical
// request. The signer becomes the request's principal, so the signer's own
// Storage permissions authorize the access, as in GCP.
func VerifySignedURL(r *gateway.RESTRequest, now time.Time, verify Verifier) (gateway.Principal, *gateway.RESTResponse) {
	account, scope, signature, refused := signedParts(r.Query, now)
	if refused != nil {
		return gateway.Principal{}, refused
	}
	// Python signs the Host header as sent (host:port); Go signs the host name
	// without its port. Real GCS is never addressed with a port, so both
	// forms name the same local endpoint.
	hosts := []string{r.Host}
	if name, _, err := net.SplitHostPort(r.Host); err == nil {
		hosts = append(hosts, name)
	}
	for _, host := range hosts {
		canonical, refused := canonicalRequest(r, host)
		if refused != nil {
			return gateway.Principal{}, refused
		}
		digest := sha256.Sum256([]byte(canonical))
		message := strings.Join([]string{signatureAlgorithm, r.Query.Get("X-Goog-Date"), scope, hex.EncodeToString(digest[:])}, "\n")
		if owner, ok := verify(account, []byte(message), signature); ok {
			return gateway.Principal{State: gateway.Authenticated, Subject: "serviceAccount:" + account, Project: owner.Project, Scopes: []string{cloudPlatform}}, nil
		}
	}
	return gateway.Principal{}, signatureMismatch()
}

// signedParts checks the signing parameters and returns the signer, the
// credential scope, and the decoded signature.
func signedParts(query url.Values, now time.Time) (string, string, []byte, *gateway.RESTResponse) {
	for _, name := range signedParameters {
		if query.Get(name) == "" {
			return "", "", nil, signedInvalid("Missing " + name)
		}
	}
	if algorithm := query.Get("X-Goog-Algorithm"); algorithm != signatureAlgorithm {
		return "", "", nil, signedInvalid("Unsupported X-Goog-Algorithm " + algorithm + "; HMAC keys are not implemented")
	}
	signed, refused := signedWindow(query, now)
	if refused != nil {
		return "", "", nil, refused
	}
	account, scope, refused := credentialScope(query.Get("X-Goog-Credential"), signed)
	if refused != nil {
		return "", "", nil, refused
	}
	signature, err := hex.DecodeString(query.Get(paramSignature))
	if err != nil {
		return "", "", nil, signatureMismatch()
	}
	return account, scope, signature, nil
}

func signedInvalid(message string) *gateway.RESTResponse {
	return Error(400, "InvalidArgument", "Invalid argument.", message)
}

func signatureMismatch() *gateway.RESTResponse {
	return Error(403, "SignatureDoesNotMatch", "Access denied.",
		"The request signature we calculated does not match the signature you provided. Check your Google secret key and signing method.")
}

// signedWindow parses X-Goog-Date and X-Goog-Expires and refuses a request
// outside the window they define.
func signedWindow(query url.Values, now time.Time) (time.Time, *gateway.RESTResponse) {
	signed, err := time.Parse(signedDateLayout, query.Get("X-Goog-Date"))
	if err != nil {
		return time.Time{}, signedInvalid("X-Goog-Date must be YYYYMMDD'T'HHMMSS'Z'")
	}
	expires, err := strconv.Atoi(query.Get("X-Goog-Expires"))
	if err != nil || expires < 1 || expires > maxSignedExpiry {
		return time.Time{}, signedInvalid(fmt.Sprintf("X-Goog-Expires must be between 1 and %d seconds", maxSignedExpiry))
	}
	if signed.After(now.Add(signedClockSkew)) {
		return time.Time{}, signedInvalid("X-Goog-Date is in the future")
	}
	if expiry := signed.Add(time.Duration(expires) * time.Second); now.After(expiry) {
		return time.Time{}, Error(400, "ExpiredToken", "The provided token has expired.", "Request signature expired at: "+expiry.UTC().Format(time.RFC3339))
	}
	return signed, nil
}

// credentialScope splits EMAIL/DATE/LOCATION/storage/goog4_request and checks
// that the scope's date is the signing date.
func credentialScope(credential string, signed time.Time) (string, string, *gateway.RESTResponse) {
	parts := strings.Split(credential, "/")
	if len(parts) != 5 || parts[0] == "" || parts[2] == "" || parts[3] != "storage" || parts[4] != "goog4_request" ||
		parts[1] != signed.Format("20060102") {
		return "", "", signedInvalid("X-Goog-Credential must be EMAIL/DATE/LOCATION/storage/goog4_request, dated the signing day")
	}
	return parts[0], strings.Join(parts[1:], "/"), nil
}

// canonicalRequest rebuilds the string the client signed: the method, the
// escaped path as sent, the sorted strictly-encoded query without the
// signature, the signed headers, and the payload hash.
func canonicalRequest(r *gateway.RESTRequest, host string) (string, *gateway.RESTResponse) {
	names := strings.Split(r.Query.Get("X-Goog-SignedHeaders"), ";")
	if !slices.Contains(names, "host") || !slices.IsSorted(names) || len(slices.Compact(slices.Clone(names))) != len(names) {
		return "", signedInvalid("X-Goog-SignedHeaders must be sorted, unique, lowercase, and include host")
	}
	headers, payload, refused := canonicalHeaders(r, host, names)
	if refused != nil {
		return "", refused
	}
	return strings.Join([]string{r.Verb, r.RawPath, canonicalQuery(r.Query), headers, strings.Join(names, ";"), payload}, "\n"), nil
}

// canonicalHeaders renders each signed header as name:value lines, with
// whitespace runs collapsed, and returns the signed payload hash.
func canonicalHeaders(r *gateway.RESTRequest, host string, names []string) (string, string, *gateway.RESTResponse) {
	var headers strings.Builder
	payload := UnsignedPayload
	for _, name := range names {
		value := host
		if name != "host" {
			values := slices.Clone(r.Headers.Values(name))
			if name != strings.ToLower(name) || len(values) == 0 {
				return "", "", signedInvalid("Signed header " + name + " is missing from the request")
			}
			for i, item := range values {
				values[i] = strings.Join(strings.Fields(item), " ")
			}
			value = strings.Join(values, ",")
		}
		if name == headerPayloadHash {
			payload = value
		}
		headers.WriteString(name + ":" + value + "\n")
	}
	return headers.String(), payload, nil
}

// canonicalQuery sorts the strictly encoded query pairs, without the signature.
func canonicalQuery(query url.Values) string {
	var pairs []string
	for key, values := range query {
		if key == paramSignature {
			continue
		}
		for _, value := range values {
			pairs = append(pairs, strictEscape(key)+"="+strictEscape(value))
		}
	}
	sort.Strings(pairs)
	return strings.Join(pairs, "&")
}

// unreserved is RFC 3986's unreserved character set.
const unreserved = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-._~"

// strictEscape percent-encodes every byte outside the unreserved set.
func strictEscape(value string) string {
	var out strings.Builder
	for _, b := range []byte(value) {
		if strings.IndexByte(unreserved, b) >= 0 {
			out.WriteByte(b)
			continue
		}
		fmt.Fprintf(&out, "%%%02X", b)
	}
	return out.String()
}
