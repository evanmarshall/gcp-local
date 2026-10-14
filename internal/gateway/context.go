package gateway

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"regexp"
	"strings"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Resolver is supplied by the identity service. The default never trusts tokens.
type Resolver func(context.Context, string) (Principal, error)

var traceParent = regexp.MustCompile(`^00-([0-9a-f]{32})-[0-9a-f]{16}-[0-9a-f]{2}$`)

// traceID keeps a well-formed W3C traceparent, or generates a fresh ID.
func traceID(trace []string) string {
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return ""
	}
	generated := hex.EncodeToString(id)
	if len(trace) != 1 {
		return generated
	}
	if match := traceParent.FindStringSubmatch(trace[0]); match != nil && match[1] != "00000000000000000000000000000000" {
		return match[1]
	}
	return generated
}

// invalidCredentials is Google's 401 message for an Authorization header it
// can't use: another scheme, an empty bearer, or extra fields (probed on
// BigQuery, Compute, Cloud SQL, and Secret Manager, 2026-10-13).
const invalidCredentials = "Request had invalid authentication credentials. Expected OAuth 2 access token, login cookie or other valid" +
	" authentication credential. See https://developers.google.com/identity/sign-in/web/devconsole-project."

// bearerToken extracts the bearer credential from a single Authorization header.
func bearerToken(authorization []string) (string, bool) {
	fields := strings.Fields(authorization[0])
	if len(authorization) != 1 || len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return "", false
	}
	return fields[1], true
}

func (g *Gateway) newCall(ctx context.Context, method string, authorization, trace []string) (Call, error) {
	call := Call{Context: ctx, Method: method, Location: global, Principal: Principal{State: Anonymous}}
	call.TraceID = traceID(trace)
	if len(authorization) == 0 {
		return call, nil
	}
	bearer, ok := bearerToken(authorization)
	if !ok {
		return call, status.Error(codes.Unauthenticated, invalidCredentials)
	}
	if g.resolve == nil {
		call.Principal.State = Unknown
		return call, nil
	}
	principal, err := g.resolve(ctx, bearer)
	if err != nil {
		return call, normalizeError(err)
	}
	switch principal.State {
	case Authenticated:
		if principal.Subject == "" {
			return call, status.Error(codes.Internal, "Identity resolver returned no subject")
		}
	case Unknown, Expired, Anonymous:
	default:
		return call, status.Error(codes.Internal, "Identity resolver returned an invalid principal state")
	}
	call.Principal = principal
	return call, nil
}
