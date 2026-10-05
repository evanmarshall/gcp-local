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

func (g *Gateway) newCall(ctx context.Context, method string, authorization, trace []string) (Call, error) {
	call := Call{Context: ctx, Method: method, Location: "global", Principal: Principal{State: Anonymous}}
	id := make([]byte, 16)
	if _, err := rand.Read(id); err != nil {
		return call, status.Error(codes.Internal, "Unable to create trace context")
	}
	call.TraceID = hex.EncodeToString(id)
	if len(trace) == 1 {
		if match := traceParent.FindStringSubmatch(trace[0]); match != nil && match[1] != "00000000000000000000000000000000" {
			call.TraceID = match[1]
		}
	}
	if len(authorization) == 0 {
		return call, nil
	}
	fields := strings.Fields(authorization[0])
	if len(authorization) != 1 || len(fields) != 2 || !strings.EqualFold(fields[0], "Bearer") {
		return call, status.Error(codes.Unauthenticated, "Malformed authorization header")
	}
	if g.resolve == nil {
		call.Principal.State = Unknown
		return call, nil
	}
	principal, err := g.resolve(ctx, fields[1])
	if err != nil {
		return call, normalizeError(err)
	}
	switch principal.State {
	case Authenticated:
		if principal.Subject == "" {
			return call, status.Error(codes.Internal, "Identity resolver returned no subject")
		}
	case Unknown, Expired:
	default:
		return call, status.Error(codes.Internal, "Identity resolver returned an invalid principal state")
	}
	call.Principal = principal
	return call, nil
}
