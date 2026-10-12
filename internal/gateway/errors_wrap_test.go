package gateway

import (
	"errors"
	"fmt"
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
