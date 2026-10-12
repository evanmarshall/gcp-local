package gateway

import (
	"errors"
	"fmt"
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
