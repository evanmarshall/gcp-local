package pubsub

import (
	"errors"

	"github.com/evanmarshall/gcp-local/internal/backend"
	"google.golang.org/protobuf/proto"
)

// errMessageType reports a protobuf clone whose concrete type changed, which
// would mean the descriptor and the Go type disagree.
var errMessageType = errors.New("cloned message has an unexpected type")

// errBackendType reports a backend that this provider did not register.
var errBackendType = errors.New("backend is not the pub/sub emulator")

// cloneMessage clones a protobuf message and preserves its concrete type. The
// assertion is checked so a descriptor/type mismatch is an error, not a panic.
func cloneMessage[T proto.Message](message T) (T, error) {
	var zero T
	clone, ok := proto.Clone(message).(T)
	if !ok {
		return zero, errMessageType
	}
	return clone, nil
}

// driverOf returns the emulator driver behind a manager-owned backend.
func driverOf(active backend.Backend) (driver, error) {
	value, ok := active.(driver)
	if !ok {
		return nil, errBackendType
	}
	return value, nil
}

// pushWorker reads a stored push worker, reporting a foreign value in the map.
func pushWorker(value any) (*pushSubscription, bool) {
	worker, ok := value.(*pushSubscription)
	return worker, ok
}
