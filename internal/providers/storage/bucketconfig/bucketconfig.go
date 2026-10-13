// Package bucketconfig validates the configurations Cloud Storage keeps as
// bucket and object metadata, with GCP's recorded errors, and decides what
// they allow. It holds no state: the Storage provider stores what it returns
// and acts on its decisions.
package bucketconfig

import "fmt"

// RefusalError is one Storage JSON API error: its HTTP status, errors[]
// reason, and message.
type RefusalError struct {
	Status          int
	Reason, Message string
}

func (r *RefusalError) Error() string { return r.Message }

func invalid(format string, args ...any) *RefusalError {
	return &RefusalError{Status: 400, Reason: "invalid", Message: fmt.Sprintf(format, args...)}
}

// forbidden is a 403 refusal with reason forbidden.
func forbidden(format string, args ...any) *RefusalError {
	return &RefusalError{Status: 403, Reason: "forbidden", Message: fmt.Sprintf(format, args...)}
}

// GapError names a configuration value GCP may accept that this package does
// not model; the provider refuses it as a Storage coverage gap.
type GapError string

func (g GapError) Error() string { return string(g) }
