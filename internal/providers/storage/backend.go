package storage

import (
	"context"
	"errors"
	"io"
	"net/http"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

// memoryBackend uses upstream's in-process transport. It never opens a listener,
// loads ADC, configures notifications, or forwards a URL to a network transport.
// Manager.Use owns access and serializes Stop/Reset with every request.
type memoryBackend struct{ server *fakestorage.Server }

func (b *memoryBackend) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true, Writer: io.Discard})
	if err == nil {
		b.server = server
	}
	return err
}
func (b *memoryBackend) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.server == nil {
		return errors.New("Storage backend is stopped")
	}
	return nil
}
func (b *memoryBackend) Stop(ctx context.Context) error {
	if b.server != nil {
		b.server.Stop()
		b.server = nil
	}
	return ctx.Err()
}
func (b *memoryBackend) Reset(ctx context.Context) error { return b.Stop(ctx) }
func (b *memoryBackend) request(r *http.Request) (*http.Response, error) {
	if err := b.Ready(r.Context()); err != nil {
		return nil, err
	}
	return b.server.HTTPClient().Transport.RoundTrip(r)
}
