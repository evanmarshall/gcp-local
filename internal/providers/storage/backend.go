package storage

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"

	"github.com/fsouza/fake-gcs-server/fakestorage"
)

// memoryBackend uses upstream's in-process transport. It never opens a listener,
// loads ADC, configures notifications, or forwards a URL to a network transport.
// Manager.Use owns access and serializes Stop/Reset with every request.
type memoryBackend struct {
	server *fakestorage.Server
	// metagenerations holds the metageneration of each patched object, keyed
	// by bucket and name. Upstream reports every object at metageneration 1;
	// GCP advances it on each metadata update. An entry applies only to the
	// generation it names, so a replaced object starts again at 1. Manager.Use
	// serializes access, and Stop clears it with the objects.
	metagenerations map[string]objectVersion
}

// objectVersion is one object generation's provider-owned metageneration.
type objectVersion struct {
	generation     string
	metageneration int64
}

func (b *memoryBackend) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	server, err := fakestorage.NewServerWithOptions(fakestorage.Options{NoListener: true, Writer: io.Discard})
	if err != nil {
		return fmt.Errorf("start the storage backend: %w", err)
	}
	b.server, b.metagenerations = server, map[string]objectVersion{}
	return nil
}

// errBackendStopped reports a readiness probe after Stop.
var errBackendStopped = errors.New("storage backend is stopped")

func (b *memoryBackend) Ready(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if b.server == nil {
		return errBackendStopped
	}
	return nil
}
func (b *memoryBackend) Stop(ctx context.Context) error {
	if b.server != nil {
		b.server.Stop()
		b.server, b.metagenerations = nil, nil
	}
	return ctx.Err()
}
func (b *memoryBackend) Reset(ctx context.Context) error { return b.Stop(ctx) }
func (b *memoryBackend) request(r *http.Request) (*http.Response, error) {
	if err := b.Ready(r.Context()); err != nil {
		return nil, err
	}
	response, err := b.server.HTTPClient().Transport.RoundTrip(r)
	if err != nil {
		return nil, fmt.Errorf("forward to the storage backend: %w", err)
	}
	return response, nil
}

// metageneration returns an object generation's metageneration: 1 until a
// patch advances it.
func (b *memoryBackend) metageneration(bucket, name, generation string) string {
	if version, ok := b.metagenerations[bucket+"/"+name]; ok && version.generation == generation {
		return strconv.FormatInt(version.metageneration, 10)
	}
	return "1"
}

// advanceMetageneration records a successful metadata update of one object
// generation. It runs only after the backend accepted the update.
func (b *memoryBackend) advanceMetageneration(bucket, name, generation string) {
	current, _ := strconv.ParseInt(b.metageneration(bucket, name, generation), 10, 64)
	b.metagenerations[bucket+"/"+name] = objectVersion{generation: generation, metageneration: current + 1}
}
