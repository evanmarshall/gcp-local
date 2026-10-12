package cloudsql

import (
	"context"
	"fmt"
	"net"

	"github.com/evanmarshall/gcp-local/internal/dataplane"
)

// publicKey names one instance's published listener.
func publicKey(project, instance string) string { return project + "/" + instance }

// openPublic gives a public-IP instance a port from the data-plane range,
// the local stand-in for its public address. Connections reach PostgreSQL's
// own password authentication with no IAM check, as public-IP connections
// in Cloud SQL do; the internal administrative role is refused. It returns
// nil when publishing is disabled or the instance has no public IP.
func (p *Provider) openPublic(ctx context.Context, project string, record instanceRecord) (*dataplane.Listener, error) {
	if p.pool == nil || !record.IPv4 {
		return nil, nil //nolint:nilnil // nothing to publish is the valid empty case
	}
	target := connectorTarget{project: project, region: record.Region, instance: record.Name}
	listener, err := p.pool.Open(ctx, func(ctx context.Context, conn net.Conn) {
		p.serve(ctx, target, conn, p.reachable)
	})
	if err != nil {
		return nil, fmt.Errorf("publish the instance address: %w", err)
	}
	return listener, nil
}

// trackPublic records an instance's listener so delete and reset can close it.
func (p *Provider) trackPublic(project, instance string, listener *dataplane.Listener) {
	if listener == nil {
		return
	}
	p.publicMu.Lock()
	defer p.publicMu.Unlock()
	p.public[publicKey(project, instance)] = listener
}

// closePublic closes one instance's listener and its forwarded sessions.
func (p *Provider) closePublic(project, instance string) {
	p.publicMu.Lock()
	listener := p.public[publicKey(project, instance)]
	delete(p.public, publicKey(project, instance))
	p.publicMu.Unlock()
	if listener != nil {
		listener.Close()
	}
}

// closeProjectPublic closes every listener of one project after its reset.
func (p *Provider) closeProjectPublic(project string) {
	p.publicMu.Lock()
	var closing []*dataplane.Listener
	for key, listener := range p.public {
		if len(key) > len(project) && key[:len(project)+1] == project+"/" {
			closing = append(closing, listener)
			delete(p.public, key)
		}
	}
	p.publicMu.Unlock()
	for _, listener := range closing {
		listener.Close()
	}
}
