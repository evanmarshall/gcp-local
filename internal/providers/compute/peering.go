package compute

import (
	"errors"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// peeringRequest is one addPeering body. The current API nests the peering in
// networkPeering; the legacy form names it and its peerNetwork at top level.
type peeringRequest struct {
	name, network                  string
	exchange, importCustom, export bool
	exportPublic, importPublic     bool
}

// parsePeering reads either addPeering body form.
func parsePeering(body *peeringBody) peeringRequest {
	if nested := body.NetworkPeering; nested != nil {
		return peeringRequest{
			name: nested.Name, network: nested.Network,
			exchange: orTrue(nested.ExchangeSubnetRoutes), importCustom: bool(nested.ImportCustomRoutes),
			export: bool(nested.ExportCustomRoutes), exportPublic: orTrue(nested.ExportPublicIP),
			importPublic: bool(nested.ImportPublicIP),
		}
	}
	return peeringRequest{name: body.Name, network: body.PeerNetwork, exchange: orTrue(body.AutoCreateRoutes), exportPublic: true}
}

// orTrue reads an optional boolean that defaults to true.
func orTrue(value *flexBool) bool {
	return value == nil || bool(*value)
}

// peerProject reports the project a peer network reference names, or "" for
// a bare network name.
func peerProject(reference string) string {
	_, rest, found := strings.Cut(reference, "projects/")
	if !found {
		return ""
	}
	project, _, _ := strings.Cut(rest, "/")
	return project
}

// addPeering stores one peering of a network toward a same-project peer.
func (p *Provider) addPeering(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeBody[peeringBody](request.JSON, peeringSchema)
	if err != nil {
		return nil, err
	}
	spec := parsePeering(body)
	peer := networkName(spec.network)
	if !uidPattern.MatchString(spec.name) || peer == "" {
		return nil, provider.Invalid("A peering requires a name and a peer network")
	}
	if project := peerProject(spec.network); project != "" && project != scope.Project {
		return nil, gap("cross-project network peering is not modeled")
	}
	network := request.Path["network"]
	peering := peeringRecord{
		Name: spec.name, PeerNetwork: peer, ExchangeSubnetRoutes: spec.exchange,
		ImportCustomRoutes: spec.importCustom, ExportCustomRoutes: spec.export,
		ExportPublicIP: spec.exportPublic, ImportPublicIP: spec.importPublic, CreatedAt: p.clock(),
	}
	if err := p.scopes.With(scope, func(s *scopeState) error { return s.addPeering(network, peering) }); err != nil {
		switch {
		case errors.Is(err, errAlreadyExists):
			return nil, conflict(globalRef(scope.Project, "networks", network) + "/peerings/" + spec.name)
		case errors.Is(err, errMissing):
			return nil, notFound(globalRef(scope.Project, "networks", network))
		default:
			return nil, err //nolint:wrapcheck // addPeering returns wire statuses or sentinels
		}
	}
	return p.newOperation(scope, "global", "", "addPeering", selfLink(scope.Project, "networks", network))
}

// addPeering stores one peering unless the network already has a peering
// with that name or toward that peer. A missing peer is reported first.
func (s *scopeState) addPeering(network string, peering peeringRecord) error {
	if s.networks[peering.PeerNetwork] == nil {
		return missingReference()
	}
	record := s.networks[network]
	if record == nil {
		return errMissing
	}
	for _, existing := range record.Peerings {
		if existing.Name == peering.Name || existing.PeerNetwork == peering.PeerNetwork {
			return errAlreadyExists
		}
	}
	record.Peerings = append(record.Peerings, peering)
	return nil
}

// removePeering deletes one named peering of a network. Compute refuses an
// unknown peering name with 400.
func (p *Provider) removePeering(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	body, err := decodeBody[removePeeringBody](request.JSON, bodySchema{})
	if err != nil {
		return nil, err
	}
	name, network := body.Name, request.Path["network"]
	removed := false
	if err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.networks[network]
		if record == nil {
			return errMissing
		}
		for i, existing := range record.Peerings {
			if existing.Name == name {
				record.Peerings = append(record.Peerings[:i], record.Peerings[i+1:]...)
				removed = true
				return nil
			}
		}
		return nil
	}); err != nil {
		return nil, notFound(globalRef(scope.Project, "networks", network))
	}
	if !removed {
		return nil, status.Errorf(codes.InvalidArgument, "There is no peering named '%s' in network '%s'", name, network)
	}
	return p.newOperation(scope, "global", "", "removePeering", selfLink(scope.Project, "networks", network))
}

// renderNetwork renders one network with its subnetworks and peerings. The
// caller holds the scope lock.
func (s *scopeState) renderNetwork(project string, record *networkRecord) map[string]any {
	peerings := make([]any, 0, len(record.Peerings))
	for _, peering := range record.Peerings {
		peerings = append(peerings, peeringDocument(project, peering, s.peeredBack(record.Name, peering.PeerNetwork)))
	}
	return networkDocument(project, record, s.subnetworksOf(project, record.Name), peerings)
}

// peeredBack reports whether the peer network has a peering toward network:
// a peering is ACTIVE only once both sides exist.
func (s *scopeState) peeredBack(network, peer string) bool {
	record := s.networks[peer]
	if record == nil {
		return false
	}
	for _, candidate := range record.Peerings {
		if candidate.PeerNetwork == network {
			return true
		}
	}
	return false
}

// peeringDocument renders one peering with the recorded field set.
func peeringDocument(project string, record peeringRecord, active bool) map[string]any {
	state, detail := "INACTIVE", "Waiting for peer network to connect."
	if active {
		state, detail = "ACTIVE", "Connected."
	}
	return map[string]any{
		keyName:                          record.Name,
		keyNetwork:                       selfLink(project, "networks", record.PeerNetwork),
		keyState:                         state,
		"stateDetails":                   "[" + stamp(record.CreatedAt) + "]: " + detail,
		"autoCreateRoutes":               true,
		"exchangeSubnetRoutes":           record.ExchangeSubnetRoutes,
		"exportCustomRoutes":             record.ExportCustomRoutes,
		"importCustomRoutes":             record.ImportCustomRoutes,
		"exportSubnetRoutesWithPublicIp": record.ExportPublicIP,
		"importSubnetRoutesWithPublicIp": record.ImportPublicIP,
		keyStackType:                     stackIPv4Only,
		"updateStrategy":                 strategyIndependent,
		"connectionStatus": map[string]any{
			"trafficConfiguration": map[string]any{
				"exportCustomRoutesToPeer":               record.ExportCustomRoutes,
				"importCustomRoutesFromPeer":             record.ImportCustomRoutes,
				"exportSubnetRoutesWithPublicIpToPeer":   record.ExportPublicIP,
				"importSubnetRoutesWithPublicIpFromPeer": record.ImportPublicIP,
				keyStackType:                             stackIPv4Only,
			},
			"updateStrategy": strategyIndependent,
		},
	}
}
