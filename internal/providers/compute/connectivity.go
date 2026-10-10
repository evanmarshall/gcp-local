package compute

import (
	"encoding/json"
	"strings"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connectivityRequest is the posted Connectivity Tests flow specification.
type connectivityRequest struct {
	Source struct {
		IPAddress string `json:"ipAddress"`
		Network   string `json:"network"`
	} `json:"source"`
	Destination struct {
		IPAddress string `json:"ipAddress"`
		Network   string `json:"network"`
	} `json:"destination"`
	Protocol string `json:"protocol"`
	Port     int    `json:"port"`
}

// connectivityDocument renders the Connectivity Tests wire shape. The local
// surface is modeled, not recorded: every response identifies itself as
// local-modeled and the analyzer's verdict semantics apply unchanged.
func (p *Provider) connectivityDocument(name string, verdict Reachability) map[string]any {
	result := "UNREACHABLE"
	switch verdict.Result {
	case "reachable":
		result = "REACHABLE"
	case verdictUnknown:
		result = "AMBIGUOUS"
	}
	return map[string]any{
		"name": name,
		"reachabilityDetails": map[string]any{
			"result":      result,
			"traces":      []any{map[string]any{"steps": verdict.Trace}},
			"verdict":     verdict.Result,
			"explanation": verdict.Reason,
		},
		"provenance": "local-modeled; no cloud recording backs this surface",
	}
}

// runConnectivity evaluates one posted flow as an ephemeral test.
func (p *Provider) runConnectivity(request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	var spec connectivityRequest
	if json.Unmarshal(request.JSON, &spec) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid connectivity test specification")
	}
	verdict := p.analyze(reachabilityRequest{
		Project:  request.Path["project"],
		SourceIP: strings.TrimSpace(spec.Source.IPAddress),
		DestIP:   strings.TrimSpace(spec.Destination.IPAddress),
		Protocol: strings.TrimSpace(strings.ToLower(spec.Protocol)),
		Port:     spec.Port,
	})
	return &gateway.RESTResponse{JSON: p.connectivityDocument("projects/"+request.Path["project"]+"/locations/global/connectivityTests/run", verdict)}, nil
}

// deleteConnectivity reports the slice boundary: tests are ephemeral here.
func (p *Provider) deleteConnectivity() (*gateway.RESTResponse, error) {
	return nil, status.Error(codes.Unimplemented, "Connectivity tests are ephemeral in this slice")
}

// executeConnectivity dispatches the connectivity test methods.
func (p *Provider) executeConnectivity(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodConnectivityRun:
		return p.runConnectivity(request)
	case methodConnectivityDelete:
		return p.deleteConnectivity()
	default:
		return nil, gap("this method is not implemented")
	}
}

// unusedState guards the state import while the scope model stays global.
var _ = state.GlobalScope
