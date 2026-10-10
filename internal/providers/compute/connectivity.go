package compute

import (
	"encoding/json"
	"strings"
	"time"

	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// connectivityRequest is the posted Connectivity Tests flow specification.
type connectivityRequest struct {
	Name   string `json:"name"`
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
func (p *Provider) executeConnectivity(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	switch call.Method {
	case methodConnectivityCreate:
		return p.createConnectivity(request, scope)
	case methodConnectivityList:
		return p.listConnectivity(scope)
	case methodConnectivityGet:
		return p.getConnectivity(request, scope)
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

// namedTest is one stored connectivity test with its flow specification.
type namedTest struct {
	Name      string    `json:"name"`
	Source    string    `json:"source"`
	Dest      string    `json:"destination"`
	Protocol  string    `json:"protocol"`
	Port      int       `json:"port"`
	CreatedAt time.Time `json:"createTime"`
}

// createConnectivity stores a named test and immediately evaluates it.
func (p *Provider) createConnectivity(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var spec connectivityRequest
	if json.Unmarshal(request.JSON, &spec) != nil {
		return nil, status.Error(codes.InvalidArgument, "Invalid connectivity test specification")
	}
	name := lastSegment(spec.Name)
	if name == "" {
		return nil, status.Error(codes.InvalidArgument, "A connectivity test requires a name")
	}
	record := &namedTest{
		Name: name, Source: spec.Source.IPAddress, Dest: spec.Destination.IPAddress,
		Protocol: strings.ToLower(strings.TrimSpace(spec.Protocol)), Port: spec.Port,
		CreatedAt: time.Now(),
	}
	var conflict bool
	_ = p.scopes.With(scope, func(s *scopeState) error {
		if s.connectivity[name] == nil {
			s.connectivity[name] = record
		} else {
			conflict = true
		}
		return nil
	})
	if conflict {
		return nil, status.Errorf(codes.AlreadyExists, "connectivity test %q already exists", name)
	}
	verdict := p.analyze(reachabilityRequest{
		Project: request.Path["project"], SourceIP: record.Source, DestIP: record.Dest,
		Protocol: record.Protocol, Port: record.Port,
	})
	return &gateway.RESTResponse{JSON: p.connectivityDocument(scope.Project+"/locations/global/connectivityTests/"+name, verdict)}, nil
}

// getConnectivity re-evaluates one stored test.
func (p *Provider) getConnectivity(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	var record *namedTest
	_ = p.scopes.With(scope, func(s *scopeState) error {
		record = s.connectivity[request.Path["test"]]
		return nil
	})
	if record == nil {
		return nil, status.Errorf(codes.NotFound, "connectivity test %q is not found", request.Path["test"])
	}
	verdict := p.analyze(reachabilityRequest{
		Project: request.Path["project"], SourceIP: record.Source, DestIP: record.Dest,
		Protocol: record.Protocol, Port: record.Port,
	})
	return &gateway.RESTResponse{JSON: p.connectivityDocument(scope.Project+"/locations/global/connectivityTests/"+record.Name, verdict)}, nil
}

// listConnectivity returns the stored tests with fresh evaluations.
func (p *Provider) listConnectivity(scope state.Scope) (*gateway.RESTResponse, error) {
	var records []*namedTest
	_ = p.scopes.With(scope, func(s *scopeState) error {
		for _, record := range s.connectivity {
			records = append(records, record)
		}
		return nil
	})
	items := make([]any, 0, len(records))
	for _, record := range records {
		verdict := p.analyze(reachabilityRequest{
			Project: scope.Project, SourceIP: record.Source, DestIP: record.Dest,
			Protocol: record.Protocol, Port: record.Port,
		})
		items = append(items, p.connectivityDocument(scope.Project+"/locations/global/connectivityTests/"+record.Name, verdict))
	}
	return &gateway.RESTResponse{JSON: map[string]any{"connectivityTests": items}}, nil
}

// lastSegment extracts the trailing identifier of a full resource name.
func lastSegment(name string) string {
	if index := strings.LastIndex(name, "/"); index >= 0 {
		return name[index+1:]
	}
	return name
}
