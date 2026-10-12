package run

import (
	"encoding/json"
	"net/url"
	"regexp"

	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName is the provider and backend registry name.
const serviceName = "run"

// uidPattern pins the accepted Cloud Run identifier shape.
var uidPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// ingressSettings are Cloud Run's documented ingress values.
var ingressSettings = map[string]bool{
	"INGRESS_TRAFFIC_ALL": true, "INGRESS_TRAFFIC_INTERNAL_ONLY": true,
	"INGRESS_TRAFFIC_INTERNAL_LOAD_BALANCER": true,
}

// validateIngress accepts a documented ingress value. Ingress and analytical
// VPC behavior remain scoped in #16/#17; the setting is recorded here.
func validateIngress(value string) error {
	if value == "" || ingressSettings[value] {
		return nil
	}
	return status.Errorf(codes.InvalidArgument, "Invalid ingress setting %q", value)
}

// serviceResource builds the canonical Cloud Run v2 service resource name.
func serviceResource(project, location, id string) string {
	return "//run.googleapis.com/projects/" + url.PathEscape(project) +
		"/locations/" + url.PathEscape(location) + "/" + kindService + "/" + url.PathEscape(id)
}

// requestEnvelope is the decoded services.create or services.update body.
type requestEnvelope struct {
	Name        string         `json:"name"`
	Template    map[string]any `json:"template"`
	Ingress     string         `json:"ingress"`
	Labels      map[string]any `json:"labels"`
	Annotations map[string]any `json:"annotations"`
	LaunchStage string         `json:"launchStage"`
	Traffic     []trafficEntry `json:"traffic,omitempty"`
}

// trafficEntry is one requested traffic split entry.
type trafficEntry struct {
	Revision       string `json:"revision"`
	Percent        int    `json:"percent"`
	Tag            string `json:"tag,omitempty"`
	LatestRevision bool   `json:"latestRevision,omitempty"`
}

// decodeRequest reads one service body. Unknown fields fail before container work.
func decodeRequest(body []byte) (requestEnvelope, error) {
	var request requestEnvelope
	if json.Unmarshal(body, &request) != nil || len(request.Template) == 0 {
		return requestEnvelope{}, status.Error(codes.InvalidArgument, "Invalid service configuration")
	}
	return request, nil
}

// templateShape is the validated revision template of a service request.
type templateShape struct {
	Revision string
	// RevisionNamed records that the caller chose the revision name; GCP echoes
	// template.revision only then and reports generated names elsewhere.
	RevisionNamed  bool
	ServiceAccount string
	Container      containerSpec
}

// containerSpec is one validated container of a revision template.
type containerSpec struct {
	Image string
	Port  int
	Env   map[string]string
}

// resolveTemplate validates the single-container revision template. Multi-revision
// template features stay out until their cloud behavior is recorded.
func resolveTemplate(template map[string]any) (templateShape, error) {
	shape := templateShape{
		Revision:       provider.Text(template["revision"]),
		ServiceAccount: provider.Text(template["serviceAccount"]),
	}
	// The v2 API names the revision identity by its email, without a member prefix.
	if account := shape.ServiceAccount; account != "" && !serviceAccountEmail.MatchString(account) {
		return templateShape{}, status.Errorf(codes.InvalidArgument, "serviceAccount must be a service account email, got %q", account)
	}
	if volumes, _ := template["volumes"].([]any); len(volumes) != 0 {
		return templateShape{}, status.Error(codes.Unimplemented, "Revision volumes are not implemented")
	}
	container, refused := resolveContainer(template["containers"])
	if refused != nil {
		return templateShape{}, refused
	}
	shape.Container = container
	return shape, nil
}

// resolveContainer validates the single container entry of a template.
func resolveContainer(value any) (containerSpec, error) {
	containers, _ := value.([]any)
	if len(containers) != 1 {
		return containerSpec{}, status.Error(codes.InvalidArgument, "A revision requires exactly one container")
	}
	first, _ := containers[0].(map[string]any)
	if first == nil {
		return containerSpec{}, status.Error(codes.InvalidArgument, "Invalid container entry")
	}
	image := provider.Text(first["image"])
	if image == "" || len(image) > maxImageLength {
		return containerSpec{}, status.Errorf(codes.InvalidArgument, "A revision requires a container image of at most %d characters", maxImageLength)
	}
	port := 8080
	// The v2 API carries the serving port as ports[0].containerPort.
	if ports := provider.MapList(first["ports"]); len(ports) > 0 && number(ports[0]["containerPort"]) > 0 {
		port = int(number(ports[0]["containerPort"]))
	}
	env, refused := resolveEnv(first["env"])
	if refused != nil {
		return containerSpec{}, refused
	}
	return containerSpec{Image: image, Port: port, Env: env}, nil
}

// resolveEnv validates plain-text environment entries of a container.
func resolveEnv(value any) (map[string]string, error) {
	env := map[string]string{}
	for _, entry := range provider.MapList(value) {
		name := provider.Text(entry["name"])
		// valueSource is an object (a Secret Manager reference); any value is refused.
		if _, sourced := entry["valueSource"]; name == "" || sourced {
			return nil, status.Errorf(codes.InvalidArgument, "Container environment variable %q is unsupported in this slice", name)
		}
		env[name] = provider.Text(entry["value"])
	}
	return env, nil
}

// serviceAccountEmail matches the bare service-account email a template names.
var serviceAccountEmail = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z0-9.-]+\.iam\.gserviceaccount\.com$`)

// maxImageLength bounds the accepted container image reference length.
const maxImageLength = 2048

// number reads a JSON number field as a float.
func number(value any) float64 {
	if value, ok := value.(float64); ok {
		return value
	}
	return 0
}

// Shared wire JSON keys of the Cloud Run v2 error and document shapes.
const (
	keyError, keyCode, keyStatus, keyMessage = "error", "code", "status", "message"
	keyName, keyType                         = "name", "type"
)
