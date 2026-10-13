package run

import (
	"maps"
	"net/url"
	"regexp"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// serviceName is the provider and backend registry name.
const serviceName = "run"

// uidPattern pins the accepted Cloud Run identifier shape.
var uidPattern = regexp.MustCompile(`^[a-z][a-z0-9-]{0,62}$`)

// serviceResource builds the canonical full resource name of a Cloud Run v2
// service, the index and IAM target name.
func serviceResource(project, location, id string) string {
	return "//run.googleapis.com/projects/" + url.PathEscape(project) +
		"/locations/" + url.PathEscape(location) + "/services/" + url.PathEscape(id)
}

// modeledService are the google.cloud.run.v2.Service input fields the
// provider models, by JSON path. Every other input field is refused with a
// gap naming it unless unset (provider.Unmodeled): service-level scaling, VPC
// access, resource limits, probes, CMEK, Binary Authorization, volumes, and
// the rest. ingress takes only the values validateService accepts.
var modeledService = []string{
	"name", "description", "labels", "annotations", "client", "clientVersion", "launchStage",
	"ingress", "traffic",
	"template.revision", "template.serviceAccount",
	"template.scaling.minInstanceCount", "template.scaling.maxInstanceCount", "template.maxInstanceRequestConcurrency",
	"template.containers.image", "template.containers.ports.containerPort", "template.containers.ports.name",
	"template.containers.env.name", "template.containers.env.value",
}

// unmodeledService is the gap list DecodeProto refuses.
var unmodeledService = provider.Unmodeled(&runpb.Service{}, modeledService...)

// decodeService strictly decodes a services.create or services.update body,
// which binds the request's service field.
func decodeService(body []byte) (*runpb.Service, error) {
	service := &runpb.Service{}
	if err := provider.DecodeProto(body, "service", service, gap, unmodeledService...); err != nil {
		return nil, err
	}
	return service, nil
}

// serviceSpec is a validated service request: its metadata, routing, and
// template.
type serviceSpec struct {
	Description, Client, ClientVersion, LaunchStage string
	Labels, Annotations                             map[string]string
	Ingress                                         string
	Traffic                                         []trafficTarget
	Template                                        templateShape
}

// Ingress settings the provider enforces. INGRESS_TRAFFIC_NONE is a gap.
const (
	ingressAll        = "INGRESS_TRAFFIC_ALL"
	ingressNone       = "INGRESS_TRAFFIC_NONE"
	requestCreateName = "CreateServiceRequest"
	requestUpdateName = "UpdateServiceRequest"
)

// validateService checks a decoded service and returns its spec. request
// names the request message, as GCP's scaling violations do.
func validateService(service *runpb.Service, request string) (serviceSpec, error) {
	ingress := service.GetIngress().String()
	if service.GetIngress() == runpb.IngressTraffic_INGRESS_TRAFFIC_UNSPECIFIED {
		ingress = ingressAll
	}
	if ingress == ingressNone {
		return serviceSpec{}, coverageGap("ingress " + ingressNone)
	}
	traffic, err := validateTraffic(service.GetTraffic())
	if err != nil {
		return serviceSpec{}, err
	}
	template, err := resolveTemplate(service.GetTemplate())
	if err != nil {
		return serviceSpec{}, err
	}
	if template.Scaling, err = validateScaling(service.GetTemplate(), request); err != nil {
		return serviceSpec{}, err
	}
	stage := service.GetLaunchStage().String()
	if service.GetLaunchStage() == 0 {
		stage = "GA"
	}
	return serviceSpec{
		Description: service.GetDescription(), Client: service.GetClient(), ClientVersion: service.GetClientVersion(),
		LaunchStage: stage, Labels: service.GetLabels(), Annotations: service.GetAnnotations(),
		Ingress: ingress, Traffic: traffic, Template: template,
	}, nil
}

// templateShape is the validated revision template of a service request.
type templateShape struct {
	Revision string
	// RevisionNamed records that the caller chose the revision name; GCP echoes
	// template.revision only then and reports generated names elsewhere.
	RevisionNamed  bool
	ServiceAccount string
	Container      containerSpec
	Scaling        scalingShape
}

// sameTemplate reports whether a request's template deploys what the
// service's current one does. GCP creates a revision only when the template
// changes (recording run-traffic); a request naming its revision must name
// the current one.
func sameTemplate(current, requested templateShape) bool {
	if requested.RevisionNamed && requested.Revision != current.Revision {
		return false
	}
	return current.ServiceAccount == requested.ServiceAccount && current.Scaling == requested.Scaling &&
		current.Container.Image == requested.Container.Image && current.Container.Port == requested.Container.Port &&
		maps.Equal(current.Container.Env, requested.Container.Env)
}

// containerSpec is one validated container of a revision template.
type containerSpec struct {
	Image string
	Port  int
	Env   map[string]string
}

// resolveTemplate validates the single-container revision template.
func resolveTemplate(template *runpb.RevisionTemplate) (templateShape, error) {
	if template == nil {
		return templateShape{}, status.Error(codes.InvalidArgument, "A service requires a revision template")
	}
	shape := templateShape{Revision: template.GetRevision(), ServiceAccount: template.GetServiceAccount()}
	// The v2 API names the revision identity by its email, without a member prefix.
	if account := shape.ServiceAccount; account != "" && !serviceAccountEmail.MatchString(account) {
		return templateShape{}, status.Errorf(codes.InvalidArgument, "serviceAccount must be a service account email, got %q", account)
	}
	container, err := resolveContainer(template.GetContainers())
	if err != nil {
		return templateShape{}, err
	}
	shape.Container = container
	return shape, nil
}

// resolveContainer validates the template's one container. Sidecars are a
// gap; GCP accepts up to ten containers.
func resolveContainer(containers []*runpb.Container) (containerSpec, error) {
	switch {
	case len(containers) == 0:
		return containerSpec{}, status.Error(codes.InvalidArgument, "A revision requires a container")
	case len(containers) > 1:
		return containerSpec{}, coverageGap("more than one container (sidecars)")
	}
	first := containers[0]
	image := first.GetImage()
	if image == "" || len(image) > maxImageLength {
		return containerSpec{}, status.Errorf(codes.InvalidArgument, "A revision requires a container image of at most %d characters", maxImageLength)
	}
	port, err := containerPort(first.GetPorts())
	if err != nil {
		return containerSpec{}, err
	}
	env := map[string]string{}
	for _, entry := range first.GetEnv() {
		if entry.GetName() == "" {
			return containerSpec{}, status.Error(codes.InvalidArgument, "A container environment variable requires a name")
		}
		env[entry.GetName()] = entry.GetValue()
	}
	return containerSpec{Image: image, Port: port, Env: env}, nil
}

// containerPort reads the serving port, default 8080. http1 is the default
// protocol; h2c end-to-end HTTP/2 is not modeled.
func containerPort(ports []*runpb.ContainerPort) (int, error) {
	switch {
	case len(ports) > 1:
		return 0, status.Error(codes.InvalidArgument, "A container exposes at most one port")
	case len(ports) == 0:
		return 8080, nil
	}
	if name := ports[0].GetName(); name != "" && name != "http1" {
		return 0, coverageGap("port name " + name)
	}
	if ports[0].GetContainerPort() > 0 {
		return int(ports[0].GetContainerPort()), nil
	}
	return 8080, nil
}

// serviceAccountEmail matches the bare service-account email a template names.
var serviceAccountEmail = regexp.MustCompile(`^[a-z][a-z0-9-]{4,28}[a-z0-9]@[a-z0-9.-]+\.iam\.gserviceaccount\.com$`)

// maxImageLength bounds the accepted container image reference length.
const maxImageLength = 2048

// Shared wire JSON keys of the Cloud Run v2 document shapes.
const keyName, keyType, keyState = "name", "type", "state"

// permissions follows Google's documented Cloud Run access-control table;
// operations.wait reads the operation (run.operations.get).
var permissions = map[string]string{
	methodServicesCreate:   "run.services.create",
	methodServicesGet:      "run.services.get",
	methodServicesList:     "run.services.list",
	methodServicesUpdate:   "run.services.update",
	methodServicesDelete:   "run.services.delete",
	methodServicesGetIam:   "run.services.getIamPolicy",
	methodServicesSetIam:   "run.services.setIamPolicy",
	methodRevisionsList:    "run.revisions.list",
	methodRevisionsGet:     "run.revisions.get",
	methodRevisionsDelete:  "run.revisions.delete",
	methodOperationsGet:    "run.operations.get",
	methodOperationsList:   "run.operations.list",
	methodOperationsDelete: "run.operations.delete",
	methodOperationsWait:   "run.operations.get",
	methodServicesInvoke:   "run.routes.invoke",
	methodServicesActAs:    "iam.serviceAccounts.actAs",
}

// Method name constants; the gateway route table uses the same identifiers.
const (
	methodServicesCreate   = "run.services.create"
	methodServicesGet      = "run.services.get"
	methodServicesList     = "run.services.list"
	methodServicesUpdate   = "run.services.update"
	methodServicesDelete   = "run.services.delete"
	methodServicesGetIam   = "run.services.getIamPolicy"
	methodServicesSetIam   = "run.services.setIamPolicy"
	methodRevisionsList    = "run.revisions.list"
	methodRevisionsGet     = "run.revisions.get"
	methodRevisionsDelete  = "run.revisions.delete"
	methodOperationsGet    = "run.operations.get"
	methodOperationsList   = "run.operations.list"
	methodOperationsDelete = "run.operations.delete"
	methodOperationsWait   = "run.operations.wait"
	methodServicesInvoke   = "run.services.invoke"
)
