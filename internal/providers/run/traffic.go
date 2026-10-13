package run

import (
	"cmp"
	"fmt"
	"math/rand/v2"
	"regexp"
	"slices"
	"strings"
	"sync"

	"cloud.google.com/go/run/apiv2/runpb"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Traffic target allocation types of google.cloud.run.v2.TrafficTarget.
const (
	latestTraffic   = "TRAFFIC_TARGET_ALLOCATION_TYPE_LATEST"
	revisionTraffic = "TRAFFIC_TARGET_ALLOCATION_TYPE_REVISION"
)

// trafficTarget is one validated entry of a service's traffic block.
type trafficTarget struct {
	Type, Revision, Tag string
	Percent             int32
}

// trafficStatus is one entry of a service's trafficStatuses: the routing in
// effect after the newest successful rollout.
type trafficStatus struct {
	Type, Revision, Tag string
	Percent             int32
}

// allLatest is GCP's default traffic block: everything to the latest ready
// revision.
var allLatest = []trafficTarget{{Type: latestTraffic, Percent: 100}}

// tagPattern is the accepted tag shape: a DNS label short enough to prefix
// the service host with "---". Not recorded.
var tagPattern = regexp.MustCompile(`^[a-z]([a-z0-9-]{0,44}[a-z0-9])?$`)

// validateTraffic checks a traffic block the way GCP does before storing it:
// percents sum to 100 and tags are unique, with GCP's messages (recording
// run-traffic). A revision that doesn't exist is not checked here: GCP stores
// the block and fails the rollout (resolveStatuses). The shape checks of a
// single target are not recorded.
func validateTraffic(targets []*runpb.TrafficTarget) ([]trafficTarget, error) {
	if len(targets) == 0 {
		return slices.Clone(allLatest), nil
	}
	validated := make([]trafficTarget, 0, len(targets))
	tags := map[string]bool{}
	var sum int32
	for _, target := range targets {
		entry, err := checkTarget(target)
		if err != nil {
			return nil, err
		}
		if entry.Tag != "" && tags[entry.Tag] {
			return nil, provider.BadRequest("spec.traffic: may not contain a duplicate tag.", "spec.traffic", "may not contain a duplicate tag.") //nolint:wrapcheck // the wire status
		}
		tags[entry.Tag] = entry.Tag != ""
		sum += entry.Percent
		validated = append(validated, entry)
	}
	if sum != 100 {
		description := fmt.Sprintf("traffic percentage adds to %d, should be 100", sum)
		return nil, provider.BadRequest("traffic[].percent: "+description, "traffic[].percent", description) //nolint:wrapcheck // the wire status
	}
	return validated, nil
}

// checkTarget validates one target's shape; GCP's wording isn't recorded.
func checkTarget(target *runpb.TrafficTarget) (trafficTarget, error) {
	entry := trafficTarget{Type: target.GetType().String(), Revision: target.GetRevision(), Tag: target.GetTag(), Percent: target.GetPercent()}
	switch {
	case target.GetType() == runpb.TrafficTargetAllocationType_TRAFFIC_TARGET_ALLOCATION_TYPE_UNSPECIFIED:
		return trafficTarget{}, status.Error(codes.InvalidArgument, "traffic[].type is required")
	case entry.Type == latestTraffic && entry.Revision != "":
		return trafficTarget{}, status.Error(codes.InvalidArgument, "traffic[].revision must be empty for the latest revision")
	case entry.Type == revisionTraffic && entry.Revision == "":
		return trafficTarget{}, status.Error(codes.InvalidArgument, "traffic[].revision is required")
	case entry.Percent < 0 || entry.Percent > 100:
		return trafficTarget{}, status.Error(codes.InvalidArgument, "traffic[].percent must be between 0 and 100")
	case entry.Tag != "" && !tagPattern.MatchString(entry.Tag):
		return trafficTarget{}, status.Errorf(codes.InvalidArgument, "traffic[].tag %q is not a valid tag", entry.Tag)
	}
	return entry, nil
}

// errMissingRevision is the rollout failure for a traffic block naming a
// revision that doesn't exist (recording run-traffic).
func errMissingRevision(revision string) error {
	return status.Errorf(codes.FailedPrecondition,
		"Revision '%s' does not exist or is deleted. Update all traffic to route to currently available revisions.", revision)
}

// resolveStatuses turns a traffic block into the statuses GCP reports once it
// is in effect: LATEST resolves to the latest ready revision, targets of one
// revision and type merge into one entry (percents add, the first tag
// names it), and entries sort by revision (recording run-traffic). A second
// tag on one revision gets its own entry; that is not recorded.
func resolveStatuses(targets []trafficTarget, latestReady string, exists func(string) bool) ([]trafficStatus, error) {
	var statuses []trafficStatus
	merged := map[[2]string]int{}
	for _, target := range targets {
		revision := target.Revision
		if target.Type == latestTraffic {
			revision = latestReady
		} else if !exists(revision) {
			return nil, errMissingRevision(revision)
		}
		key := [2]string{target.Type, revision}
		if at, ok := merged[key]; ok && (target.Tag == "" || statuses[at].Tag == "") {
			statuses[at].Percent += target.Percent
			statuses[at].Tag = cmp.Or(statuses[at].Tag, target.Tag)
			continue
		}
		merged[key] = len(statuses)
		statuses = append(statuses, trafficStatus{Type: target.Type, Revision: revision, Tag: target.Tag, Percent: target.Percent})
	}
	slices.SortStableFunc(statuses, func(a, b trafficStatus) int { return strings.Compare(a.Revision, b.Revision) })
	return statuses, nil
}

// trafficDocuments renders a traffic block; proto3 JSON omits a zero percent.
func trafficDocuments(targets []trafficTarget) []any {
	documents := make([]any, 0, len(targets))
	for _, target := range targets {
		document := map[string]any{keyType: target.Type}
		optional(document, "revision", target.Revision)
		optional(document, "tag", target.Tag)
		if target.Percent != 0 {
			document["percent"] = target.Percent
		}
		documents = append(documents, document)
	}
	return documents
}

// statusDocuments renders trafficStatuses; a tagged entry carries its tag URL.
func statusDocuments(statuses []trafficStatus, tagURI func(tag string) string) []any {
	documents := make([]any, 0, len(statuses))
	for _, entry := range statuses {
		document := map[string]any{keyType: entry.Type}
		optional(document, "revision", entry.Revision)
		if entry.Percent != 0 {
			document["percent"] = entry.Percent
		}
		if entry.Tag != "" {
			document["tag"], document["uri"] = entry.Tag, tagURI(entry.Tag)
		}
		documents = append(documents, document)
	}
	return documents
}

// tagSeparator joins a tag to its service's host, as GCP's tag URLs do.
const tagSeparator = "---"

// splitTag separates a tag URL host into its tag and the service host.
func splitTag(host string) (string, string) {
	if tag, base, found := strings.Cut(host, tagSeparator); found {
		return tag, base
	}
	return "", host
}

// router picks a revision for each request to a service URL by a weighted
// choice over the percents in effect. The sequence is deterministic for a
// seed, so tests can assert it.
type router struct {
	mu     sync.Mutex
	random *rand.Rand
}

func newRouter(seed uint64) *router {
	return &router{random: rand.New(rand.NewPCG(seed, seed))}
}

// pick returns the revision one request goes to: the status whose cumulative
// percent first exceeds a uniform draw from [0, 100). Statuses without a
// percent (tag-only) get none.
func (r *router) pick(statuses []trafficStatus) string {
	r.mu.Lock()
	draw := int32(r.random.IntN(100)) //nolint:gosec // a percent fits int32
	r.mu.Unlock()
	var cumulative int32
	for _, entry := range statuses {
		cumulative += entry.Percent
		if draw < cumulative {
			return entry.Revision
		}
	}
	return ""
}

// tagged returns the revision a tag routes to.
func tagged(statuses []trafficStatus, tag string) string {
	for _, entry := range statuses {
		if entry.Tag == tag {
			return entry.Revision
		}
	}
	return ""
}

// serves reports whether the revision receives a percent of the traffic in
// effect; GCP refuses to delete it then (recording run-traffic).
func serves(statuses []trafficStatus, revision string) bool {
	return slices.ContainsFunc(statuses, func(entry trafficStatus) bool { return entry.Revision == revision && entry.Percent > 0 })
}

// referenced reports whether the traffic in effect names the revision, with
// a percent or a tag.
func referenced(statuses []trafficStatus, revision string) bool {
	return slices.ContainsFunc(statuses, func(entry trafficStatus) bool { return entry.Revision == revision })
}

// Revision scaling defaults and bounds (recording run-traffic: GCP echoes the
// default concurrency 80 and refuses 1001; RevisionScaling documents the
// default of 100 instances).
const (
	defaultConcurrency  = 80
	maxConcurrency      = 1000
	defaultMaxInstances = 100
)

// scalingShape is a revision's validated scaling: instance bounds and
// requests per instance. Zero means unset.
type scalingShape struct {
	MinInstances, MaxInstances, Concurrency int32
}

// validateScaling checks template scaling with GCP's messages, which name the
// request message (recording run-traffic, for UpdateServiceRequest).
func validateScaling(template *runpb.RevisionTemplate, request string) (scalingShape, error) {
	shape := scalingShape{
		MinInstances: template.GetScaling().GetMinInstanceCount(), MaxInstances: template.GetScaling().GetMaxInstanceCount(),
		Concurrency: template.GetMaxInstanceRequestConcurrency(),
	}
	prefix := "Violation in " + request + ".service.template."
	switch {
	case shape.MinInstances < 0 || shape.MaxInstances < 0:
		return scalingShape{}, status.Error(codes.InvalidArgument, "template.scaling instance counts must not be negative")
	case shape.MaxInstances > 0 && shape.MinInstances > shape.MaxInstances:
		return scalingShape{}, provider.BadRequest(prefix+"scaling.max_instance_count: must be greater or equal than min_instance_count.", //nolint:wrapcheck // the wire status
			prefix+"scaling.max_instance_count", "must be greater or equal than min_instance_count.")
	case shape.Concurrency < 0 || shape.Concurrency > maxConcurrency:
		return scalingShape{}, provider.BadRequest(prefix+"max_instance_request_concurrency: Must be a number between 0 and 1000.", //nolint:wrapcheck // the wire status
			prefix+"max_instance_request_concurrency", "Must be a number between 0 and 1000.")
	}
	return shape, nil
}

// concurrency is the requests one instance takes at once; 0 is the default.
func (s scalingShape) concurrency() int32 { return cmp.Or(s.Concurrency, defaultConcurrency) }

// capacity is the requests a revision serves at once: its instances times
// their concurrency. Locally one process stands in for every instance.
func (s scalingShape) capacity() int {
	return int(cmp.Or(s.MaxInstances, defaultMaxInstances) * s.concurrency())
}

// scalingDocument renders template.scaling; GCP omits it when unset
// (recording run-traffic).
func (s scalingShape) document() map[string]any {
	document := map[string]any{}
	if s.MinInstances > 0 {
		document["minInstanceCount"] = s.MinInstances
	}
	if s.MaxInstances > 0 {
		document["maxInstanceCount"] = s.MaxInstances
	}
	return document
}

// errNoInstance is GCP's documented answer when a revision is at its maximum
// instances and each is at its concurrency (Cloud Run troubleshooting, HTTP
// 429). Not recorded.
var errNoInstance = status.Error(codes.ResourceExhausted, "The request was aborted because there was no available instance.")
