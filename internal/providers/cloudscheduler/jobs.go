package cloudscheduler

import (
	"encoding/json"
	"fmt"
	"math/rand/v2"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"cloud.google.com/go/scheduler/apiv1/schedulerpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"github.com/evanmarshall/gcp-local/internal/state"
	"github.com/robfig/cron/v3"
	"google.golang.org/genproto/googleapis/rpc/status"
	"google.golang.org/grpc/codes"
	grpcstatus "google.golang.org/grpc/status"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// wirePrefix is the full resource-name prefix of the index and IAM targets.
const wirePrefix = "//cloudscheduler.googleapis.com/"

// maxPageSize is the documented ListJobs maximum and default page size.
const maxPageSize = 500

// notAttempted is the status GCP reports on a job that has not run yet
// (recording cloudscheduler-job-lifecycle).
const notAttempted = -1

// jobRecord is one stored job. job holds every wire field, output fields
// included; schedule and zone are its parsed schedule.
type jobRecord struct {
	job      *schedulerpb.Job
	schedule *cron.SpecSchedule
	zone     *time.Location
	run      bool          // a RunJob request awaits the worker
	wake     chan struct{} // nudges the worker after a change
}

// nudge wakes the job's worker without blocking.
func (r *jobRecord) nudge() {
	select {
	case r.wake <- struct{}{}:
	default:
	}
}

// jobName builds the relative job name GCP returns on the wire.
func jobName(project, location, id string) string {
	return "projects/" + url.PathEscape(project) + "/locations/" + url.PathEscape(location) + "/jobs/" + url.PathEscape(id)
}

// jobResource builds the full job name of the index and IAM targets.
func jobResource(project, location, id string) string {
	return wirePrefix + jobName(project, location, id)
}

// execute dispatches one authorized method.
func (p *Provider) execute(call gateway.Call, request *gateway.RESTRequest) (*gateway.RESTResponse, error) {
	scope := provider.ProjectScope(request.Path["project"])
	location, id := request.Path["location"], request.Path["job"]
	switch call.Method {
	case methodCreate:
		return p.createJob(call, request, scope)
	case methodList:
		return p.listJobs(request, scope)
	case methodGet:
		return p.withJob(scope, location, id, func(*jobRecord) error { return nil })
	case methodPatch:
		return p.patchJob(call, request, scope)
	case methodDelete:
		return p.deleteJob(scope, location, id)
	case methodPause:
		return p.withJob(scope, location, id, p.pause)
	case methodResume:
		return p.withJob(scope, location, id, p.resume)
	case methodRun:
		return p.withJob(scope, location, id, func(record *jobRecord) error {
			record.run = true
			record.nudge()
			return nil
		})
	}
	return nil, gap("this method is not implemented")
}

// decodeJob reads a REST job body. Unknown fields are refused, as GCP does.
func decodeJob(body []byte) (*schedulerpb.Job, error) {
	job := &schedulerpb.Job{}
	if len(body) == 0 {
		return job, nil
	}
	if err := protojson.Unmarshal(body, job); err != nil {
		return nil, provider.Invalid("Invalid job: " + err.Error())
	}
	return job, nil
}

// render returns the job's REST document.
func render(job *schedulerpb.Job) (map[string]any, error) {
	raw, err := protojson.Marshal(job)
	if err != nil {
		return nil, fmt.Errorf("encode the job: %w", err)
	}
	var document map[string]any
	if err := json.Unmarshal(raw, &document); err != nil {
		return nil, fmt.Errorf("decode the job: %w", err)
	}
	return document, nil
}

// createJob validates, authorizes the token account, stores, indexes, and
// starts the job's worker.
func (p *Provider) createJob(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	job, err := decodeJob(request.JSON)
	if err != nil {
		return nil, err
	}
	location := request.Path["location"]
	id, err := jobID(job.GetName(), scope.Project, location)
	if err != nil {
		return nil, err
	}
	record, err := newRecord(job, scope.Project)
	if err != nil {
		return nil, err
	}
	if email := tokenAccount(job); email != "" {
		if err := p.authorizeActAs(call, scope.Project, email); err != nil {
			return nil, err
		}
	}
	now := p.clock.Now()
	job.Name = jobName(scope.Project, location, id)
	job.State, job.Status, job.LastAttemptTime = schedulerpb.Job_ENABLED, &status.Status{Code: notAttempted}, nil
	job.UserUpdateTime = timestamppb.New(now)
	job.ScheduleTime = timestamppb.New(record.schedule.Next(now.In(record.zone)))
	key := jobKey(location, id)
	var document map[string]any
	err = p.scopes.With(scope, func(s *scopeState) error {
		if s.jobs[key] != nil {
			return grpcstatus.Error(codes.AlreadyExists, "Job "+job.GetName()+" already exists")
		}
		var renderErr error
		if document, renderErr = render(job); renderErr != nil {
			return renderErr
		}
		s.jobs[key] = record
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	if err := p.index.Put(serviceName, state.Resource{
		Name: jobResource(scope.Project, location, id), Service: serviceName, Kind: "job",
		Parent: provider.ProjectResource(scope.Project), Scope: scope,
	}); err != nil {
		return nil, fmt.Errorf("index the job: %w", err)
	}
	p.launch(scope, key)
	return &gateway.RESTResponse{JSON: document}, nil
}

// jobIDLimit is the documented maximum job ID length.
const jobIDLimit = 500

// jobID reads the job ID from the requested name, which must be in the
// request's project and location; an empty name gets a generated ID.
func jobID(name, project, location string) (string, error) {
	if name == "" {
		return strconv.FormatUint(rand.Uint64N(9e18)+1e18, 10), nil
	}
	prefix := "projects/" + project + "/locations/" + location + "/jobs/"
	id, found := strings.CutPrefix(name, prefix)
	if !found || id == "" || len(id) > jobIDLimit || strings.IndexFunc(id, invalidIDRune) >= 0 {
		return "", provider.Invalid("Invalid job name " + name)
	}
	return id, nil
}

// invalidIDRune reports a rune outside letters, digits, hyphens, and underscores.
func invalidIDRune(r rune) bool {
	return (r < 'a' || r > 'z') && (r < 'A' || r > 'Z') && (r < '0' || r > '9') && r != '-' && r != '_'
}

// withJob reads or changes one existing job under the scope lock and renders it.
func (p *Provider) withJob(scope state.Scope, location, id string, use func(*jobRecord) error) (*gateway.RESTResponse, error) {
	var document map[string]any
	err := p.scopes.With(scope, func(s *scopeState) error {
		record := s.jobs[jobKey(location, id)]
		if record == nil {
			return provider.NotFound("jobs", id)
		}
		if err := use(record); err != nil {
			return err
		}
		var renderErr error
		document, renderErr = render(record.job)
		return renderErr
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	return &gateway.RESTResponse{JSON: document}, nil
}

// pause stops scheduling; a paused job reports no scheduleTime.
func (p *Provider) pause(record *jobRecord) error {
	record.job.State, record.job.ScheduleTime = schedulerpb.Job_PAUSED, nil
	record.job.UserUpdateTime = timestamppb.New(p.clock.Now())
	record.nudge()
	return nil
}

// resume schedules the next execution from now.
func (p *Provider) resume(record *jobRecord) error {
	now := p.clock.Now()
	record.job.State, record.job.UserUpdateTime = schedulerpb.Job_ENABLED, timestamppb.New(now)
	record.job.ScheduleTime = timestamppb.New(record.schedule.Next(now.In(record.zone)))
	record.nudge()
	return nil
}

// listJobs returns one page of the location's jobs in name order. The page
// token is the offset of the next page.
func (p *Provider) listJobs(request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	size, offset, err := pageWindow(request.Query.Get("pageSize"), request.Query.Get("pageToken"))
	if err != nil {
		return nil, err
	}
	var jobs []*schedulerpb.Job
	total := 0
	_ = p.scopes.With(scope, func(s *scopeState) error {
		jobs, total = locationPage(s, request.Path["location"], offset, size)
		return nil
	})
	items := make([]any, 0, len(jobs))
	for _, job := range jobs {
		document, err := render(job)
		if err != nil {
			return nil, err
		}
		items = append(items, document)
	}
	response := map[string]any{}
	if len(items) > 0 {
		response["jobs"] = items
	}
	if offset+size < total {
		response["nextPageToken"] = strconv.Itoa(offset + size)
	}
	return &gateway.RESTResponse{JSON: response}, nil
}

// locationPage copies one page of a location's jobs in name order and counts
// them all; call it under the scope lock.
func locationPage(s *scopeState, location string, offset, size int) ([]*schedulerpb.Job, int) {
	keys := make([]string, 0, len(s.jobs))
	for key := range s.jobs {
		if strings.HasPrefix(key, location+"/") {
			keys = append(keys, key)
		}
	}
	sort.Strings(keys)
	var page []*schedulerpb.Job
	for i := offset; i < len(keys) && i < offset+size; i++ {
		page = append(page, proto.CloneOf(s.jobs[keys[i]].job))
	}
	return page, len(keys)
}

// pageWindow validates the page size (default and maximum 500) and token.
func pageWindow(sizeText, token string) (int, int, error) {
	size := maxPageSize
	if sizeText != "" {
		parsed, err := strconv.Atoi(sizeText)
		if err != nil || parsed < 0 {
			return 0, 0, provider.Invalid("Invalid pageSize " + sizeText)
		}
		if parsed > 0 && parsed < maxPageSize {
			size = parsed
		}
	}
	offset := 0
	if token != "" {
		parsed, err := strconv.Atoi(token)
		if err != nil || parsed < 0 {
			return 0, 0, provider.Invalid("Invalid pageToken")
		}
		offset = parsed
	}
	return size, offset, nil
}

// deleteJob removes one job and stops its worker. GCP returns an empty body.
func (p *Provider) deleteJob(scope state.Scope, location, id string) (*gateway.RESTResponse, error) {
	key := jobKey(location, id)
	err := p.scopes.With(scope, func(s *scopeState) error {
		if s.jobs[key] == nil {
			return provider.NotFound("jobs", id)
		}
		delete(s.jobs, key)
		return nil
	})
	if err != nil {
		return nil, err //nolint:wrapcheck // status errors carry the wire status
	}
	p.stopWorker(scope, key)
	_ = p.index.Remove(serviceName, scope, jobResource(scope.Project, location, id))
	return &gateway.RESTResponse{JSON: map[string]any{}}, nil
}

// patchJob applies the update mask, re-validates the merged job, and
// re-authorizes a token account the update names.
func (p *Provider) patchJob(call gateway.Call, request *gateway.RESTRequest, scope state.Scope) (*gateway.RESTResponse, error) {
	update, err := decodeJob(request.JSON)
	if err != nil {
		return nil, err
	}
	location, id := request.Path["location"], request.Path["job"]
	if update.GetName() != "" && update.GetName() != jobName(scope.Project, location, id) {
		return nil, provider.Invalid("The job name does not match the request path")
	}
	paths, err := maskPaths(request.Query.Get("updateMask"), update)
	if err != nil {
		return nil, err
	}
	var current *schedulerpb.Job
	if _, err := p.withJob(scope, location, id, func(record *jobRecord) error {
		current = proto.CloneOf(record.job)
		return nil
	}); err != nil {
		return nil, err
	}
	replacement, err := newRecord(applyMask(current, update, paths), scope.Project)
	if err != nil {
		return nil, err
	}
	if email := tokenAccount(replacement.job); email != "" && touchesTarget(paths) {
		if err := p.authorizeActAs(call, scope.Project, email); err != nil {
			return nil, err
		}
	}
	return p.withJob(scope, location, id, func(record *jobRecord) error {
		p.replace(record, replacement)
		return nil
	})
}

// replace installs an updated job, keeping its state and attempt outputs,
// and reschedules it from now.
func (p *Provider) replace(record, replacement *jobRecord) {
	now, job := p.clock.Now(), replacement.job
	job.State, job.Status, job.LastAttemptTime = record.job.GetState(), record.job.GetStatus(), record.job.GetLastAttemptTime()
	job.UserUpdateTime, job.ScheduleTime = timestamppb.New(now), nil
	if job.GetState() == schedulerpb.Job_ENABLED {
		job.ScheduleTime = timestamppb.New(replacement.schedule.Next(now.In(replacement.zone)))
	}
	record.job, record.schedule, record.zone = job, replacement.schedule, replacement.zone
	record.nudge()
}
