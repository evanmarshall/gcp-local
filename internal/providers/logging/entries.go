package logging

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"maps"
	"net/url"
	"regexp"
	"slices"
	"sort"
	"strconv"
	"strings"

	"cloud.google.com/go/logging/apiv2/loggingpb"
	"github.com/evanmarshall/gcp-local/internal/gateway"
	"github.com/evanmarshall/gcp-local/internal/providers/provider"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// logIDPattern is the documented LOG_ID character set after URL decoding.
var logIDPattern = regexp.MustCompile(`^[A-Za-z0-9/_.\-]{1,511}$`)

// escapeLogID renders a log ID in its URL-encoded resource-name form.
func escapeLogID(id string) string { return url.PathEscape(id) }

// parseLogName splits "projects/{project}/logs/{LOG_ID}" into the project and
// the decoded log ID.
func (p *Provider) parseLogName(name string) (string, string, error) {
	parent, encoded, ok := strings.Cut(name, "/logs/")
	if !ok {
		return "", "", status.Errorf(codes.InvalidArgument, "Invalid log name %q", name)
	}
	project, err := p.project(parent)
	if err != nil {
		return "", "", err
	}
	id, err := url.PathUnescape(encoded)
	if err != nil || !logIDPattern.MatchString(id) {
		return "", "", status.Errorf(codes.InvalidArgument, "Invalid log name %q", name)
	}
	return project, id, nil
}

// logName renders the stored log's resource name.
func logName(project, id string) string { return "projects/" + project + "/logs/" + escapeLogID(id) }

// pending is one validated entry waiting for authorization.
type pending struct {
	project, logID string
	entry          *loggingpb.LogEntry
}

// prepareEntry applies the request defaults to one entry and validates it.
func (p *Provider) prepareEntry(index int, source *loggingpb.LogEntry, request *loggingpb.WriteLogEntriesRequest, now *timestamppb.Timestamp) (pending, error) {
	entry := proto.CloneOf(source)
	name := entry.GetLogName()
	if name == "" {
		name = request.GetLogName()
	}
	if name == "" {
		return pending{}, status.Errorf(codes.InvalidArgument, "Log entry %d has no log name", index)
	}
	project, id, err := p.parseLogName(name)
	if err != nil {
		return pending{}, err
	}
	if entry.GetResource() == nil && request.GetResource() != nil {
		entry.Resource = proto.CloneOf(request.GetResource())
	}
	if entry.GetResource().GetType() == "" {
		return pending{}, status.Errorf(codes.InvalidArgument, "Log entry %d has no monitored resource type", index)
	}
	// Cloud Logging fills in the project of a "global" resource that omits it.
	if entry.GetResource().GetType() == "global" && entry.GetResource().GetLabels()["project_id"] == "" {
		if entry.Resource.Labels == nil {
			entry.Resource.Labels = map[string]string{}
		}
		entry.Resource.Labels["project_id"] = project
	}
	if len(request.GetLabels()) > 0 {
		labels := maps.Clone(request.GetLabels())
		maps.Copy(labels, entry.GetLabels())
		entry.Labels = labels
	}
	if entry.GetTimestamp() == nil {
		entry.Timestamp = now
	}
	entry.ReceiveTimestamp = now
	if entry.GetInsertId() == "" {
		entry.InsertId = insertID()
	}
	entry.LogName = ""
	return pending{project: project, logID: id, entry: entry}, nil
}

// prepare validates every entry and lists the distinct projects they name.
func (p *Provider) prepare(request *loggingpb.WriteLogEntriesRequest) ([]pending, []string, error) {
	if len(request.GetEntries()) == 0 {
		return nil, nil, status.Error(codes.InvalidArgument, "At least one log entry is required")
	}
	now := timestamppb.New(p.clock())
	entries := make([]pending, 0, len(request.GetEntries()))
	var projects []string
	for index, source := range request.GetEntries() {
		item, err := p.prepareEntry(index, source, request, now)
		if err != nil {
			return nil, nil, err
		}
		entries = append(entries, item)
		projects = appendUnique(projects, item.project)
	}
	return entries, projects, nil
}

// appendUnique adds a value that is not yet present.
func appendUnique(values []string, value string) []string {
	if slices.Contains(values, value) {
		return values
	}
	return append(values, value)
}

// store appends one project's entries, dropping the oldest beyond the cap.
func (p *Provider) store(project string, entries []pending) error {
	if err := p.scopes.With(provider.ProjectScope(project), func(s *scopeState) error {
		for _, item := range entries {
			if item.project != project {
				continue
			}
			s.seq++
			s.entries = append(s.entries, storedEntry{logID: item.logID, seq: s.seq, entry: item.entry})
		}
		if excess := len(s.entries) - maxEntriesPerProject; excess > 0 {
			s.entries = append([]storedEntry(nil), s.entries[excess:]...)
		}
		return nil
	}); err != nil {
		return fmt.Errorf("store log entries: %w", err)
	}
	return nil
}

// write validates every entry, authorizes each project it names, then stores
// them. Nothing is stored when any entry or project fails.
func (p *Provider) write(call gateway.Call, request *loggingpb.WriteLogEntriesRequest) (*loggingpb.WriteLogEntriesResponse, error) {
	entries, projects, err := p.prepare(request)
	if err != nil {
		return nil, err
	}
	if err := p.authorize(call, projects); err != nil {
		return nil, err
	}
	if request.GetDryRun() {
		return &loggingpb.WriteLogEntriesResponse{}, nil
	}
	for _, project := range projects {
		if err := p.store(project, entries); err != nil {
			return nil, err
		}
	}
	return &loggingpb.WriteLogEntriesResponse{}, nil
}

// insertID generates a unique entry ID when the writer supplies none.
func insertID() string {
	raw := make([]byte, 8)
	_, _ = rand.Read(raw)
	return hex.EncodeToString(raw)
}

// listed is one entry selected for a response, with its owning project.
type listed struct {
	project string
	stored  storedEntry
}

// listQuery is one validated entries.list request.
type listQuery struct {
	projects    []string
	descending  bool
	filter      *node
	size        int
	offset      int
	fingerprint string
}

// projects resolves resource names to distinct project IDs.
func (p *Provider) projects(names []string) ([]string, error) {
	if len(names) == 0 {
		return nil, status.Error(codes.InvalidArgument, "resource_names must name at least one project")
	}
	var projects []string
	for _, name := range names {
		project, err := p.project(name)
		if err != nil {
			return nil, err
		}
		projects = appendUnique(projects, project)
	}
	return projects, nil
}

// query validates an entries.list request before authorization.
func (p *Provider) query(request *loggingpb.ListLogEntriesRequest) (listQuery, error) {
	var q listQuery
	var err error
	if q.projects, err = p.projects(request.GetResourceNames()); err != nil {
		return q, err
	}
	if q.descending, err = orderOf(request.GetOrderBy()); err != nil {
		return q, err
	}
	if q.filter, err = parseFilter(request.GetFilter()); err != nil {
		return q, err
	}
	if q.size, err = pageSize(request.GetPageSize(), defaultEntryPage, maxEntryPage); err != nil {
		return q, err
	}
	q.fingerprint = strings.Join(append(append([]string{}, q.projects...), request.GetFilter(), request.GetOrderBy()), "\x00")
	q.offset, err = decodeToken(request.GetPageToken(), q.fingerprint)
	return q, err
}

// matching collects the filter's matches across the query's projects.
func (p *Provider) matching(q listQuery) ([]listed, error) {
	var matches []listed
	for _, project := range q.projects {
		if err := p.scopes.With(provider.ProjectScope(project), func(s *scopeState) error {
			for _, stored := range s.entries {
				if q.filter.matches(view{project: project, stored: stored}) {
					matches = append(matches, listed{project: project, stored: stored})
				}
			}
			return nil
		}); err != nil {
			return nil, fmt.Errorf("read log entries: %w", err)
		}
	}
	sort.SliceStable(matches, func(i, j int) bool {
		left, right := matches[i].stored, matches[j].stored
		a, b := left.entry.GetTimestamp().AsTime(), right.entry.GetTimestamp().AsTime()
		if !a.Equal(b) {
			return a.Before(b) != q.descending
		}
		return (left.seq < right.seq) != q.descending
	})
	return matches, nil
}

// list returns the matching entries of every named project in timestamp order.
func (p *Provider) list(call gateway.Call, request *loggingpb.ListLogEntriesRequest) (*loggingpb.ListLogEntriesResponse, error) {
	q, err := p.query(request)
	if err != nil {
		return nil, err
	}
	if err := p.authorize(call, q.projects); err != nil {
		return nil, err
	}
	matches, err := p.matching(q)
	if err != nil {
		return nil, err
	}
	response := &loggingpb.ListLogEntriesResponse{}
	end := min(q.offset+q.size, len(matches))
	for _, item := range matches[min(q.offset, len(matches)):end] {
		entry := proto.CloneOf(item.stored.entry)
		entry.LogName = logName(item.project, item.stored.logID)
		response.Entries = append(response.Entries, entry)
	}
	if end < len(matches) {
		response.NextPageToken = encodeToken(end, q.fingerprint)
	}
	return response, nil
}

// orderOf accepts the two documented timestamp orders.
func orderOf(order string) (bool, error) {
	switch strings.Join(strings.Fields(strings.ToLower(order)), " ") {
	case "", fieldTimestamp, fieldTimestamp + " asc":
		return false, nil
	case fieldTimestamp + " desc":
		return true, nil
	default:
		return false, status.Errorf(codes.InvalidArgument, "Unsupported order_by %q: use \"timestamp asc\" or \"timestamp desc\"", order)
	}
}

// pageSize applies the default and rejects a negative or oversized page.
func pageSize(requested int32, fallback, limit int) (int, error) {
	switch {
	case requested < 0:
		return 0, status.Error(codes.InvalidArgument, "page_size must not be negative")
	case requested == 0:
		return fallback, nil
	default:
		return min(int(requested), limit), nil
	}
}

// encodeToken binds an offset to the request it continues.
func encodeToken(offset int, fingerprint string) string {
	sum := sha256.Sum256([]byte(fingerprint))
	return base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(offset) + ":" + hex.EncodeToString(sum[:8])))
}

// decodeToken reads an offset, rejecting a token from a different request.
func decodeToken(token, fingerprint string) (int, error) {
	if token == "" {
		return 0, nil
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	offset, hash, ok := strings.Cut(string(raw), ":")
	value, parseErr := strconv.Atoi(offset)
	sum := sha256.Sum256([]byte(fingerprint))
	if err != nil || !ok || parseErr != nil || value < 0 || hash != hex.EncodeToString(sum[:8]) {
		return 0, status.Error(codes.InvalidArgument, "Invalid page token")
	}
	return value, nil
}

// logsQuery validates a logs.list request before authorization.
func (p *Provider) logsQuery(request *loggingpb.ListLogsRequest) (string, int, int, error) {
	project, err := p.project(request.GetParent())
	if err != nil {
		return "", 0, 0, err
	}
	for _, name := range request.GetResourceNames() {
		if other, err := p.project(name); err != nil || other != project {
			return "", 0, 0, status.Error(codes.InvalidArgument, "resource_names must match the parent project")
		}
	}
	size, err := pageSize(request.GetPageSize(), defaultLogPage, defaultLogPage)
	if err != nil {
		return "", 0, 0, err
	}
	offset, err := decodeToken(request.GetPageToken(), "logs\x00"+project)
	return project, size, offset, err
}

// logNames lists the distinct logs of one project in name order.
func (p *Provider) logNames(project string) ([]string, error) {
	names := map[string]bool{}
	if err := p.scopes.With(provider.ProjectScope(project), func(s *scopeState) error {
		for _, stored := range s.entries {
			names[logName(project, stored.logID)] = true
		}
		return nil
	}); err != nil {
		return nil, fmt.Errorf("read logs: %w", err)
	}
	sorted := make([]string, 0, len(names))
	for name := range names {
		sorted = append(sorted, name)
	}
	sort.Strings(sorted)
	return sorted, nil
}

// listLogs names the logs of one project that hold entries.
func (p *Provider) listLogs(call gateway.Call, request *loggingpb.ListLogsRequest) (*loggingpb.ListLogsResponse, error) {
	project, size, offset, err := p.logsQuery(request)
	if err != nil {
		return nil, err
	}
	if err := p.authorize(call, []string{project}); err != nil {
		return nil, err
	}
	names, err := p.logNames(project)
	if err != nil {
		return nil, err
	}
	end := min(offset+size, len(names))
	response := &loggingpb.ListLogsResponse{LogNames: names[min(offset, len(names)):end]}
	if end < len(names) {
		response.NextPageToken = encodeToken(end, "logs\x00"+project)
	}
	return response, nil
}

// deleteLog removes every entry of one log.
func (p *Provider) deleteLog(call gateway.Call, request *loggingpb.DeleteLogRequest) error {
	project, id, err := p.parseLogName(request.GetLogName())
	if err != nil {
		return err
	}
	if err := p.authorize(call, []string{project}); err != nil {
		return err
	}
	removed := 0
	if err := p.scopes.With(provider.ProjectScope(project), func(s *scopeState) error {
		kept := s.entries[:0]
		for _, stored := range s.entries {
			if stored.logID == id {
				removed++
				continue
			}
			kept = append(kept, stored)
		}
		s.entries = kept
		return nil
	}); err != nil {
		return fmt.Errorf("delete log entries: %w", err)
	}
	if removed == 0 {
		return status.Errorf(codes.NotFound, "Log %s does not exist", logName(project, id))
	}
	return nil
}
