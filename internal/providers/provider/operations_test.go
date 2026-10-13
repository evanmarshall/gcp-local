package provider

import (
	"context"
	"errors"
	"net/url"
	"testing"
	"time"

	"github.com/evanmarshall/gcp-local/internal/state"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

var (
	regionA = state.Scope{Project: "p", Location: "us-central1"}
	regionB = state.Scope{Project: "p", Location: "europe-west1"}
)

func newTestOperations(latency time.Duration) *Operations {
	return NewOperations(func(state.Scope) error { return nil }, latency, func(op Operation) map[string]any {
		metadata := map[string]any{"@type": "test.Metadata", "verb": op.Verb, "target": op.Target}
		if op.Done {
			metadata["endTime"] = Stamp(op.Ended)
		}
		if op.CancelRequested {
			metadata["cancelRequested"] = true
		}
		return metadata
	})
}

func TestOperationIsNotDoneUntilWorkAndLatencyComplete(t *testing.T) {
	ops := newTestOperations(50 * time.Millisecond)
	id, err := ops.Start(regionA, Launch{Target: "projects/p/locations/us-central1/instances/i", Verb: "create"})
	if err != nil {
		t.Fatal(err)
	}
	time.Sleep(60 * time.Millisecond)
	if body, _ := ops.Get(regionA, id); body["done"] != false || body["response"] != nil {
		t.Fatalf("unfinished work reported %v", body)
	}
	ops.Finish(regionA, id, map[string]any{"@type": "test.Instance"}, nil)
	body, err := ops.Get(regionA, id)
	if err != nil || body["done"] != true || body["name"] != "projects/p/locations/us-central1/operations/"+id {
		t.Fatalf("finished after the latency: %v %v", body, err)
	}
	if metadata := object(t, body["metadata"]); metadata["endTime"] == nil || metadata["verb"] != "create" {
		t.Fatal(metadata)
	}
}

func TestOperationFailureWaitsForTheLatency(t *testing.T) {
	ops := newTestOperations(50 * time.Millisecond)
	quick, _ := ops.Start(regionA, Launch{Target: "t", Verb: "update"})
	ops.Finish(regionA, quick, nil, errFailure)
	if body, _ := ops.Get(regionA, quick); body["done"] != false {
		t.Fatalf("finished work reported done before the latency: %v", body)
	}
	body, err := ops.Wait(context.Background(), regionA, quick, time.Second)
	if err != nil || body["done"] != true || object(t, body["error"])["code"] != int(codes.Unknown) {
		t.Fatalf("wait: %v %v", body, err)
	}
}

var errFailure = errors.New("plain failure")

// object reads a rendered JSON object.
func object(t *testing.T, value any) map[string]any {
	t.Helper()
	result, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("not an object: %v", value)
	}
	return result
}

func TestOperationWaitTimesOutAndCancels(t *testing.T) {
	ops := newTestOperations(0)
	id, _ := ops.Start(regionA, Launch{Target: "t", Verb: "create"})
	if body, err := ops.Wait(context.Background(), regionA, id, 20*time.Millisecond); err != nil || body["done"] != false {
		t.Fatalf("timed-out wait: %v %v", body, err)
	}
	go func() {
		time.Sleep(10 * time.Millisecond)
		ops.Finish(regionA, id, map[string]any{"@type": "test.Instance"}, nil)
	}()
	if body, err := ops.Wait(context.Background(), regionA, id, time.Second); err != nil || body["done"] != true {
		t.Fatalf("woken wait: %v %v", body, err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	pending, _ := ops.Start(regionA, Launch{Target: "t", Verb: "create"})
	if _, err := ops.Wait(ctx, regionA, pending, time.Second); status.Code(err) != codes.Canceled {
		t.Fatalf("cancelled wait: %v", err)
	}
}

func TestOperationCancelAndDelete(t *testing.T) {
	ops := newTestOperations(time.Hour)
	running, _ := ops.Start(regionA, Launch{Target: "t", Verb: "create"})
	must(t, ops.Cancel(regionA, running))
	ops.Finish(regionA, running, map[string]any{"@type": "late"}, nil)
	body, _ := ops.Get(regionA, running)
	if body["done"] != true || object(t, body["error"])["code"] != int(codes.Canceled) || body["response"] != nil {
		t.Fatalf("cancelled running work: %v", body)
	}
	if object(t, body["metadata"])["cancelRequested"] != true {
		t.Fatal(body)
	}
	must(t, ops.Delete(regionA, running))
	for _, err := range []error{ops.Delete(regionA, running), ops.Cancel(regionA, running), func() error { _, err := ops.Get(regionA, running); return err }()} {
		if status.Code(err) != codes.NotFound {
			t.Fatalf("deleted operation: %v", err)
		}
	}
}

func TestOperationsListByScopeInCreationOrder(t *testing.T) {
	ops := newTestOperations(0)
	ids := make([]string, 0, 3)
	for range 3 {
		id, _ := ops.Start(regionA, Launch{Target: "t", Verb: "create"})
		ids = append(ids, id)
	}
	other, _ := ops.Start(regionB, Launch{Target: "t", Verb: "create"})
	body, err := ops.List(regionA, url.Values{"pageSize": {"2"}}, Paging{Default: 100, Max: 100}, testGap)
	if err != nil {
		t.Fatal(err)
	}
	page, _ := body["operations"].([]map[string]any)
	if len(page) != 2 || page[0]["name"] != OperationName(regionA, ids[0]) || body["nextPageToken"] == nil {
		t.Fatalf("first page: %v", body)
	}
	if _, err := ops.Get(regionA, other); status.Code(err) != codes.NotFound {
		t.Fatalf("another location's operation: %v", err)
	}
	if _, err := ops.List(regionA, url.Values{"filter": {"done=true"}}, Paging{Default: 100, Max: 100}, testGap); status.Code(err) != codes.Unimplemented {
		t.Fatalf("filter: %v", err)
	}
	must(t, ops.Reset(regionA))
	if body, _ := ops.List(regionA, url.Values{}, Paging{Default: 100, Max: 100}, testGap); len(body) != 0 {
		t.Fatalf("after reset: %v", body)
	}
	if _, err := ops.Get(regionB, other); err != nil {
		t.Fatalf("reset reached another location: %v", err)
	}
}

func TestOperationsKeepTheNewest(t *testing.T) {
	ops := newTestOperations(0)
	first, _ := ops.Start(regionA, Launch{Target: "t", Verb: "create"})
	ops.Finish(regionA, first, nil, nil)
	for range maxOperations {
		_, _ = ops.Start(regionA, Launch{Target: "t", Verb: "create"})
	}
	if _, err := ops.Get(regionA, first); status.Code(err) != codes.NotFound {
		t.Fatalf("oldest finished operation kept: %v", err)
	}
}

func must(t *testing.T, err error) {
	t.Helper()
	if err != nil {
		t.Fatal(err)
	}
}

func TestOperationsNamedAndImmediate(t *testing.T) {
	ops := newTestOperations(time.Hour).Named(func(_ state.Scope, target, id string) string { return "operations/" + target + "/operations/" + id })
	id, err := ops.Start(regionA, Launch{Target: "projects/p/instances/i", Verb: "update", Immediate: true})
	if err != nil {
		t.Fatal(err)
	}
	ops.Finish(regionA, id, map[string]any{"@type": "test.Instance"}, nil)
	body, err := ops.Get(regionA, id)
	if err != nil || body["done"] != true || body["name"] != "operations/projects/p/instances/i/operations/"+id {
		t.Fatalf("an immediate operation is done once finished: %v %v", body, err)
	}
	slow, _ := ops.Start(regionA, Launch{Target: "projects/p/instances/j", Verb: "create"})
	ops.Finish(regionA, slow, map[string]any{}, nil)
	if body, _ := ops.Get(regionA, slow); body["done"] != false {
		t.Fatalf("Start keeps the store's latency: %v", body)
	}
}

func TestOperationMetadataAndNewestFirst(t *testing.T) {
	operations := NewOperations(state.Scope.Validate, 0, func(op Operation) map[string]any {
		if op.Done {
			return op.Response
		}
		return op.Metadata
	})
	first, _ := operations.Start(regionA, Launch{Target: "t", Verb: "create", Metadata: map[string]any{"phase": "start"}})
	second, _ := operations.Start(regionA, Launch{Target: "t", Verb: "update"})
	operations.Finish(regionA, first, map[string]any{"phase": "done"}, nil)
	body, err := operations.Get(regionA, first)
	if err != nil || object(t, body["metadata"])["phase"] != "done" {
		t.Fatalf("finished operation = %v, %v", body, err)
	}
	listed, err := operations.NewestFirst().List(regionA, url.Values{}, Paging{Default: 10, Max: 10}, testGap)
	items, _ := listed["operations"].([]map[string]any)
	if err != nil || len(items) != 2 || items[0]["name"] != OperationName(regionA, second) {
		t.Fatalf("newest first = %v, %v", listed, err)
	}
}
