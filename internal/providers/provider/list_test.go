package provider

import (
	"net/url"
	"slices"
	"testing"

	"google.golang.org/grpc/codes"
)

func TestPageWalksEveryItemOnce(t *testing.T) {
	items := []string{"a", "b", "c", "d", "e"}
	var seen []string
	token, pages := "", 0
	for {
		page, next, err := Page(items, url.Values{"pageSize": {"2"}, "pageToken": {token}}, Paging{Default: 10, Max: 10}, testGap)
		if err != nil {
			t.Fatal(err)
		}
		seen, token, pages = append(seen, page...), next, pages+1
		if token == "" {
			break
		}
	}
	if !slices.Equal(seen, items) || pages != 3 {
		t.Fatalf("seen %v in %d pages", seen, pages)
	}
}

func TestPageSizeDefaultsAndCaps(t *testing.T) {
	items := make([]int, 30)
	for _, tc := range []struct {
		size string
		want int
	}{{"", 10}, {"0", 10}, {"3", 3}, {"500", 20}} {
		page, next, err := Page(items, url.Values{"pageSize": {tc.size}}, Paging{Default: 10, Max: 20}, testGap)
		if err != nil || len(page) != tc.want || next == "" {
			t.Fatalf("pageSize %q: %d items, next %q, %v", tc.size, len(page), next, err)
		}
	}
	page, next, err := Page(items[:20], url.Values{"pageSize": {"20"}}, Paging{Default: 10, Max: 20}, testGap)
	if err != nil || len(page) != 20 || next != "" {
		t.Fatalf("last page: %d items, next %q, %v", len(page), next, err)
	}
}

func TestPageRefusals(t *testing.T) {
	items := []int{1, 2, 3}
	_, stale, _ := Page(items, url.Values{"pageSize": {"2"}}, Paging{Default: 2, Max: 2}, testGap)
	if page, next, err := Page(items[:1], url.Values{"pageToken": {stale}}, Paging{Default: 2, Max: 2}, testGap); err != nil || len(page) != 0 || next != "" {
		t.Fatalf("token past the end: %v %q %v", page, next, err)
	}
	for _, tc := range []struct {
		query   url.Values
		code    codes.Code
		message string
	}{
		{url.Values{"pageSize": {"-1"}}, codes.InvalidArgument, "ListThingsRequest.pageSize cannot be negative."},
		{url.Values{"pageSize": {"-2"}, "default": {""}}, codes.InvalidArgument, "Invalid argument: 'page_size'"},
		{url.Values{"pageSize": {"ten"}}, codes.InvalidArgument, `Invalid value at 'page_size' (TYPE_INT32), "ten"`},
		{url.Values{"pageToken": {"2"}}, codes.InvalidArgument, "Invalid token"},
		{url.Values{"pageToken": {"2"}, "default": {""}}, codes.InvalidArgument, "Invalid page_token"},
		{url.Values{"pageToken": {"MA"}}, codes.InvalidArgument, "Invalid token"}, // offset 0 is never issued
		{url.Values{"filter": {"state=READY"}}, codes.Unimplemented, "Test coverage gap: list filter is not implemented"},
		{url.Values{"orderBy": {"name desc"}}, codes.Unimplemented, "Test coverage gap: list orderBy is not implemented"},
	} {
		paging := Paging{Default: 2, Max: 2, InvalidToken: Invalid("Invalid token"), NegativeSize: Invalid("ListThingsRequest.pageSize cannot be negative.")}
		if tc.query.Has("default") {
			paging = Paging{Default: 2, Max: 2}
		}
		code, message, _ := violation(t, func() error {
			_, _, err := Page(items, tc.query, paging, testGap)
			return err
		}())
		if code != tc.code || message != tc.message {
			t.Fatalf("%v: got %v %q", tc.query, code, message)
		}
	}
	if _, _, err := Page(items, url.Values{"filter": {"x"}}, Paging{Default: 2, Max: 2}, testGap, "filter"); err != nil {
		t.Fatalf("a handled filter: %v", err)
	}
}

func TestListBodyOmitsEmptyFields(t *testing.T) {
	if body := ListBody("jobs", []int{}, ""); len(body) != 0 {
		t.Fatal(body)
	}
	if body := ListBody("jobs", []int{1}, "t"); len(body) != 2 || body["nextPageToken"] != "t" {
		t.Fatal(body)
	}
}
