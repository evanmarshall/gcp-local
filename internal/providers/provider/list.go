package provider

import (
	"encoding/base64"
	"net/url"
	"strconv"
)

// Paging is one list method's documented default and maximum page size and
// the service's recorded refusals of a page token Page didn't issue and of a
// negative page size. A nil refusal is a plain INVALID_ARGUMENT.
// NegativeDefault reads a negative size as the default instead, as Cloud Run
// does (recording run-service-lifecycle). FullPageToken returns a next
// token after every full page, even the last, as Cloud Tasks' task list
// does (recording cloudtasks-dispatch-retry); the next page is then empty.
type Paging struct {
	Default, Max               int
	InvalidToken, NegativeSize error
	NegativeDefault            bool
	FullPageToken              bool
}

// Page returns the page of items a list request asks for and the token of
// the next page, "" on the last page. items must already be in the API's
// documented order: by name unless the method's reference says otherwise.
//
//   - pageSize: absent or 0 is size.Default, above size.Max is size.Max;
//     negative is size.NegativeSize (size.Default with NegativeDefault) and
//     not an integer is GCP's 400 with a BadRequest violation.
//   - pageToken: only a token Page returned; anything else is
//     size.InvalidToken. A token past the end (items were deleted) is an
//     empty last page.
//   - filter and orderBy: a non-empty value is refused with gap, never
//     ignored. A provider that applies one itself names it in handled.
func Page[T any](items []T, query url.Values, size Paging, gap func(string) error, handled ...string) ([]T, string, error) {
	skip := names(handled)
	for _, option := range []string{"filter", "orderBy"} {
		if query.Get(option) != "" && !skip[option] {
			return nil, "", gap("list " + option + " is not implemented")
		}
	}
	limit, err := pageSize(query.Get("pageSize"), size)
	if err != nil {
		return nil, "", err
	}
	start, ok := pageStart(query.Get("pageToken"))
	if !ok {
		return nil, "", refusal(size.InvalidToken, "Invalid page_token")
	}
	start = min(start, len(items))
	end := min(start+limit, len(items))
	next := ""
	if end < len(items) || size.FullPageToken && end-start == limit {
		next = base64.RawURLEncoding.EncodeToString([]byte(strconv.Itoa(end)))
	}
	return items[start:end], next, nil
}

// pageSize reads the requested page size.
func pageSize(text string, size Paging) (int, error) {
	if text == "" {
		return size.Default, nil
	}
	parsed, err := strconv.Atoi(text)
	switch {
	case err != nil:
		return 0, FieldViolation("page_size", "Invalid value at 'page_size' (TYPE_INT32), "+strconv.Quote(text))
	case parsed < 0 && !size.NegativeDefault:
		return 0, refusal(size.NegativeSize, "Invalid argument: 'page_size'")
	case parsed > 0:
		return min(parsed, size.Max), nil
	default:
		return size.Default, nil
	}
}

// pageStart decodes a page token Page issued into its offset.
func pageStart(token string) (int, bool) {
	if token == "" {
		return 0, true
	}
	raw, err := base64.RawURLEncoding.DecodeString(token)
	offset, parseErr := strconv.Atoi(string(raw))
	return offset, err == nil && parseErr == nil && offset > 0 && strconv.Itoa(offset) == string(raw)
}

// refusal is a service's recorded refusal, or INVALID_ARGUMENT with message.
func refusal(recorded error, message string) error {
	if recorded != nil {
		return recorded
	}
	return Invalid(message)
}

// ListBody renders one page as the list response: field holds the items and
// nextPageToken the next page's token, each omitted when empty, as proto3
// JSON does.
func ListBody[T any](field string, items []T, next string) map[string]any {
	body := map[string]any{}
	if len(items) > 0 {
		body[field] = items
	}
	if next != "" {
		body["nextPageToken"] = next
	}
	return body
}
