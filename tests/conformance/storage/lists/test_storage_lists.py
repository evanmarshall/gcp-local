"""objects.list and buckets.list paging: delimiters, offsets, page tokens, and maxResults.

Seven objects under a "-" delimiter list as four prefixes and two items. The
pages show whether a prefix counts toward maxResults and whether a later page
repeats earlier prefixes. The refusals show GCP's answer to an invalid page
token and to out-of-range maxResults values.
"""

import platform

import pytest

from gcp_validation.services.storage.normalization import StorageObservations

NAMES = ("a-1", "a-2", "b-1", "c", "d-1", "e", "f-1")
MAX_PAGES = 8


@pytest.mark.gcp_scenario(
    id="storage-list-paging", service="storage", transport="rest-json-v1",
    versions={"sdk": "stdlib-http/" + platform.python_version() + ";google-auth/2.56.2",
              "provider": "not-applicable", "backend": "not-applicable"},
    contract_files=(
        "gcp_validation/services/storage/client.py",
        "gcp_validation/services/storage/normalization.py",
        "gcp_validation/sandbox.py",
        "gcp_validation/federation.py",
        "tests/conformance/storage/conftest.py",
    ),
)
def test_storage_list_paging(storage, gcp_snapshot):
    observed = StorageObservations(storage)
    for name in NAMES:
        storage.upload(name, name.encode()).require(200)

    def listing(query, path=storage.path + "/o"):
        # The fixture identity lists buckets; the allowed one has bucket roles only.
        actor = "fixture" if path == "/storage/v1/b" else "allowed"
        return storage.client.request(actor, "GET", path, query=query)

    def pages(query):
        found, token = [], None
        for _ in range(MAX_PAGES):
            page = listing({**query, **({"pageToken": token} if token else {})}).require(200)
            found.append({"items": [entry["name"] for entry in page.get("items", [])],
                          "prefixes": page.get("prefixes", []),
                          "has_next": "nextPageToken" in page})
            token = page.get("nextPageToken")
            if not token:
                return found
        raise AssertionError("Listing did not end within the bounded page count")

    gcp_snapshot.match("delimited-pages", pages({"delimiter": "-", "maxResults": 2}))
    gcp_snapshot.match("delimited-pages-of-four", pages({"delimiter": "-", "maxResults": 4}))
    gcp_snapshot.match("delimited-one-page", pages({"delimiter": "-"}))
    gcp_snapshot.match("flat-pages", pages({"maxResults": 3}))
    gcp_snapshot.match("prefix-delimited", pages({"prefix": "a-", "delimiter": "-"}))
    gcp_snapshot.match("prefix-collapsed", pages({"prefix": "a", "delimiter": "-"}))
    gcp_snapshot.match("offsets-delimited",
                       pages({"delimiter": "-", "startOffset": "b", "endOffset": "e"}))
    gcp_snapshot.match("offsets-inside-prefix",
                       pages({"delimiter": "-", "startOffset": "a-2", "endOffset": "d-1"}))

    def outcome(response):
        if response.status >= 400:
            return observed.error(response)
        page = response.json()
        return {"status": response.status, "count": len(page.get("items", [])),
                "has_next": "nextPageToken" in page, "has_unreachable": "unreachable" in page}

    for label, query in {
        "invalid-page-token": {"pageToken": "not-a-page-token"},
        "max-results-negative": {"maxResults": -1},
        "max-results-text": {"maxResults": "many"},
        "max-results-zero": {"maxResults": 0},
        "max-results-above-limit": {"maxResults": 1001},
    }.items():
        gcp_snapshot.match(label, outcome(listing(query)))

    def buckets(query):
        return outcome(listing({"project": storage.project, **query}, path="/storage/v1/b"))

    gcp_snapshot.match("bucket-page", buckets({"maxResults": 1}))
    gcp_snapshot.match("bucket-invalid-page-token", buckets({"pageToken": "not-a-page-token"}))
    gcp_snapshot.match("bucket-max-results-zero", buckets({"maxResults": 0}))
    gcp_snapshot.match("bucket-max-results-negative", buckets({"maxResults": -1}))
