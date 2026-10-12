"""Bounded Cloud Logging filter-precedence recording.

Writes the three entries of the lifecycle scenario to one run-owned log and lists them
through filters that mix AND, OR, NOT, implicit AND, and parentheses. The Logging query
language binds NOT, then OR, then AND, the reverse of most languages:
https://cloud.google.com/logging/docs/view/logging-query-language#boolean_operators
"""

from gcp_validation.evidence import SnapshotCase
from gcp_validation.services.cloudlogging import LogFixture, payloads, write_entries

# Each filter is parenthesized after the owned log's name, so precedence is decided
# inside the parentheses and no reading can reach another log's entries.
FILTERS = {
    'and-or': 'severity=ERROR AND severity=INFO OR severity=NOTICE',
    'or-and': 'severity=INFO OR severity=ERROR AND "second"',
    'or-implicit-and': 'severity=INFO OR severity=ERROR "second"',
    'parenthesized': '(severity=ERROR AND severity=INFO) OR severity=NOTICE',
    'not-or-and': 'NOT severity=INFO OR severity=ERROR AND NOT severity=NOTICE',
    'documented-example': '"first" OR NOT "second" AND NOT "third" OR "second"',
}


def run_scenario(fixture: LogFixture, snapshot: SnapshotCase) -> None:
    """Record the same selected observations in local, recording, and revalidation runs."""
    written = write_entries(fixture, {'gcp-local-run': fixture.manifest['run_id']})
    snapshot.match('write', {'http_status': written.status})
    everything = fixture.poll('', 3)
    snapshot.match('list-all', {'http_status': everything.status, 'payloads': payloads(everything)})
    for name, expression in FILTERS.items():
        listed = fixture.list(f'({expression})')
        observed = {'http_status': listed.status, 'payloads': payloads(listed)}
        snapshot.match('filter-' + name, observed)
