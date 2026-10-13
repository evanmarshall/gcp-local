"""Outcome observations for the security scenarios: errors with details, minus diagnostics."""

import re

from gcp_validation.errors import detailed
from gcp_validation.evidence import check_observation

# ErrorInfo metadata GCP mints per call: an opaque id and a console link that embeds it.
VOLATILE = frozenset({'error_info_id', 'troubleshooter_url'})
# The sentence some denials end with, linking that call's Troubleshooter page.
TROUBLESHOOTER = re.compile(
    r' Remediate access with this Troubleshooter URL or share it with your administrator - \S+ \.$'
)


def outcome(observe: object, response: object) -> dict:
    """Select an error's message and details, or a success's status and field names.

    Per-call diagnostic metadata and the Troubleshooter sentence are dropped;
    the reason, domain, and remaining metadata stay.
    """
    if response.status < 400:
        selected = {'http_status': response.status, 'fields': sorted(response.json())}
        check_observation(selected)
        return selected
    selected = detailed(observe, response)
    selected['message'] = TROUBLESHOOTER.sub('', selected['message'])
    for detail in selected['details']:
        if isinstance(detail.get('metadata'), dict):
            detail['metadata'] = {
                key: value for key, value in detail['metadata'].items() if key not in VOLATILE
            }
    return selected
