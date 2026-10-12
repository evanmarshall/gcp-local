"""Error observations that keep an error's message and google.rpc details."""

from gcp_validation.evidence import check_observation
from gcp_validation.live_rest import Observations, Response


def detailed(observe: Observations, response: Response) -> dict:
    """Select an error with its message and google.rpc details.

    ErrorInfo keeps its reason, domain, and metadata; BadRequest keeps its
    violations. Identifiers in the text are aliased.
    """
    error = response.json().get('error', {})
    details = []
    for detail in error.get('details') or ():
        selected = {'type': detail.get('@type')}
        for key in ('reason', 'domain', 'metadata', 'fieldViolations'):
            if key in detail:
                selected[key] = observe.value(detail[key])
        details.append(selected)
    selected = {**observe.error(response), 'message': observe.text(error.get('message', '')),
                'details': details}
    check_observation(selected)
    return selected
