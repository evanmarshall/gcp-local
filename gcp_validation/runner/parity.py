"""Put one call's REST and gRPC results into one normalized shape and diff them.

Both transports render to ``{'code', 'fields', <selected fields>}`` on success
and ``{'code', 'message', 'details'}`` on error, where ``code`` is the
canonical status name (``OK``, ``PERMISSION_DENIED``), ``fields`` the sorted
top-level proto3 JSON names, and ``details`` the ``google.rpc`` details with
ErrorInfo's reason, domain, and metadata and BadRequest's violations. Identifiers
are aliased by the scenario's ``Observations``.

A recording stores both views and the differences between them. GCP's own
transports may differ; the emulator must differ the same way.
"""

from gcp_validation.evidence import check_observation
from gcp_validation.live_rest import Observations, Response
from gcp_validation.runner.rpc import Reply
from gcp_validation.services.security.refusals import TROUBLESHOOTER, VOLATILE

DETAIL_KEYS = ('reason', 'domain', 'metadata', 'fieldViolations')


def details(observe: Observations, values: list) -> list[dict]:
    """Select each google.rpc detail's type and its stable fields, minus per-call metadata."""
    selected = []
    for detail in values or ():
        item = {'type': detail.get('@type')}
        for key in DETAIL_KEYS:
            if key in detail:
                item[key] = observe.value(detail[key])
        if isinstance(item.get('metadata'), dict):
            item['metadata'] = {k: v for k, v in item['metadata'].items() if k not in VOLATILE}
        selected.append(item)
    return selected


def success(observe: Observations, value: dict, fields: tuple) -> dict:
    """Select a success's field names and the chosen fields."""
    selected = {'code': 'OK', 'fields': sorted(value)}
    for name in fields:
        selected[name] = observe.value(value.get(name))
    return selected


def failure(observe: Observations, code: str, status: dict) -> dict:
    """Select an error's code, message (minus the Troubleshooter link), and details."""
    message = TROUBLESHOOTER.sub('', observe.text(status.get('message', '')))
    return {'code': code, 'message': message, 'details': details(observe, status.get('details'))}


def rest_view(observe: Observations, response: Response, fields: tuple = ()) -> dict:
    """Normalize a REST response."""
    if response.status < 400:
        return success(observe, response.json(), fields)
    error = response.json().get('error') or {}
    return failure(observe, error.get('status') or f'HTTP_{response.status}', error)


def grpc_view(observe: Observations, reply: Reply, fields: tuple = ()) -> dict:
    """Normalize a gRPC reply."""
    if reply.code == 'OK':
        return success(observe, reply.message, fields)
    return failure(observe, reply.code, reply.status)


def differences(rest: dict, grpc: dict) -> list[str]:
    """Name each normalized key whose value differs between the transports."""
    return sorted(key for key in rest.keys() | grpc.keys() if rest.get(key) != grpc.get(key))


def parity(observe: Observations, response: Response, reply: Reply, fields: tuple = ()) -> dict:
    """Return both views of one call and the keys where they differ."""
    rest, grpc = rest_view(observe, response, fields), grpc_view(observe, reply, fields)
    selected = {'rest': rest, 'grpc': grpc, 'differences': differences(rest, grpc)}
    check_observation(selected)
    return selected
