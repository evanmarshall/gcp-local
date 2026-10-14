"""Explicit-identity gRPC transport for scenarios, sharing a REST client's identities.

Locally it dials gcp-local's gRPC listener, the REST endpoint's port plus one
(4566 and 4567), with the synthetic tokens; live it dials the service's
``host:443`` over TLS with the sandbox identities' tokens. Requests are built
from proto3 JSON, the same field names a REST body uses, and replies come back
as proto3 JSON with their ``google.rpc.Status`` details, so a scenario can put
REST and gRPC results side by side (``gcp_validation.runner.parity``).

``grpc`` is imported on first call: the offline harness installs no gRPC.
"""

import importlib
from dataclasses import dataclass, field
from urllib.parse import urlencode, urlsplit

from google.protobuf import json_format
from google.rpc import error_details_pb2, status_pb2

from gcp_validation.evidence import Inconclusive
from gcp_validation.live_rest import ACTORS, RestClient

STATUS_DETAILS = 'grpc-status-details-bin'


@dataclass
class Reply:
    """One gRPC result: the canonical status name, the response, and the status."""

    code: str
    message: dict = field(default_factory=dict)
    status: dict = field(default_factory=dict)


def status_of(trailers: tuple, text: str) -> dict:
    """Render the google.rpc.Status a server sent as JSON, or just its text.

    Details resolve against the google.rpc detail types (ErrorInfo, BadRequest,
    and the rest), so they render as REST error details do.
    """
    for key, value in trailers or ():
        if key == STATUS_DETAILS:
            return json_format.MessageToDict(status_pb2.Status.FromString(value),
                                             descriptor_pool=error_details_pb2.DESCRIPTOR.pool)
    return {'message': text}


class GrpcClient:
    """Unary gRPC calls in the selected project as one of the scenario identities."""

    def __init__(self, rest: RestClient, live_host: str) -> None:
        """Dial gcp-local's gRPC port locally, or ``live_host:443`` over TLS live."""
        self.rest, self.target = rest, rest.target
        if self.target.mode == 'local':
            url = urlsplit(self.target.endpoint)
            self.address, self.secure = f'{url.hostname}:{url.port + 1}', False
        else:
            self.address, self.secure = live_host + ':443', True
        self.channel = None

    def stub(self, method: str, response_type: type) -> object:
        """Return a callable for one unary method on the shared channel."""
        grpc = importlib.import_module('grpc')
        if self.channel is None:
            self.channel = (
                grpc.secure_channel(self.address, grpc.ssl_channel_credentials())
                if self.secure else grpc.insecure_channel(self.address)
            )
        return self.channel.unary_unary(
            method,
            request_serializer=lambda message: message.SerializeToString(),
            response_deserializer=response_type.FromString,
        )

    def call(self, actor: str, method: str, request: object, response_type: type, *,
             params: dict, timeout: float = 30) -> Reply:
        """Send ``request`` (a protobuf message) to ``method`` and return its reply.

        ``params`` are the routing parameters Google's clients send in
        ``x-goog-request-params``; each must name the selected project.
        """
        if actor not in ACTORS:
            raise ValueError('Unknown scenario identity')
        project = 'projects/' + self.target.project
        if not params or not all(value == project or value.startswith(project + '/')
                                 for value in params.values()):
            raise ValueError('Select explicit routing parameters in the selected project')
        metadata = [('authorization', 'Bearer ' + self.rest.token(actor)),
                    ('x-goog-request-params', urlencode(params))]
        if self.target.mode != 'local':
            metadata.append(('x-goog-user-project', self.target.project))
        grpc = importlib.import_module('grpc')
        try:
            response = self.stub(method, response_type)(request, metadata=metadata,
                                                        timeout=timeout)
        except grpc.RpcError as error:
            code = error.code()
            if code in {grpc.StatusCode.UNAVAILABLE, grpc.StatusCode.DEADLINE_EXCEEDED}:
                raise Inconclusive('Transport failed; no cloud fallback is available') from None
            return Reply(code.name, status=status_of(error.trailing_metadata(),
                                                     error.details() or ''))
        return Reply('OK', message=json_format.MessageToDict(response))

    def close(self) -> None:
        """Close the channel."""
        if self.channel is not None:
            self.channel.close()
            self.channel = None
