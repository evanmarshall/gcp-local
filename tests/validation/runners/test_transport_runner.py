from concurrent import futures

import pytest
from google.protobuf import any_pb2, empty_pb2, struct_pb2
from google.rpc import error_details_pb2, status_pb2

from gcp_validation.config import Target
from gcp_validation.live_rest import Observations, Response, RestClient
from gcp_validation.runner.parity import parity
from gcp_validation.runner.rpc import STATUS_DETAILS, GrpcClient, Reply, status_of

TARGET = Target.select('local', project='project-a', endpoint='http://127.0.0.1:4566')
OBSERVE = Observations({'projects/project-a/secrets/s': '<secret>', 'project-a': '<project>'})


def denial(permission='secretmanager.secrets.get'):
    info = error_details_pb2.ErrorInfo(reason='IAM_PERMISSION_DENIED', domain='iam.googleapis.com',
                                       metadata={'permission': permission, 'error_info_id': 'x'})
    detail = any_pb2.Any()
    detail.Pack(info)
    return status_pb2.Status(code=7, message='Permission denied on projects/project-a/secrets/s',
                             details=[detail])


def rest_denial():
    body = ('{"error": {"code": 403, "status": "PERMISSION_DENIED", "message": "Permission denied '
            'on projects/project-a/secrets/s", "details": [{"@type": "type.googleapis.com/'
            'google.rpc.ErrorInfo", "reason": "IAM_PERMISSION_DENIED", "domain": '
            '"iam.googleapis.com", "metadata": {"permission": "secretmanager.secrets.get"}}]}}')
    return Response(403, body.encode())


def test_rich_status_renders_like_rest_error_details():
    status = status_of(((STATUS_DETAILS, denial().SerializeToString()),), 'ignored')
    assert status['details'][0]['@type'] == 'type.googleapis.com/google.rpc.ErrorInfo'
    assert status['details'][0]['reason'] == 'IAM_PERMISSION_DENIED'
    assert status_of((), 'plain text') == {'message': 'plain text'}


def test_matching_transports_have_no_differences():
    reply = Reply('PERMISSION_DENIED', status=status_of(
        ((STATUS_DETAILS, denial().SerializeToString()),), ''))
    result = parity(OBSERVE, rest_denial(), reply)
    assert result['differences'] == []
    assert result['grpc'] == {
        'code': 'PERMISSION_DENIED', 'message': 'Permission denied on <secret>',
        'details': [{'type': 'type.googleapis.com/google.rpc.ErrorInfo',
                     'reason': 'IAM_PERMISSION_DENIED', 'domain': 'iam.googleapis.com',
                     'metadata': {'permission': 'secretmanager.secrets.get'}}],
    }


def test_differences_name_each_differing_key():
    rest = Response(200, b'{"name": "projects/project-a/secrets/s", "createTime": "t"}')
    result = parity(OBSERVE, rest, Reply('OK', message={'name': 'projects/project-a/secrets/s'}),
                    ('name',))
    assert result['rest']['name'] == '<secret>'
    assert result['differences'] == ['fields']
    assert parity(OBSERVE, rest, Reply('NOT_FOUND', status={'message': 'gone'}))['differences'] \
        == ['code', 'details', 'fields', 'message']


def test_local_client_dials_the_next_port_and_stays_in_the_project():
    client = GrpcClient(RestClient(TARGET, 'https://unused.googleapis.com'), 'unused')
    assert (client.address, client.secure) == ('127.0.0.1:4567', False)
    for params in ({}, {'name': 'projects/project-b/secrets/s'}, {'name': 'projects/project-ab'}):
        with pytest.raises(ValueError, match='routing parameters'):
            client.call('fixture', '/x.Y/Z', empty_pb2.Empty(), empty_pb2.Empty, params=params)
    with pytest.raises(ValueError, match='identity'):
        client.call('admin', '/x.Y/Z', empty_pb2.Empty(), empty_pb2.Empty,
                    params={'name': 'projects/project-a'})


@pytest.mark.harness_loopback
def test_call_sends_identity_and_routing_and_reads_status():
    grpc = pytest.importorskip('grpc')
    seen = {}

    def handle(request, context):
        seen.update(context.invocation_metadata())
        if request.fields['deny'].bool_value:
            context.abort_with_status(_RichStatus(grpc, denial()))
        return struct_pb2.Struct(fields={'name': struct_pb2.Value(string_value='ok')})

    server = grpc.server(futures.ThreadPoolExecutor(max_workers=1))
    server.add_generic_rpc_handlers((grpc.method_handlers_generic_handler('x.Y', {
        'Z': grpc.unary_unary_rpc_method_handler(
            handle, request_deserializer=struct_pb2.Struct.FromString,
            response_serializer=struct_pb2.Struct.SerializeToString)}),))
    port = server.add_insecure_port('127.0.0.1:0')
    server.start()
    try:
        endpoint = f'http://127.0.0.1:{port - 1}'
        target = Target.select('local', project='project-a', endpoint=endpoint)
        client = GrpcClient(RestClient(target, 'https://unused.googleapis.com'), 'unused')
        params = {'name': 'projects/project-a/secrets/s'}
        allowed = client.call('fixture', '/x.Y/Z', struct_pb2.Struct(), struct_pb2.Struct,
                              params=params)
        assert (allowed.code, allowed.message) == ('OK', {'name': 'ok'})
        assert seen['authorization'] == 'Bearer gcp-local-test:fixture'
        assert seen['x-goog-request-params'] == 'name=projects%2Fproject-a%2Fsecrets%2Fs'
        request = struct_pb2.Struct(fields={'deny': struct_pb2.Value(bool_value=True)})
        denied = client.call('denied', '/x.Y/Z', request, struct_pb2.Struct, params=params)
        assert denied.code == 'PERMISSION_DENIED'
        assert parity(OBSERVE, rest_denial(), denied)['differences'] == []
        client.close()
    finally:
        server.stop(None)


class _RichStatus:
    """A grpc.Status carrying a google.rpc.Status in the details trailer."""

    def __init__(self, grpc, status):
        self.code = grpc.StatusCode.PERMISSION_DENIED
        self.details = status.message
        self.trailing_metadata = ((STATUS_DETAILS, status.SerializeToString()),)
