"""The shared live transport for APIs whose paths are not /v1/projects/..."""

import json
from urllib.error import HTTPError, URLError
from urllib.parse import urlencode
from urllib.request import Request

from .evidence import Inconclusive
from .live_rest import Response, RestClient


class PrefixClient(RestClient):
    """RestClient bound to one API version prefix, such as /v2beta3 or /bigquery/v2."""

    prefix = '/v1'

    def request(self, actor, method, path, *, query=None, body=None, timeout=30):
        if actor not in self.emails:
            raise ValueError('Unknown scenario identity')
        if not path.startswith(f'{self.prefix}/projects/{self.target.project}/'):
            raise ValueError('Select an explicit path in the selected project')
        headers = {'Content-Type': 'application/json', 'Authorization': 'Bearer ' + self.token(actor)}
        if self.target.mode != 'local':
            headers['x-goog-user-project'] = self.target.project
        data = json.dumps(body).encode() if body is not None else (b'' if method == 'POST' else None)
        url = self.endpoint + path + ('?' + urlencode(query) if query else '')
        try:
            response = self.opener.open(Request(url, data=data, headers=headers, method=method), timeout=timeout)
        except HTTPError as error:
            response = error
        except (URLError, TimeoutError, OSError):
            raise Inconclusive('Transport failed; no cloud fallback is available') from None
        with response:
            return Response(response.code, response.read(1 << 20))
