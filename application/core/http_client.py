import os
import ssl
import sys
from urllib.parse import urlparse

import aiozipkin as az
import aiozipkin.aiohttp_helpers as azah
import aiozipkin.constants as azc
from aiohttp import TCPConnector, ClientSession, client_exceptions

from application.core.component import Component
from application.core.helper import annotate_bytes


class HttpClient(Component):
    async def prepare(self):
        pass

    async def start(self):
        pass

    async def stop(self):
        pass

    async def request(
        self,
        context_span,
        span_params,
        method,
        url,
        body=None,
        headers=None,
        cert=None,
        **kwargs
    ):
        if cert:
            pem_file = '%s/cert/%s' % (os.path.realpath(os.path.dirname(sys.argv[0])), cert)
            sslcontext = ssl.create_default_context(ssl.Purpose.CLIENT_AUTH)
            sslcontext.load_cert_chain(pem_file)
        else:
            sslcontext = None

        conn = TCPConnector(ssl_context=sslcontext)
        headers = headers or {}
        headers.update(context_span.context.make_headers())

        span = None

        if context_span:
            span = context_span.tracer.new_child(context_span.context)
            headers.update(span.context.make_headers())

        try:
            async with ClientSession(
                    loop=self.loop,
                    headers=headers,
                    read_timeout=self.app.config['system']['time_out'],
                    conn_timeout=self.app.config['system']['time_out'],
                    connector=conn
            ) as session:
                if span:
                    if 'name' in span_params:
                        span.name(span_params['name'])
                    if 'endpoint_name' in span_params:
                        span.remote_endpoint(span_params['endpoint_name'])
                    if 'tags' in span_params and span_params['tags']:
                        for tag_name, tag_val in span_params['tags'].items():
                            span.tag(tag_name, tag_val)
                    span.kind(az.CLIENT)
                    span.tag(azah.HTTP_METHOD, method)
                    parsed = urlparse(url)
                    span.tag(azc.HTTP_HOST, parsed.netloc)
                    span.tag(azc.HTTP_PATH, parsed.path)
                    if body:
                        span.tag(azc.HTTP_REQUEST_SIZE, str(len(body)))
                    span.tag(azc.HTTP_URL, url)
                    annotate_bytes(span, body)
                    span.start()
                resp = await session._request(method, url, data=body, **kwargs)
                response_body = await resp.read()
                resp.release()
                annotate_bytes(span, response_body)
                if span:
                    span.tag(azc.HTTP_STATUS_CODE, resp.status)
                    span.tag(azc.HTTP_RESPONSE_SIZE, str(len(response_body)))
                    span.finish()
                a_resp = await self.adapt_resp(span, resp)
                return a_resp
        except Exception as err:
            raise

    @staticmethod
    async def adapt_resp(span, resp):
        html = await resp.read()
        html = html.decode()
        http_code = resp.status
        _resp_hdrs = resp.headers
        resp_hdrs = {}
        for i in _resp_hdrs:
            resp_hdrs[i] = _resp_hdrs[i]

        return {
            'http_code': http_code,
            'response_headers': resp_hdrs,
            'response': html,
            'span': span
        }
