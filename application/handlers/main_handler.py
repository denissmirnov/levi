import logging
import os
import sys

from aiohttp import web
from aiohttp.web_request import Request

from application.core.handler import BaseHandler
from application.parsers.main_parser import MainParser


class MainHandler(BaseHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)

        self.http_cln = self.app.http_cln
        main_parser = MainParser(self)

        self.server.add_route('GET', '/', main_parser.index_parser)
        self.server.add_static('/static', os.path.realpath(os.path.dirname(sys.argv[0])) + '/static/')

        self.server.set_error_handler(self.error_handler)
        pass

    @staticmethod
    async def status(context_span, request: Request):
        """
        curl 'http://localhost:8080/status/'
        """
        logging.debug('[i] status_handler')
        http_code = 200
        response = {}
        return web.json_response(response, status=http_code)

    async def error_handler(self, context_span, request: Request, error: Exception) -> web.Response:
        self.app.log_err(error)
        if isinstance(error, web.HTTPException):
            return error
        return web.Response(body='Internal Error: ' + str(error), status=500)
