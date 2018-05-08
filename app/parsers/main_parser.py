import logging

import aiohttp_jinja2
from aiohttp.web_request import Request

from app.core.handler import BaseHandler
from app.lib.main_misc import MainMisc


class MainParser(BaseHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.db = self.app.db_main
        self.lib = MainMisc(self)

    async def index_parser(self, context_span, request: Request):
        logging.debug('[i] index page')
        response = aiohttp_jinja2.render_template('sample.jinja2', request, {})
        response.headers['Content-Language'] = 'ru'
        return response
