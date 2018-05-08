import asyncio

from app.core.app import BaseApp
from app.core.http_client import HttpClient
from app.core.http_server import HttpServer
from app.db.main_db import MainDb
from app.handlers.main_handler import MainHandler


class Application(BaseApp):
    def __init__(self, config: dict, loop: asyncio.AbstractEventLoop):
        super(Application, self).__init__(config=config, loop=loop)
        self.attach_component(
            'http_cln',
            HttpClient()
        )
        self.attach_component(
            'db_main',
            MainDb(config['db']),
            stop_after=['http_srv']
        )
        self.attach_component(
            'http_srv',
            HttpServer(
                self,
                config['system']['host'],
                config['system']['port'],
                MainHandler
            ),
        )
        self.setup_logging(
            tracer_driver=config['logging']['tracer'],
            tracer_svc_name=config['logging']['tracer_svc_name'],
            tracer_url=config['logging']['tracer_url'],
            statsd_addr=config['logging']['statsd_addr'],
            statsd_prefix=config['logging']['statsd_prefix']
        )
