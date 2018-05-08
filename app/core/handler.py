import asyncio
import json
import logging
from abc import ABCMeta

import aioamqp


class BaseHandler(object):
    __metaclass__ = ABCMeta

    def __init__(self, server):
        self.server = server
        self.loop = None

    @property
    def app(self):
        return self.server.app

    @property
    def config(self):
        return self.server.app.config

    def get_response_message(self, code):
        return self.config['errors'][str(code)]

    def gen_response(self, code):
        return {'code': code, 'message': self.get_response_message(code)}

    async def task(self, context_span, url_out, method, params, pid, non_json=False):
        # Connecting to rabbitmq
        time_waited = 0
        to_wait = 1.5
        while True:
            try:
                transport, protocol = await aioamqp.connect(**self.config['tasks'])
                break
            except (OSError, aioamqp.AmqpClosedConnection) as e:
                to_wait = round(min(30, (to_wait ** 1.5)), 2)
                logging.info(
                    "[x] Failed to connect to tasks RabbitMQ: %s. Waiting %s seconds...", e, to_wait)
                await asyncio.sleep(to_wait)
                time_waited += to_wait
        channel = await protocol.channel()
        context_headers = {}
        context_headers.update(context_span.context.make_headers())
        data = {
            'method': method,
            'url_out': url_out,
            'url_in': None,
            'params': params,
            'params_extra': {},
            'is_api_task': False,
            'retry': 44,
            'pid': pid,
            'delay_time': 2,
            'non_json': non_json,
            'context_headers': context_headers
        }
        await channel.publish(
            json.dumps(data),
            self.config['tasks']['exchange_name'],
            self.config['tasks']['routing_key']
        )
        await protocol.close()
        # ensure the socket is closed.
        transport.close()
        logging.info('[i] Task sended')
