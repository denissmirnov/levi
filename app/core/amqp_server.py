import json
import asyncio
import aioamqp

from .app import Component


class AMQPServer(Component):

    CONNECTION_DELAY = 1

    def __init__(self, config, handlers):
        super().__init__()
        self.config = {
            'host': config.get('host', 'localhost'),
            'port': config.get('port', ),
            'virtualhost': config.get('virtualhost', '/'),
            'login': config.get('login', 'guest'),
            'password': config.get('password', 'guest'),
            'durable': config.get('durable', True),
        }
        self.exchange_name = config.get('exchange_name', 'mocker')
        self.exchange_type = config.get('exchange_type', 'topic')
        self.exchange_durable = config.get('exchange_durable', True)
        self.transport = None
        self.protocol = None
        self.channel = None
        self.consumers = handlers
        self.consumers_list = []
        self.started = []

    async def prepare(self):
        self.app.log_info("Preparing to start AMQP server...")
        self.consumers_list = self.consumers.routes()

    async def start(self):
        await self.connect()
        await self.channel.exchange_declare(
            exchange_name=self.exchange_name,
            type_name=self.exchange_type,
            durable=self.exchange_durable,
        )
        for name, queue_name, func in self.consumers_list:
            # TODO: wait for
            await self.channel.queue_declare(
                queue_name=queue_name,
                # durable=self.config['durable'],
            )
            await self.channel.queue_bind(          # TODO: timeout
                exchange_name=self.exchange_name,
                queue_name=queue_name,
                routing_key=queue_name
            )
            await self.channel.basic_consume(       # TODO: get tag
                func,
                queue_name=queue_name,
                no_ack=True
            )
            self.app.log_info("Start consumer '%s'." % name)

    async def stop(self):
        # TODO: сделать через basic_cancel и тег
        # await self.channel.stop_consuming()
        await self.disconnect()
        self.app.log_info("Stop AMQP server.")

    async def connect(self):
        try:
            loop = asyncio.get_event_loop()
            self.transport, self.protocol = await aioamqp.connect(
                **self.config,
                loop=loop,
                on_error=self.error_callback
            )
            self.channel = await self.protocol.channel()
        except aioamqp.AmqpClosedConnection as e:
            await asyncio.sleep(self.CONNECTION_DELAY)
            await self.connect()

    async def disconnect(self):
        if self.transport and self.protocol:
            await self.protocol.close()
            self.transport.close()

    async def error_callback(self, error):
        if isinstance(error, aioamqp.AmqpClosedConnection):
            await asyncio.sleep(self.CONNECTION_DELAY)
            self.app.log_info("Restarting AMQP server.")
            await self.start()
