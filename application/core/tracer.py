import re
import asyncio
import aiozipkin.transport as azt
import aiozipkin.constants as azc
from aiostatsd.client import StatsdClient
import logging

STATS_CLEAN_NAME_RE = re.compile('[^0-9a-zA-Z_.-]')
STATS_CLEAN_TAG_RE = re.compile('[^0-9a-zA-Z_=.-]')


class TracerTransport(azt.Transport):
    def __init__(
            self,
            tracer,
            tracer_url,
            statsd_addr,
            statsd_prefix,
            send_inteval,
            loop
    ):
        tracer_url = tracer_url or 'http://localhost:9411/'
        super(TracerTransport, self).__init__(tracer_url, send_inteval=send_inteval, loop=loop)
        self.loop = loop
        self.__tracer = tracer
        self.__statsd_addr = statsd_addr
        self.__statsd_prefix = statsd_prefix

        self.stats = None
        if self.__statsd_addr:
            addr = self.__statsd_addr.split(':')
            host = addr[0]
            port = int(addr[1]) if len(addr) > 1 else 8125
            self.stats = StatsdClient(host, port)
            asyncio.ensure_future(self.stats.run(), loop=loop)

    async def close(self):
        if self.stats:
            try:
                await asyncio.sleep(.001, loop=self.loop)
                await self.stats.stop()
            except Exception as e:
                logging.exception(e)
        await super(TracerTransport, self).close()

    async def _send(self):
        data = self._queue[:]

        try:
            if self.__statsd_addr:
                await self._send_to_statsd(data)
        except Exception as e:
            logging.exception(e)

        try:
            if self.__tracer == 'zipkin':
                await super(TracerTransport, self)._send()
            else:
                self._queue = []
        except Exception as e:
            logging.exception(e)

    async def _send_to_statsd(self, data):
        if self.stats:
            for rec in data:
                tags = []
                t = rec['tags']
                if azc.HTTP_PATH in t and 'kind' in rec:
                    name = 'http'
                    if rec["kind"] == 'SERVER':
                        tags.append(('kind', 'in'))
                    else:
                        tags.append(('kind', 'out'))

                    copy_tags = {
                        azc.HTTP_STATUS_CODE: 'status',
                        azc.HTTP_METHOD: 'method',
                        azc.HTTP_HOST: 'host',
                        'api.key': 'api_key',
                        'api.method': 'api_method',
                        'api.code': 'api_code',
                        'api_merc.code': 'api_merc_code',
                    }
                    for tag_key, tag_name in copy_tags.items():
                        if tag_key in t:
                            tags.append((tag_name, t[tag_key]))

                elif rec['name'].startswith('db:'):
                    name = 'db'
                    tags.append(('kind', rec['name'][len('db:'):]))
                elif rec['name'].startswith('redis:'):
                    name = 'redis'
                    tags.append(('kind', rec['name'][len('redis:'):]))
                elif rec['name'] == 'sleep':
                    name = 'sleep'
                else:
                    name = rec['name']

                name = self.__statsd_prefix + name
                name = name.replace(' ', '_')
                name = STATS_CLEAN_NAME_RE.sub('', name)

                if len(tags) > 0:
                    for tag in tags:
                        t = tag[1].replace(':', '-')
                        t = STATS_CLEAN_TAG_RE.sub('', t)
                        name += ',' + tag[0] + "=" + t
                self.stats.send_timer(name, int(round(rec["duration"] / 1000)), rate=1.0)
