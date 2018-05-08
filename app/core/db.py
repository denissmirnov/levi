import logging
import aiozipkin as az
import asyncio
import asyncpg

from app.core.app import Component
from app.core.helper import PrepareError


def db_decorator(func):
    async def wrapper(*args, **kwargs):
        context_span, db_id, res = await func(*args, **kwargs)
        with context_span.tracer.new_child(context_span.context) as span:
            i = 0
            a_args = []
            for arg in args:
                if i > 3:
                    a_args.append(arg)
                i = i + 1

            span.kind(az.CLIENT)
            span.name("db:%s" % db_id)
            span.remote_endpoint("postgres")
            span.annotate(repr(a_args))
        return res
    return wrapper


class DB(Component):
    def __init__(self, config):
        super(DB, self).__init__()
        dsn = 'postgres://%s:%s@%s:5432/%s' % (
            config['username'],
            config['password'],
            config['host'],
            config['dbname']
        )
        self.connect_max_attempts = config['connect_max_attempts']
        self.connect_retry_delay = config['connect_retry_delay']
        self._dsn = dsn
        self.pool = None

    async def _connect(self):
        while True:
            try:
                self.pool = await asyncpg.create_pool(
                    dsn=self._dsn,
                    max_size=self.app.config['system']['pool_max_size'],
                    min_size=self.app.config['system']['pool_min_size'],
                    max_queries=self.app.config['system']['pool_max_queries'],
                    max_inactive_connection_lifetime=(
                        self.app.config['system']['pool_max_inactive_connection_lifetime']),
                    )
            except:
                await asyncio.sleep(5)
                logging.error('Trying to reconnect to database...')
            else:
                break

    @db_decorator
    async def execute(self, context_span, db_id, sql, *args):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute(sql, *args)
                except Exception as e:
                    logging.error('DB _execute %s %s error: %s' % (sql, self.__class__.__name__, str(e)))
                    res = e
                else:
                    res = None
        return context_span, db_id, res

    @db_decorator
    async def query(self, context_span, db_id, sql, *args):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    res = await conn.fetchrow(sql, *args)
                except Exception as e:
                    logging.error('DB _query %s %s error: %s' % (sql, self.__class__.__name__, str(e)))
                    res = None
        return context_span, db_id, res

    @db_decorator
    async def query_all(self, context_span, db_id, sql, *args):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    res = await conn.fetch(sql, *args)
                except Exception as e:
                    logging.error('DB _query_all %s %s error: %s' % (sql, self.__class__.__name__, str(e)))
                    res = None
        return context_span, db_id, res

    async def prepare(self):
        self.app.log_info("Connecting to %s" % self._dsn)
        for i in range(self.connect_max_attempts):
            try:
                await self._connect()
                self.app.log_info("Connected to %s" % self._dsn)
                return
            except Exception as e:
                self.app.log_err(str(e))
                await asyncio.sleep(self.connect_retry_delay)
        raise PrepareError("Could not connect to %s" % self._dsn)

    async def start(self):
        pass

    async def stop(self):
        self.app.log_info("Disconnecting from %s" % self._dsn)
        await asyncio.sleep(0.3)

    async def ping(self):
        async with self.pool.acquire() as conn:
            async with conn.transaction():
                try:
                    await conn.execute('select 1')
                except Exception as e:
                    logging.error('DB _execute ping')
                    res = e
                else:
                    res = None
        return res
