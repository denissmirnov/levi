from app.core.db import DB


class MainDb(DB):
    def __init__(self, config):
        super().__init__(config)

    async def get_sample_data(self, context_span, msisdn):
        sql = """
            SELECT
                *
            FROM
                main.sample
        """
        res = await self.query_all(context_span, 'get_sample_data', sql, int(msisdn))
        return res
