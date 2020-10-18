from typing import Iterable

from galileodb.model import RequestTrace


class RedisTraceReporter:
    channel = 'galileo/results/traces'
    line_format = '%s,%s,%s,%.7f,%.7f,%.7f,%d,%s,%s,%s'

    def __init__(self, rds) -> None:
        super().__init__()
        self.rds = rds

    def report_multiple(self, traces: Iterable[RequestTrace]):
        rds = self.rds.pipeline()
        key = self.channel
        fmt = self.line_format

        for t in traces:
            # FIXME: this is turning into a bad line-based protocol ...
            response = t.response
            if response:
                response = response.replace('\n', '\\n')

            value = fmt % (
                t.request_id,
                t.client,
                t.service,
                t.created,
                t.sent,
                t.done,
                t.status,
                t.server,
                t.exp_id,
                response,
            )
            rds.publish(key, value)

        rds.execute()
