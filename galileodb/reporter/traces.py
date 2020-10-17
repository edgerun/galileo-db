from typing import Iterable

from galileodb.model import RequestTrace


def _compile_line_format():
    """
    produces something like %s,%s,%s,%.7f,%.7f,... for the RequestTrace named tuple
    :return: a string used for formatting the line
    """

    def placeholder(field):
        if field == float:
            return '%.7f'
        if field == int:
            return '%d'
        return '%s'

    return ','.join([placeholder(f) for f in RequestTrace._field_types.values()])


class RedisTraceReporter:
    channel = 'galileo/results/traces'
    line_format = _compile_line_format()

    def __init__(self, rds) -> None:
        super().__init__()
        self.rds = rds

    def report_multiple(self, traces: Iterable[RequestTrace]):
        rds = self.rds.pipeline()
        key = self.channel
        fmt = self.line_format

        for trace in traces:
            value = fmt % trace
            rds.publish(key, value)

        rds.execute()
