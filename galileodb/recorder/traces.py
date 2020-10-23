import logging
import threading
from abc import ABC
from typing import Iterator

from galileodb.model import RequestTrace
from galileodb.reporter.traces import RedisTraceReporter
from galileodb.trace import TraceWriter

logger = logging.getLogger(__name__)


class TraceRecorder(threading.Thread, ABC):

    def __init__(self, rds, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.rds = rds
        self._sub = None

    def stop(self, timeout=None):
        """
        Equivalent to recorder.close() and recorder.join(timeout).

        :param timeout: the join timeout
        """
        self.close()
        self.join(timeout=timeout)

    def close(self):
        if self._sub:
            self._sub.close()

    def run(self):
        self._sub = TracesSubscriber(self.rds)
        sub = self._sub.run()

        try:
            for trace in sub:
                self._record(trace)
        finally:
            self._sub.close()

    def _record(self, service_request: RequestTrace):
        raise NotImplementedError


class RedisTraceRecorder(TraceRecorder):

    def __init__(self, rds, exp_id: str, writer: TraceWriter, flush_every=36) -> None:
        super().__init__(rds)
        self.exp_id = exp_id
        self.writer = writer
        self.buffer = list()

        self.flush_every = flush_every
        self.i = 0

    def run(self):
        try:
            logger.debug('starting RedisTraceRecorder for experiment %s', self.exp_id)
            super().run()
        finally:
            logger.debug('closing RedisTraceRecorder for experiment %s', self.exp_id)
            self._flush()

    def _record(self, t: RequestTrace):
        t = t._replace(exp_id=self.exp_id)
        self.buffer.append(t)

        self.i = (self.i + 1) % self.flush_every
        if self.i == 0:
            self._flush()

    def _flush(self):
        self.writer.write(self.buffer)
        self.buffer.clear()


class TracesSubscriber:
    def __init__(self, rds, channel=None) -> None:
        super().__init__()
        self.rds = rds
        self.channel = channel or RedisTraceReporter.channel
        self.line_format = RedisTraceReporter.line_format
        self.pubsub = None

    def run(self) -> Iterator[RequestTrace]:
        self.pubsub = self.rds.pubsub()

        try:
            self.pubsub.subscribe(self.channel)

            for item in self.pubsub.listen():
                data = item['data']
                if type(data) == int:
                    continue
                try:
                    yield self.parse(data)
                except Exception as e:
                    logger.error('error parsing data string `%s`: %s', data, e)
        finally:
            self.pubsub.close()

    def close(self):
        if self.pubsub:
            self.pubsub.unsubscribe()

    @staticmethod
    def parse(line: str) -> RequestTrace:
        # FIXME: this is turning into a bad line-based protocol ...
        data = line.split(',', maxsplit=9)

        for i in range(len(data)):
            if data[i] == 'None':
                data[i] = None

        response = data[9]

        if response:
            response = response.replace('\\n', '\n')

        return RequestTrace(
            request_id=data[0],
            client=data[1],
            service=data[2],
            created=float(data[3]),
            sent=float(data[4]),
            done=float(data[5]),
            status=int(data[6]),
            server=data[7],
            exp_id=data[8],
            response=response,
        )
