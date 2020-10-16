import logging
import threading
from abc import ABC
from typing import Iterator

from galileodb.model import ServiceRequestEntity, CompletedServiceRequest, ServiceRequestTrace, ServiceRequestTraceData
from galileodb.trace import TraceLogger

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

    def _record(self, service_request: ServiceRequestEntity):
        raise NotImplementedError


class RedisTraceRecorder(TraceRecorder):

    def __init__(self, rds, trace_logger: TraceLogger, flush_every=36) -> None:
        super().__init__(rds)
        self.trace_logger = trace_logger

        self.flush_every = flush_every
        self.i = 0

    def run(self):
        try:
            logger.debug('starting RedisTraceRecorder')
            super().run()
        finally:
            logger.debug('closing RedisTraceRecorder')
            self._flush()

    def _record(self, t: ServiceRequestEntity):
        request = CompletedServiceRequest(
            ServiceRequestTrace.from_entity(t),
            ServiceRequestTraceData(t.request_id, t.content)
        )
        self.trace_logger.buffer.append(request)

        self.i = (self.i + 1) % self.flush_every
        if self.i == 0:
            self._flush()

    def _flush(self):
        self.trace_logger.flush()


class TracesSubscriber:

    def __init__(self, rds, pattern=None) -> None:
        super().__init__()
        self.rds = rds
        self.pattern = pattern or 'galileo/results/traces'
        self.pubsub = None

    def run(self) -> Iterator[ServiceRequestEntity]:
        self.pubsub = self.rds.pubsub()

        try:
            self.pubsub.subscribe(self.pattern)

            for item in self.pubsub.listen():
                data = item['data']
                if type(data) == int:
                    continue

                client, service, host, created, sent, done, exp_id, request_id, status, content = data.split(',')

                if exp_id == 'None' or len(exp_id) == 0:
                    exp_id = None

                if content == '':
                    content = None

                yield ServiceRequestEntity(
                    client,
                    service,
                    host,
                    float(created),
                    float(sent),
                    float(done),
                    exp_id,
                    request_id,
                    int(status),
                    content
                )
        finally:
            self.pubsub.close()

    def close(self):
        if self.pubsub:
            self.pubsub.punsubscribe()
