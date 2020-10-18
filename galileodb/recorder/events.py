"""
Clients can publish into the redis topic 'galileo/events' with values: '<timestamp> <name> [<value>]'.
"""
import logging
import threading
from typing import Iterator

from galileodb import ExperimentDatabase
from galileodb.model import ExperimentEvent, Event

logger = logging.getLogger(__name__)


class RedisEventSubscriber:
    def __init__(self, rds, topic='galileo/events') -> None:
        super().__init__()
        self.rds = rds
        self.topic = topic
        self.pubsub = None

    def listen(self) -> Iterator[Event]:
        self.pubsub = self.rds.pubsub()

        try:
            self.pubsub.psubscribe(self.topic)

            for item in self.pubsub.listen():
                data = item['data']
                if type(data) == int:
                    continue

                # timestamp name [value]
                payload = data.split(' ', maxsplit=2)

                if len(payload) == 2 or len(payload) == 3:
                    yield Event(*payload)
                else:
                    logger.warning('Unknown event payload format %s', payload)

        finally:
            self.pubsub.close()

    def close(self):
        if self.pubsub:
            self.pubsub.punsubscribe()


class ExperimentEventRecorder:
    def __init__(self, rds, db: ExperimentDatabase, exp_id: str) -> None:
        self.rds = rds
        self.db = db
        self.exp_id = exp_id
        self._subscriber = None

    def run(self):
        self._subscriber = RedisEventSubscriber(self.rds)

        for event in self._subscriber.listen():
            try:
                self._record(event)
            except:
                logger.exception("error saving ExperimentEvent")

    def close(self):
        if self._subscriber:
            self._subscriber.close()

    def _record(self, event: Event):
        self.db.save_event(ExperimentEvent(self.exp_id, *event))


class BatchingExperimentEventRecorder:
    def __init__(self, rds, db: ExperimentDatabase, exp_id: str, flush_every=36) -> None:
        self.rds = rds
        self.db = db
        self.exp_id = exp_id
        self.flush_every = flush_every

        self._buffer = list()
        self._subscriber = None
        self._i = 0

    def run(self):
        self._subscriber = RedisEventSubscriber(self.rds)
        try:
            for event in self._subscriber.listen():
                self._record(event)
        finally:
            self.flush()

    def close(self):
        if self._subscriber:
            self._subscriber.close()

    def flush(self):
        if not self._buffer:
            logger.debug('event buffer empty')
            return

        logger.debug('saving %s event records of experiment "%s"', len(self._buffer), self.exp_id)

        self.db.save_events(self._buffer)
        self._buffer.clear()

    def _record(self, event: Event):
        self._buffer.append(ExperimentEvent(self.exp_id, *event))
        self._increment_and_flush()

    def _increment_and_flush(self):
        self._i = (self._i + 1) % self.flush_every
        if self._i == 0:
            self.flush()


class ExperimentEventRecorderThread(threading.Thread):
    def __init__(self, recorder, *args, **kwargs) -> None:
        super().__init__(*args, **kwargs)
        self.recorder = recorder

    def stop(self, timeout=None):
        self.recorder.close()
        self.join(timeout)

    def run(self) -> None:
        self.recorder.run()
