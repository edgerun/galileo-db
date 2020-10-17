from queue import Queue
from threading import Thread
from unittest import TestCase

from timeout_decorator import timeout_decorator

from galileodb.model import RequestTrace
from galileodb.reporter.traces import RedisTraceReporter
from tests.testutils import RedisResource


class TestRedisTraceReporter(TestCase):
    redis = RedisResource()

    def setUp(self) -> None:
        self.redis.setUp()

    def tearDown(self) -> None:
        self.redis.tearDown()

    @timeout_decorator.timeout(5)
    def test_report_multiple(self):
        reporter = RedisTraceReporter(self.redis.rds)

        pubsub = self.redis.rds.pubsub()
        q = Queue()

        def listen():
            pubsub.subscribe(RedisTraceReporter.channel)
            try:
                for item in pubsub.listen():
                    d = item['data']
                    if type(d) == int:
                        continue
                    q.put(d)
            except:
                return

        t = Thread(target=listen)
        t.start()

        fixture = [
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response='hello there'),
            RequestTrace('r2', 'c1', 's1', 2.1, 2.2, 2.3, 200, server='server'),
        ]

        reporter.report_multiple(fixture)

        try:
            data = q.get(timeout=1)
            self.assertEqual(reporter.line_format % fixture[0], data)
            data = q.get(timeout=1)
            self.assertEqual(reporter.line_format % fixture[1], data)
        finally:
            pubsub.close()

        t.join(1)
