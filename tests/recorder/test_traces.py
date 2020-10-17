import unittest
from queue import Queue

from galileodb.model import RequestTrace
from galileodb.recorder.traces import TraceRecorder
from galileodb.reporter.traces import RedisTraceReporter
from tests.testutils import RedisResource


class TraceRecorderTest(unittest.TestCase):
    redis = RedisResource()

    def setUp(self) -> None:
        self.redis.setUp()

    def tearDown(self) -> None:
        self.redis.tearDown()

    def test_recorder(self):
        queue = Queue()

        class TestTraceRecorder(TraceRecorder):
            def _record(self, t):
                queue.put(t)

        recorder = TestTraceRecorder(self.redis.rds)
        recorder.start()

        reporter = RedisTraceReporter(self.redis.rds)
        fixture = [
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response='hello there'),
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, server='s1', exp_id='exp1')
        ]
        reporter.report_multiple(fixture)

        trace = queue.get(timeout=2)
        self.assertEqual(fixture[0], trace)

        trace = queue.get(timeout=2)
        self.assertEqual(fixture[1], trace)

        recorder.stop(timeout=2)
