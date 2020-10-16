import unittest
from queue import Queue

from galileodb.recorder.trace import TraceRecorder
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

        self.redis.rds.publish('galileo/results/traces',
                               'client,service,host,0.0000000,1.0000000,1.0000000,None,id1,200,None')
        self.redis.rds.publish('galileo/results/traces',
                               'client,service,host,0.0000000,1.0000000,1.0000000,exp1,id2,200,data')

        trace = queue.get(timeout=2)
        self.assertEqual('client', trace.client)
        self.assertEqual('service', trace.service)
        self.assertEqual('host', trace.host)
        self.assertEqual(0.0000000, trace.created)
        self.assertEqual(1.0000000, trace.sent)
        self.assertEqual(1.0000000, trace.done)
        self.assertEqual(None, trace.exp_id)
        self.assertEqual('id1', trace.request_id)
        self.assertEqual(200, trace.status)
        self.assertEqual(None, trace.content)

        trace = queue.get(timeout=2)
        self.assertEqual('client', trace.client)
        self.assertEqual('service', trace.service)
        self.assertEqual('host', trace.host)
        self.assertEqual(0.0000000, trace.created)
        self.assertEqual(1.0000000, trace.sent)
        self.assertEqual(1.0000000, trace.done)
        self.assertEqual('exp1', trace.exp_id)
        self.assertEqual('id2', trace.request_id)
        self.assertEqual(200, trace.status)
        self.assertEqual('data', trace.content)

        recorder.stop(timeout=2)
