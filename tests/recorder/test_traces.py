import unittest
from queue import Queue
from typing import List

from galileodb.model import RequestTrace
from galileodb.recorder.traces import TraceRecorder, TracesSubscriber, RedisTraceRecorder
from galileodb.reporter.traces import RedisTraceReporter
from galileodb.trace import TraceWriter
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

        headers = '{"server": ["Server", "BaseHTTP/0.6 Python/3.9.5"], "date": ["Date", "Tue, 14 Sep 2021 08:54:40 GMT"], "content-type": ["Content-type", "text/html"]}'
        reporter = RedisTraceReporter(self.redis.rds)
        fixture = [
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response='hello there', headers=headers),
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, server='s1', exp_id='exp1')
        ]
        reporter.report_multiple(fixture)

        trace = queue.get(timeout=2)
        self.assertEqual(fixture[0], trace)

        trace = queue.get(timeout=2)
        self.assertEqual(fixture[1], trace)

        recorder.stop(timeout=2)


class TraceSubscriberTest(unittest.TestCase):
    redis = RedisResource()

    def setUp(self) -> None:
        self.redis.setUp()

    def tearDown(self) -> None:
        self.redis.tearDown()

    def test_parse(self):
        parse = TracesSubscriber.parse

        expected = RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response="foo=bar\nfield1=value1,a,b,c")
        actual = parse(r"r1,c1,s1,1.1000000,1.2000000,1.3000000,200,None,None,None,foo=bar\nfield1=value1,a,b,c")

        self.assertEqual(expected, actual)

    def test_parse_with_headers(self):
        parse = TracesSubscriber.parse
        headers = '{"server": ["Server", "BaseHTTP/0.6 Python/3.9.5"], "date": ["Date", "Tue, 14 Sep 2021 08:54:40 GMT"], "content-type": ["Content-type", "text/html"]}'
        expected = RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response='foo=bar\nfield1=value1,a,b,c',
                                headers=headers)
        actual = parse(
            fr"r1,c1,s1,1.1000000,1.2000000,1.3000000,200,None,None,{headers.replace(',', '|')},foo=bar\nfield1=value1,a,b,c")
        self.assertEqual(expected, actual)


class RedisTraceRecorderTest(unittest.TestCase):
    redis = RedisResource()

    def setUp(self) -> None:
        self.redis.setUp()

    def tearDown(self) -> None:
        self.redis.tearDown()

    @unittest.skip("FIXME: sometimes fails")
    def test_recorder(self):
        queue = Queue()

        class TestTraceWriter(TraceWriter):
            def write(self, traces: List[RequestTrace]):
                for t in traces:
                    queue.put(t)

        recorder = RedisTraceRecorder(self.redis.rds, exp_id='exp1', writer=TestTraceWriter(), flush_every=2)
        recorder.start()

        reporter = RedisTraceReporter(self.redis.rds)
        headers = '\'\"{"server": ["Server", "BaseHTTP/0.6 Python/3.9.5"], "date": ["Date", "Tue, 14 Sep 2021 08:54:40 GMT"], "content-type": ["Content-type", "text/html"]}\"\''
        fixture = [
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, headers=headers, response='hello there'),
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, server='s1'),
        ]

        expected = [
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, headers=headers, response='hello there', exp_id='exp1'),
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, server='s1', exp_id='exp1')
        ]

        reporter.report_multiple(fixture)

        trace = queue.get(timeout=2)
        self.assertEqual(expected[0], trace)

        trace = queue.get(timeout=2)
        self.assertEqual(expected[1], trace)

        recorder.stop(timeout=2)
