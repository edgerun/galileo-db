from unittest import TestCase

from timeout_decorator import timeout_decorator

from galileodb.model import RequestTrace
from galileodb.reporter.traces import RedisTraceReporter
from tests.testutils import RedisResource, RedisSubscriber


class TestRedisTraceReporter(TestCase):
    redis = RedisResource()

    def setUp(self) -> None:
        self.redis.setUp()

    def tearDown(self) -> None:
        self.redis.tearDown()

    @timeout_decorator.timeout(5)
    def test_report_multiple(self):
        reporter = RedisTraceReporter(self.redis.rds)
        subscriber = RedisSubscriber(self.redis.rds, RedisTraceReporter.channel)
        subscriber.start()

        fixture = [
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response='hello there'),
            RequestTrace('r2', 'c1', 's1', 2.1, 2.2, 2.3, 200, server='server'),
        ]

        reporter.report_multiple(fixture)

        try:
            data = subscriber.queue.get(timeout=1)
            self.assertTrue('r1' in data)
            self.assertTrue('r2' not in data)
            self.assertTrue('server' not in data)
            self.assertTrue('hello there' in data)

            data = subscriber.queue.get(timeout=1)
            self.assertTrue('r1' not in data)
            self.assertTrue('r2' in data)
            self.assertTrue('server' in data)
            self.assertTrue('hello there' not in data)

            self.assertTrue(subscriber.queue.empty())
        finally:
            subscriber.shutdown()

    @timeout_decorator.timeout(5)
    def test_report_response_with_noisy_string(self):
        reporter = RedisTraceReporter(self.redis.rds)
        subscriber = RedisSubscriber(self.redis.rds, RedisTraceReporter.channel)

        subscriber.start()

        response = "foo=bar\nfield1=value1,a,b,c"

        reporter.report_multiple([
            RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response=response),
        ])

        data = subscriber.queue.get(1)
        self.assertEqual(r"r1,c1,s1,1.1000000,1.2000000,1.3000000,200,None,None,None,foo=bar\nfield1=value1,a,b,c",
                         data)

        subscriber.shutdown()

    @timeout_decorator.timeout(5)
    def test_report_with_heaers(self):
        reporter = RedisTraceReporter(self.redis.rds)
        subscriber = RedisSubscriber(self.redis.rds, RedisTraceReporter.channel)

        subscriber.start()

        headers = '{"server": ["Server", "BaseHTTP/0.6 Python/3.9.5"], "date": ["Date", "Tue, 14 Sep 2021 08:54:40 GMT"], "content-type": ["Content-type", "text/html"]}'
        expected = RequestTrace('r1', 'c1', 's1', 1.1, 1.2, 1.3, 200, response='foo=bar\nfield1=value1,a,b,c',
                                headers=headers)

        reporter.report_multiple([
            expected,
        ])

        data = subscriber.queue.get(1)
        self.assertEqual(
            fr"r1,c1,s1,1.1000000,1.2000000,1.3000000,200,None,None,{headers.replace(',', '|')},foo=bar\nfield1=value1,a,b,c",
            data)

        subscriber.shutdown()
