import multiprocessing
import shutil
import threading
import unittest
from time import sleep
from unittest.mock import patch

from timeout_decorator import timeout_decorator

from galileodb.model import RequestTrace
from galileodb.reporter.traces import RedisTraceReporter
from galileodb.trace import TraceLogger, POISON, START, PAUSE, FLUSH, TraceWriter, FileTraceWriter, \
    RedisTopicTraceWriter, DatabaseTraceWriter
from tests.testutils import RedisResource, SqliteResource, assert_poll, RedisSubscriber

traces = [
    RequestTrace('req1', 'client', 'service', 1.1, 1.2, 1.3),
    RequestTrace('req2', 'client', 'service', 2.2, 2.3, 2.4, 200, 'server1', 'exp1','{"headers": 1}', 'hello'),
    RequestTrace('req3', 'client', 'service', 3.2, 3.3, 3.4, status=200, server='server1',
                 response='foo=bar\nx=1,"2",3')
]


class TestTraceLogger(unittest.TestCase):

    def setUp(self) -> None:
        self.queue = multiprocessing.Queue()
        self.logger = TraceLogger(self.queue)
        self.logger.flush_interval = 2
        self.flush_interval = 2
        self.thread = threading.Thread(target=self.logger.run)
        self.thread.start()

    def tearDown(self) -> None:
        if not self.logger.closed:
            self.logger.close()
        self.thread.join(2)

    @patch('galileodb.trace.TraceLogger.flush')
    def test_flush_called(self, flush):
        self.trigger_flush()
        assert_poll(lambda: flush.called_once(), 'Flush was not called after triggering flush')

    @patch('galileodb.trace.TraceLogger.flush')
    def test_flush_called_after_flush_msg(self, flush):
        self.send_message(FLUSH)
        assert_poll(lambda: flush.called_once(), 'Flush was not called after triggering FLUSH')

    def test_closed_after_poison(self):
        self.send_message(POISON)
        assert_poll(lambda: self.logger.closed, 'POISON didnt close logger')

    @patch('galileodb.trace.TraceLogger.flush')
    def test_flushed_after_poison(self, flush):
        self.send_message(POISON)
        assert_poll(lambda: flush.called_once(), 'Flush was not called after POISON')

    def test_running_after_start(self):
        self.send_message(START)
        assert_poll(lambda: self.logger.running, 'Logger not running after START')

    def test_not_running_after_pause(self):
        self.send_message(PAUSE)
        assert_poll(lambda: not self.logger.running, 'Logger running after PAUSE')

    @patch('galileodb.trace.TraceLogger.flush')
    def test_discarding_messages_in_paused_state(self, flush):
        self.send_message(PAUSE)
        assert_poll(lambda: not self.logger.running, 'Logger running after PAUSE')

        self.queue.put(traces[0])
        self.queue.put(traces[1])

        sleep(0.5)
        self.assertEqual(0, len(self.logger.buffer), 'buffer should not be filled when logger is paused')

        self.send_message(START)
        assert_poll(lambda: self.logger.running, 'Logger not running after START')

        self.queue.put(traces[2])
        assert_poll(lambda: len(self.logger.buffer) == 1, 'buffer should contain one message after started')

    def test_flush_calls_writer(self):
        written = list()

        class DummyWriter(TraceWriter):
            def write(self, buffer):
                written.extend(buffer)

        self.logger.writer = DummyWriter()

        self.trigger_flush()

        assert_poll(lambda: len(written) == self.flush_interval)

    def trigger_flush(self):
        for i in range(self.flush_interval):
            self.queue.put(RequestTrace(f'req{i}', 'client', 'service', 1, 1, 1, status=200, response='data'))

    def send_message(self, msg):
        self.queue.put(msg)


class TestDatabaseTraceWriter(unittest.TestCase):
    sql_resource = SqliteResource()

    def setUp(self) -> None:
        self.sql_resource.setUp()

    def tearDown(self) -> None:
        self.sql_resource.tearDown()

    @timeout_decorator.timeout(5)
    def test_write(self):
        self.writer = DatabaseTraceWriter(self.sql_resource.db)
        self.writer.write(traces)

        actual = self.sql_resource.db.get_traces()

        self.assertEqual(3, len(actual))

        self.assertEqual(traces[0], actual[0])
        self.assertEqual(traces[1], actual[1])
        self.assertEqual(traces[2], actual[2])


class TestRedisTopicTraceWriter(unittest.TestCase):
    redis_resource = RedisResource()
    writer: RedisTopicTraceWriter

    def setUp(self) -> None:
        self.redis_resource.setUp()

        self.writer = RedisTopicTraceWriter(self.redis_resource.rds)
        self.sub = RedisSubscriber(self.redis_resource.rds, RedisTraceReporter.channel)
        self.sub.start()

    def tearDown(self) -> None:
        self.sub.shutdown()
        self.redis_resource.tearDown()

    @timeout_decorator.timeout(5)
    def test_write(self):
        self.writer.write(traces)

        t1 = self.sub.queue.get(timeout=2)
        self.assertEqual(t1, 'req1,client,service,1.1000000,1.2000000,1.3000000,-1,None,None,None,None')
        t2 = self.sub.queue.get(timeout=2)
        self.assertEqual(t2, 'req2,client,service,2.2000000,2.3000000,2.4000000,200,server1,exp1,{"headers": 1},hello')
        t3 = self.sub.queue.get(timeout=2)
        self.assertEqual(t3, r'req3,client,service,3.2000000,3.3000000,3.4000000,200,server1,None,None,foo=bar\nx=1,"2",3')


class TestFileTraceWriter(unittest.TestCase):
    target_dir = '/tmp/galileo_test'

    def setUp(self) -> None:
        self.writer = FileTraceWriter('test', self.target_dir)

    def tearDown(self) -> None:
        shutil.rmtree(self.target_dir)

    def test_write(self):
        self.writer.write(traces[:1])
        self.writer.write(traces[1:])  # makes sure consecutive write calls append to file

        expected = \
            'request_id,client,service,created,sent,done,status,server,exp_id,headers,response\n' + \
            'req1,client,service,1.1,1.2,1.3,-1,,,,\n' + \
            'req2,client,service,2.2,2.3,2.4,200,server1,exp1,"{""headers"": 1}",hello\n' + \
            'req3,client,service,3.2,3.3,3.4,200,server1,,,"foo=bar\n' + \
            'x=1,""2"",3"'  # this is just how the csv writer resolves the double quotes ...

        with open(self.writer.file_path) as fd:
            actual = fd.read()

        self.assertEqual(expected.strip(), actual.strip())
