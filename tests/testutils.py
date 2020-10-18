import os
import shutil
import tempfile
import time
from queue import Queue
from threading import Thread, Event

import redislite

from galileodb.sql.driver.sqlite import SqliteAdapter
from tests.sql.adapter import AbstractTestSqlDatabase


def poll(condition, timeout=None, interval=0.5):
    remaining = 0
    if timeout is not None:
        remaining = timeout

    while not condition():
        if timeout is not None:
            remaining -= interval

            if remaining <= 0:
                raise TimeoutError('gave up waiting after %s seconds' % timeout)

        time.sleep(interval)


def assert_poll(condition, msg='Condition failed'):
    try:
        poll(condition, 2, 0.01)
    except TimeoutError:
        raise AssertionError(msg)


class TestResource(object):

    def setUp(self):
        pass

    def tearDown(self):
        pass


class RedisResource(TestResource):
    tmpfile: str
    rds: redislite.Redis

    def setUp(self):
        self.tmpfile = tempfile.mktemp('.db', 'galileo_test_')
        self.rds = redislite.Redis(self.tmpfile, decode_responses=True)
        self.rds.get('dummykey')  # run a first command to initiate

    def tearDown(self):
        self.rds.shutdown()

        os.remove(self.tmpfile)
        os.remove(self.rds.redis_configuration_filename)
        os.remove(self.rds.settingregistryfile)
        shutil.rmtree(self.rds.redis_dir)

        self.rds = None
        self.tmpfile = None


class SqliteResource(AbstractTestSqlDatabase, TestResource):
    db_file = None

    def setUp(self) -> None:
        self.db_file = tempfile.mktemp('.sqlite', 'galileo_test_')
        super().setUp()

    def _create_sql_adapter(self):
        return SqliteAdapter(self.db_file)

    def tearDown(self) -> None:
        super().tearDown()
        os.remove(self.db_file)


class RedisSubscriber:

    def __init__(self, rds, channel, queue=None) -> None:
        super().__init__()
        self.rds = rds
        self.channel = channel
        self.queue = queue or Queue()
        self.pubsub = rds.pubsub()
        self.t = None

        self._listening = Event()

    def start(self) -> Thread:
        if not self.t:
            self.t = Thread(target=self.listen)
            self.t.start()
            self._listening.wait(2)

        return self.t

    def join(self, timeout):
        if self.t:
            self.t.join(timeout)

    def listen(self):
        self.pubsub.subscribe(self.channel)
        try:
            for item in self.pubsub.listen():
                if item['type'] == 'subscribe':
                    self._listening.set()
                elif item['type'] == 'message':
                    self.queue.put(item['data'])
                else:
                    print('ignoring pubsub data', item)
        except:
            return

    def close(self):
        self.pubsub.close()

    def shutdown(self, timeout=1):
        self.close()
        self.join(timeout)
