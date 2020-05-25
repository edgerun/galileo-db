import os
import shutil
import tempfile
import time

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
