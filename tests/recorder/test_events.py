import time
import unittest

from galileodb.model import Event, ExperimentEvent
from galileodb.recorder.events import ExperimentEventRecorder, ExperimentEventRecorderThread, \
    BatchingExperimentEventRecorder
from galileodb.reporter.events import RedisEventReporter
from galileodb.sql.adapter import ExperimentSQLDatabase
from tests.testutils import RedisResource, SqliteResource


class TestExperimentEventRecorder(unittest.TestCase):
    redis_resource: RedisResource = RedisResource()
    db_resource: SqliteResource = SqliteResource()

    exp_db: ExperimentSQLDatabase
    reporter: RedisEventReporter

    def setUp(self) -> None:
        self.redis_resource.setUp()
        self.db_resource.setUp()
        self.reporter = RedisEventReporter(self.redis_resource.rds)

    def tearDown(self) -> None:
        self.redis_resource.tearDown()
        self.db_resource.tearDown()

    def test_batching_recorder_records_correctly(self):
        thread = ExperimentEventRecorderThread(
            BatchingExperimentEventRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest', flush_every=1)
        )
        thread.start()

        time.sleep(0.1)

        thread.recorder._record(Event(1., 'start', 'function1'))
        thread.recorder._record(Event(2., 'stop', 'function1'))
        thread.recorder._record(Event(3., 'exit'))
        thread.stop()

        records = self.db_resource.sql.fetchall('SELECT * FROM `events` WHERE EXP_ID = "unittest"')
        self.assertEqual(3, len(records))

        self.assertEqual(ExperimentEvent('unittest', 1., 'start', 'function1'), records[0])
        self.assertEqual(ExperimentEvent('unittest', 2., 'stop', 'function1'), records[1])
        self.assertEqual(ExperimentEvent('unittest', 3., 'exit'), records[2])

    def test_batching_recorder_flush_after_stop(self):
        thread = ExperimentEventRecorderThread(
            BatchingExperimentEventRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest', flush_every=5)
        )
        thread.start()

        time.sleep(0.1)

        thread.recorder._record(Event(1., 'start', 'function1'))
        thread.recorder._record(Event(2., 'stop', 'function1'))
        thread.recorder._record(Event(3., 'exit'))

        thread.stop()

        records = self.db_resource.sql.fetchall('SELECT * FROM `events` WHERE EXP_ID = "unittest"')
        self.assertEqual(3, len(records))

        self.assertEqual(ExperimentEvent('unittest', 1., 'start', 'function1'), records[0])
        self.assertEqual(ExperimentEvent('unittest', 2., 'stop', 'function1'), records[1])
        self.assertEqual(ExperimentEvent('unittest', 3., 'exit'), records[2])

    def test_batching_recorder_with_redis(self):
        thread = ExperimentEventRecorderThread(
            BatchingExperimentEventRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest', flush_every=1)
        )
        thread.start()

        time.sleep(0.5)

        self.redis_resource.rds.publish("galileo/events", "1. start function1")
        self.redis_resource.rds.publish("galileo/events", "2. stop function1")
        self.redis_resource.rds.publish("galileo/events", "3. exit")

        time.sleep(0.5)

        thread.stop()

        records = self.db_resource.sql.fetchall('SELECT * FROM `events` WHERE EXP_ID = "unittest"')
        self.assertEqual(3, len(records))

        self.assertEqual(ExperimentEvent('unittest', 1., 'start', 'function1'), records[0])
        self.assertEqual(ExperimentEvent('unittest', 2., 'stop', 'function1'), records[1])
        self.assertEqual(ExperimentEvent('unittest', 3., 'exit'), records[2])

    def test_recorder_with_redis(self):
        thread = ExperimentEventRecorderThread(
            ExperimentEventRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest')
        )
        thread.start()

        time.sleep(0.5)

        self.redis_resource.rds.publish("galileo/events", "1. start function1")
        self.redis_resource.rds.publish("galileo/events", "2. stop function1")
        self.redis_resource.rds.publish("galileo/events", "3. exit")

        time.sleep(0.5)

        thread.stop()

        records = self.db_resource.sql.fetchall('SELECT * FROM `events` WHERE EXP_ID = "unittest"')
        self.assertEqual(3, len(records))

        self.assertEqual(ExperimentEvent('unittest', 1., 'start', 'function1'), records[0])
        self.assertEqual(ExperimentEvent('unittest', 2., 'stop', 'function1'), records[1])
        self.assertEqual(ExperimentEvent('unittest', 3., 'exit'), records[2])
