import logging
import time
import unittest

from telemc import Telemetry
from timeout_decorator import timeout_decorator

from galileodb.reporter.telemetry import RedisTelemetryReporter
from galileodb.sql.adapter import ExperimentSQLDatabase
from galileodb.recorder.telemetry import ExperimentTelemetryRecorder
from tests.testutils import RedisResource, SqliteResource, poll

logging.basicConfig(level=logging.DEBUG)


class TestExperimentTelemetryRecorder(unittest.TestCase):
    redis_resource: RedisResource = RedisResource()
    db_resource: SqliteResource = SqliteResource()

    exp_db: ExperimentSQLDatabase
    reporter: RedisTelemetryReporter

    def setUp(self) -> None:
        self.redis_resource.setUp()
        self.db_resource.setUp()
        self.reporter = RedisTelemetryReporter(self.redis_resource.rds)

    def tearDown(self) -> None:
        self.redis_resource.tearDown()
        self.db_resource.tearDown()

    @timeout_decorator.timeout(5)
    def test_recorder_records_correctly(self):
        recorder = ExperimentTelemetryRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest', flush_every=1)

        recorder._record(Telemetry('1.0', '31', 'node1', 'cpu'))
        recorder._record(Telemetry('2.0', '32', 'node2', 'cpu'))
        recorder._record(Telemetry('3.0', '33', 'node2', 'rx', 'eth0'))

        records = self.db_resource.sql.fetchall('SELECT * FROM `telemetry` WHERE EXP_ID = "unittest"')
        self.assertEqual(3, len(records))
        self.assertEqual(('unittest', 1.0, 'cpu', None, 'node1', 31.0), records[0])
        self.assertEqual(('unittest', 2.0, 'cpu', None, 'node2', 32.0), records[1])
        self.assertEqual(('unittest', 3.0, 'rx', 'eth0', 'node2', 33.0), records[2])

    @timeout_decorator.timeout(5)
    def test_publish_non_float_value_does_not_break_recorder(self):
        recorder = ExperimentTelemetryRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest')
        recorder.start()
        time.sleep(0.5)

        try:
            self.reporter.report(Telemetry('5', '35', 'node1', 'cpu'))
            self.reporter.report(Telemetry('6', 'foo', 'node1', 'cpu'))
            self.reporter.report(Telemetry('7', '37', 'node2', 'cpu'))
        finally:
            time.sleep(0.5)
            recorder.stop(timeout=2)

        records = self.db_resource.sql.fetchall('SELECT * FROM `telemetry` WHERE EXP_ID = "unittest"')
        self.assertEqual(2, len(records))
        self.assertEqual(('unittest', 5.0, 'cpu', None, 'node1', 35.0), records[0])
        self.assertEqual(('unittest', 7.0, 'cpu', None, 'node2', 37.0), records[1])


if __name__ == '__main__':
    unittest.main()
