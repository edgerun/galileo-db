import logging
import time
import unittest

from telemc import Telemetry

from galileodb.reporter.telemetry import RedisTelemetryReporter
from galileodb.sql.adapter import ExperimentSQLDatabase
from galileodb.recorder.telemetry import ExperimentTelemetryRecorder
from tests.testutils import RedisResource, SqliteResource

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

    def test_recorder_records_correctly(self):
        recorder = ExperimentTelemetryRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest', flush_every=1)
        recorder.start()
        time.sleep(0.1)

        recorder._record(Telemetry('1.0', '31', 'node1', 'cpu'))
        recorder._record(Telemetry('2.0', '32', 'node2', 'cpu'))

        records = self.db_resource.sql.fetchall('SELECT * FROM `telemetry` WHERE EXP_ID = "unittest"')
        self.assertEqual(2, len(records))
        self.assertEqual(('unittest', 1.0, 'cpu', 'node1', 31.0), records[0])
        self.assertEqual(('unittest', 2.0, 'cpu', 'node2', 32.0), records[1])

    def test_publish_non_float_value_does_not_break_recorder(self):
        recorder = ExperimentTelemetryRecorder(self.redis_resource.rds, self.db_resource.db, 'unittest')
        recorder.start()
        time.sleep(0.5)

        try:
            self.reporter.report(Telemetry('5', '35', 'node1', 'cpu'))
            self.reporter.report(Telemetry('6', 'foo', 'node1', 'cpu'))
            self.reporter.report(Telemetry('7', '37', 'node2', 'cpu'))
        finally:
            recorder.stop()

        recorder.join(timeout=2)

        records = self.db_resource.sql.fetchall('SELECT * FROM `telemetry` WHERE EXP_ID = "unittest"')
        self.assertEqual(2, len(records))
        self.assertEqual(('unittest', 5.0, 'cpu', 'node1', 35.0), records[0])
        self.assertEqual(('unittest', 7.0, 'cpu', 'node2', 37.0), records[1])


if __name__ == '__main__':
    unittest.main()
