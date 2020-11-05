import time
import unittest

from galileodb.factory import create_influxdb_from_env
from galileodb.model import ExperimentEvent


class TestInfluxExperimentDatabase(unittest.TestCase):
    def test_open(self):
        exp_db = create_influxdb_from_env()
        exp_db.open()

        print(exp_db.get_events())

        exp_db.save_events([
            ExperimentEvent('exp_01', time.time(), 'some-event', 'some-value'),
            ExperimentEvent('exp_01', time.time(), 'some-event', 'some-value'),
            ExperimentEvent('exp_01', time.time(), 'some-event', 'some-value'),
            ExperimentEvent('exp_01', time.time(), 'some-event', 'some-value'),
            ExperimentEvent('exp_01', time.time(), 'some-event', 'some-value'),
        ])

        exp_db.close()
