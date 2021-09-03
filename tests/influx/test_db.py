import time
import unittest

from galileodb.factory import create_influxdb_from_env
from galileodb.model import ExperimentEvent, RequestTrace


class TestInfluxExperimentDatabase(unittest.TestCase):

    def setUp(self) -> None:
        self.exp_db = create_influxdb_from_env()
        self.exp_db.open()
        self.exp_id = 'exp-1'
        self.exp_bucket = self.exp_db.client.buckets_api().create_bucket(bucket_name=self.exp_id,
                                                                         org_id='a0ec9985b4a69119')

    def tearDown(self) -> None:
        self.exp_db = create_influxdb_from_env()
        self.exp_db.open()
        self.exp_db.client.buckets_api().delete_bucket(self.exp_bucket.id)

    def test_open(self):
        exp_id = self.exp_id

        exp_db = self.exp_db

        print(exp_db.get_events(exp_id))

        exp_db.save_events([
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
        ])

        exp_db.close()

    def test_save_traces(self):
        exp_id = self.exp_id
        exp_db = self.exp_db

        original_traces = [
            RequestTrace('1', 'client-1', 'resnet', time.time(), time.time() + 0.01, time.time() + 0.1, 200, 'server-1',
                         exp_id, 'response')
        ]
        exp_db.save_traces(original_traces)

        traces = exp_db.get_traces(exp_id)

        self.assertEquals(original_traces, traces, 'traces are not equal')
