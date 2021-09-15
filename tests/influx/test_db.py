import time
import unittest

from galileodb.factory import create_influxdb_from_env
from galileodb.model import ExperimentEvent, RequestTrace, Telemetry


class TestInfluxExperimentDatabase(unittest.TestCase):

    def setUp(self) -> None:
        self.exp_db = create_influxdb_from_env()
        self.exp_db.open()
        self.exp_id = 'exp-1'
        self.exp_bucket = self.exp_db.client.buckets_api().create_bucket(bucket_name=self.exp_id,
                                                                         org_id=self.exp_db.org_id)

    def tearDown(self) -> None:
        self.exp_db = create_influxdb_from_env()
        self.exp_db.open()
        self.exp_db.client.buckets_api().delete_bucket(self.exp_bucket.id)

    @unittest.skip("needs environment variables set - test manually")
    def test_save_event(self):
        exp_id = self.exp_id

        exp_db = self.exp_db

        # print(exp_db.get_events(exp_id))

        events = [
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
            ExperimentEvent(exp_id, time.time(), 'some-event', 'some-value'),
        ]
        exp_db.save_events(events)

        for event in events:
            exp_db.save_events([event])

        retrieved_events = exp_db.get_events(exp_id)
        self.assertEquals(events, retrieved_events)

    @unittest.skip("needs environment variables set - test manually")
    def test_save_traces(self):
        exp_id = self.exp_id
        exp_db = self.exp_db

        original_traces = [
            RequestTrace('1', 'client-1', 'resnet', time.time(), time.time() + 0.01, time.time() + 0.1, 200, 'server-1',
                         exp_id, '{"headers": 1}', 'response'),
            RequestTrace('1', 'client-1', 'resnet', time.time(), time.time() + 0.01, time.time() + 0.1, 200, 'server-1',
                         exp_id, response='response'),
            RequestTrace('1', 'client-1', 'resnet', time.time(), time.time() + 0.01, time.time() + 0.1, 200, 'server-1',
                         exp_id, response='response'),
            RequestTrace('1', 'client-1', 'resnet', time.time(), time.time() + 0.01, time.time() + 0.1, 200, 'server-1',
                         exp_id, response='response'),
            RequestTrace('1', 'client-1', 'resnet', time.time(), time.time() + 0.01, time.time() + 0.1, 200, 'server-1',
                         exp_id, response='response')
        ]

        for trace in original_traces:
            exp_db.save_traces([trace])

        traces = exp_db.get_traces(exp_id)
        print(traces)
        self.assertEquals(original_traces, traces, 'traces are not equal')

    @unittest.skip("needs environment variables set - test manually")
    def test_save_telemetry(self):
        exp_id = self.exp_id
        exp_db = self.exp_db

        original_telemetry = [
            Telemetry(timestamp=time.time(), metric='cpu', node='node-0', value=0, exp_id=exp_id, subsystem="core1"),
            Telemetry(timestamp=time.time(), metric='cpu', node='node-0', value=1, exp_id=exp_id),
            Telemetry(timestamp=time.time(), metric='cpu', node='node-0', value=2, exp_id=exp_id),
            Telemetry(timestamp=time.time(), metric='cpu', node='node-0', value=3, exp_id=exp_id),
            Telemetry(timestamp=time.time(), metric='cpu', node='node-0', value=-1, exp_id=exp_id)
        ]

        for tel in original_telemetry:
            exp_db.save_telemetry([tel])

        telemetry = exp_db.get_telemetry(exp_id)

        self.assertEquals(original_telemetry, telemetry, 'telemetry are not equal')
