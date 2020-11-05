from typing import List

from influxdb_client import InfluxDBClient, Point, WriteOptions, WriteApi, QueryApi
from influxdb_client.client.delete_api import DeleteApi
from influxdb_client.client.write_api import WriteType

from galileodb import ExperimentDatabase, Experiment, NodeInfo, Telemetry
from galileodb.model import ExperimentEvent, RequestTrace


class InfluxExperimentDatabase(ExperimentDatabase):
    client: InfluxDBClient
    writer: WriteApi
    query: QueryApi
    delete: DeleteApi

    def __init__(self, client: InfluxDBClient, org_name: str = 'galieo') -> None:
        super().__init__()
        self.client = client
        self.org_name = org_name
        self.write_options = WriteOptions(write_type=WriteType.synchronous)

        self.bucket = 'galileo'

    def open(self):
        self.writer = self.client.write_api(self.write_options)
        self.query = self.client.query_api()
        self.delete = self.client.delete_api()

    def close(self):
        self.client.close()

    def save_experiment(self, experiment: Experiment):
        pass

    def update_experiment(self, experiment: Experiment):
        pass

    def delete_experiment(self, exp_id: str):
        pass

    def get_experiment(self, exp_id: str) -> Experiment:
        pass

    def save_traces(self, traces: List[RequestTrace]):
        pass

    def touch_traces(self, experiment: Experiment):
        pass

    def get_traces(self, exp_id: str = None) -> List[RequestTrace]:
        pass

    def save_telemetry(self, telemetry: List[Telemetry]):
        pass

    def get_telemetry(self, exp_id=None) -> List[Telemetry]:
        pass

    def save_event(self, event: ExperimentEvent):
        return self.save_events([event])

    def save_events(self, events: List[ExperimentEvent]):
        points: List[Point] = list()

        for event in events:
            p = Point("event") \
                .time(int(event.timestamp)) \
                .field("name", event.name) \
                .field("value", event.value) \
                .tag("exp_id", event.exp_id)
            points.append(p)

        self.writer.write(bucket=self.bucket, org=self.org_name, record=points)

    def get_events(self, exp_id=None) -> List[ExperimentEvent]:
        tables = self.query.query(
            '''
            from(bucket:"galileo")
              |> range(start: 1970-01-01)
              |> filter(fn: (r) =>
                  r._measurement == "event"
              )
            '''
        )

        records = tables[0].records
        events = list()

        for record in records:
            print(record)

        return events

    def save_nodeinfos(self, infos: List[NodeInfo]):
        pass

    def find_all(self) -> List[Experiment]:
        pass

    def get_running_experiment(self) -> Experiment:
        pass
