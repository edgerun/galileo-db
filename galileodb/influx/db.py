from typing import List

from influxdb_client import InfluxDBClient, Point, WriteOptions, WriteApi, QueryApi
from influxdb_client.client.delete_api import DeleteApi
from influxdb_client.client.flux_table import FluxRecord
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
        if len(traces) == 0:
            return

        points: List[Point] = list()
        for trace in traces:
            p = Point("trace") \
                .time(int(trace.created)) \
                .field("request_id", trace.request_id) \
                .tag("client", trace.client) \
                .tag("service", trace.service) \
                .tag("created", trace.created) \
                .tag("sent", trace.sent) \
                .tag("done", trace.done) \
                .tag("status", trace.status) \
                .tag("server", trace.server) \
                .tag("exp_id", trace.exp_id) \
                .tag("response", trace.response)
            points.append(p)

        self.writer.write(bucket=traces[0].exp_id, org=self.org_name, record=points)

    def touch_traces(self, experiment: Experiment):
        pass

    @staticmethod
    def _map_flux_record_to_request_trace(record: FluxRecord) -> RequestTrace:
        return RequestTrace(
            request_id=record.get_value(),
            client=record.values['client'],
            service=record.values['service'],
            done=float(record.values['done']),
            created=float(record.values['created']),
            sent=float(record.values['sent']),
            status=int(record.values['status']),
            server=record.values['server'],
            exp_id=record.values['exp_id'],
            response=record.values['response']
        )

    def get_traces(self, exp_id: str) -> List[RequestTrace]:
        records = self.query.query_stream(
            f'''
            from(bucket:"{exp_id}")
              |> range(start: 1970-01-01)
              |> filter(fn: (r) =>
                  r._measurement == "trace"
              )
            ''', org=self.org_name
        )
        events = list()

        for record in records:
            events.append(self._map_flux_record_to_request_trace(record))

        return events

    # https://github.com/influxdata/influxdb-client-python/blob/eadbf6ac014582127e2df54698682e2924973e19/examples/nanosecond_precision.py#L37

    def save_telemetry(self, telemetry: List[Telemetry]):
        pass

    def get_telemetry(self, exp_id=None) -> List[Telemetry]:
        pass

    def save_event(self, event: ExperimentEvent):
        return self.save_events([event])

    def save_events(self, events: List[ExperimentEvent]):
        if len(events) == 0:
            return

        points: List[Point] = list()

        for event in events:
            p = Point("event") \
                .time(int(event.timestamp)) \
                .field("name", event.name) \
                .field("value", event.value) \
                .tag("exp_id", event.exp_id)
            points.append(p)

        self.writer.write(bucket=events[0].exp_id, org=self.org_name, record=points)

    def get_events(self, exp_id) -> List[ExperimentEvent]:
        tables = self.query.query(
            f'''
            from(bucket:"{exp_id}")
              |> range(start: 1970-01-01)
              |> filter(fn: (r) =>
                  r._measurement == "event"
              )
            '''
        )
        if len(tables) == 0:
            return []

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
