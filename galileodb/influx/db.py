import datetime
from typing import List

from influxdb_client import InfluxDBClient, Point, WriteOptions, WriteApi, QueryApi, WritePrecision
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

    def __init__(self, client: InfluxDBClient, org_name: str = 'galileo', org_id='org-id') -> None:
        super().__init__()
        self.client = client
        self.org_name = org_name
        self.org_id = org_id
        self.write_options = WriteOptions(write_type=WriteType.synchronous)

    def open(self):
        self.writer = self.client.write_api(self.write_options)
        self.query = self.client.query_api()
        self.delete = self.client.delete_api()

    def close(self):
        self.client.close()

    def save_experiment(self, experiment: Experiment):
        raise NotImplementedError()

    def update_experiment(self, experiment: Experiment):
        raise NotImplementedError()

    def delete_experiment(self, exp_id: str):
        raise NotImplementedError()

    def get_experiment(self, exp_id: str) -> Experiment:
        raise NotImplementedError()

    def touch_traces(self, experiment: Experiment):
        raise NotImplementedError()

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
        records = self._query_for_measurement("trace", exp_id)
        events = list()

        for record in records:
            events.append(self._map_flux_record_to_request_trace(record))

        return events

    # https://github.com/influxdata/influxdb-client-python/blob/eadbf6ac014582127e2df54698682e2924973e19/examples/nanosecond_precision.py#L37

    def get_telemetry(self, exp_id=None) -> List[Telemetry]:
        records = self._query_for_measurement("telemetry", exp_id)
        events = list()

        for record in records:
            events.append(self._map_flux_record_to_telemetry(record))

        return events

    @staticmethod
    def _map_flux_record_to_telemetry(record: FluxRecord):
        return Telemetry(
            timestamp=float(record.values['ts']),
            metric=record.values['metric'],
            node=record.values['node'],
            value=record.get_value(),
            exp_id=record.values['exp_id']
        )

    def save_telemetry(self, telemetry: List[Telemetry]):
        if len(telemetry) == 0:
            return

        points: List[Point] = list()
        for data in telemetry:
            strftime = datetime.datetime.utcfromtimestamp(data.timestamp)
            p = Point("telemetry") \
                .time(strftime, WritePrecision.MS) \
                .tag('ts', data.timestamp) \
                .field('value', data.value) \
                .tag('exp_id', data.exp_id) \
                .tag('node', data.node) \
                .tag('metric', data.metric)
            points.append(p)

        self.writer.write(bucket=telemetry[0].exp_id, org=self.org_name, record=points)

    def save_event(self, event: ExperimentEvent):
        return self.save_events([event])

    def save_traces(self, traces: List[RequestTrace]):
        if len(traces) == 0:
            return

        points: List[Point] = list()
        for trace in traces:
            strftime = datetime.datetime.utcfromtimestamp(trace.sent)
            p = Point("trace") \
                .time(strftime, WritePrecision.MS) \
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

    def save_events(self, events: List[ExperimentEvent]):
        if len(events) == 0:
            return

        points: List[Point] = list()

        for event in events:
            strftime = datetime.datetime.utcfromtimestamp(event.timestamp)
            p = Point("event") \
                .time(strftime, WritePrecision.MS) \
                .tag("ts", event.timestamp) \
                .tag("name", event.name) \
                .field("value", event.value) \
                .tag("exp_id", event.exp_id)
            points.append(p)

        self.writer.write(bucket=events[0].exp_id, org=self.org_name, record=points)

    @staticmethod
    def _map_flux_record_to_exp_event(record: FluxRecord) -> ExperimentEvent:
        return ExperimentEvent(
            exp_id=record.values['exp_id'],
            timestamp=float(record.values['ts']),
            name=record.values['name'],
            value=record.get_value()
        )

    def _query_for_measurement(self, measurement: str, exp_id: str):
        stop = datetime.datetime.utcnow() + datetime.timedelta(days=1)
        stop = stop.strftime('%Y-%m-%dT%H:%M:%S.%fZ')
        records = self.query.query_stream(
            f'''  
               from(bucket: "{exp_id}")
                 |> range(start: 1970-01-01, stop: {stop})
                 |> filter(fn: (r) => r["_measurement"] == "{measurement}")    
            '''
        )
        return records

    def get_events(self, exp_id) -> List[ExperimentEvent]:
        records = self._query_for_measurement("event", exp_id)
        events = []
        for record in records:
            events.append(InfluxExperimentDatabase._map_flux_record_to_exp_event(record))

        return events


    def save_nodeinfos(self, infos: List[NodeInfo]):
        raise NotImplementedError()

    def find_all(self) -> List[Experiment]:
        raise NotImplementedError()

    def get_running_experiment(self) -> Experiment:
        raise NotImplementedError()
