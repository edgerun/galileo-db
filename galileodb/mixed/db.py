from typing import List

from galileodb import ExperimentDatabase, Experiment, NodeInfo, Telemetry
from galileodb.influx.db import InfluxExperimentDatabase
from galileodb.model import ExperimentEvent, RequestTrace
from galileodb.sql.adapter import ExperimentSQLDatabase


class MixedExperimentDatabase(ExperimentDatabase):
    """
    Implements the ExperimentDatabase using InfluxDB for telemetry, traces and events and
    SQL for experiment metadata
    """
    influxdb: InfluxExperimentDatabase
    sqldb: ExperimentSQLDatabase

    def __init__(self, influxdb: InfluxExperimentDatabase, sqldb: ExperimentSQLDatabase):
        self.influxdb = influxdb
        self.sqldb = sqldb

    def open(self):
        self.influxdb.open()
        self.sqldb.open()

    def close(self):
        self.influxdb.close()
        self.sqldb.close()

    def save_experiment(self, experiment: Experiment):
        self.sqldb.save_experiment(experiment)

    def update_experiment(self, experiment: Experiment):
        self.sqldb.update_experiment(experiment)

    def delete_experiment(self, exp_id: str):
        self.sqldb.delete_experiment(exp_id)

    def get_experiment(self, exp_id: str) -> Experiment:
        return self.sqldb.get_experiment(exp_id)

    def save_traces(self, traces: List[RequestTrace]):
        self.influxdb.save_traces(traces)

    def touch_traces(self, experiment: Experiment):
        raise NotImplementedError()

    def get_traces(self, exp_id: str) -> List[RequestTrace]:
        return self.influxdb.get_traces(exp_id)

    def save_telemetry(self, telemetry: List[Telemetry]):
        self.influxdb.save_telemetry(telemetry)

    def get_telemetry(self, exp_id=None) -> List[Telemetry]:
        return self.influxdb.get_telemetry(exp_id)

    def save_event(self, event: ExperimentEvent):
        self.influxdb.save_event(event)

    def save_events(self, events: List[ExperimentEvent]):
        self.influxdb.save_events(events)

    def get_events(self, exp_id) -> List[ExperimentEvent]:
        return self.influxdb.get_events(exp_id)

    def save_nodeinfos(self, infos: List[NodeInfo]):
        self.sqldb.save_nodeinfos(infos)

    def find_all(self) -> List[Experiment]:
        return self.sqldb.find_all()

    def get_running_experiment(self) -> Experiment:
        return self.sqldb.get_running_experiment()
