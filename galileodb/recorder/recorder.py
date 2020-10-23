from galileodb.recorder.events import ExperimentEventRecorderThread, ExperimentEventRecorder
from galileodb.recorder.telemetry import ExperimentTelemetryRecorder
from galileodb.recorder.traces import RedisTraceRecorder
from galileodb.trace import DatabaseTraceWriter


class Recorder:

    def __init__(self, rds, exp_db, experiment_id) -> None:
        self.rds = rds
        self.exp_db = exp_db
        self.experiment_id = experiment_id

        self.telemetry_recorder = ExperimentTelemetryRecorder(rds, exp_db, experiment_id)
        self.event_recorder = ExperimentEventRecorderThread(ExperimentEventRecorder(rds, exp_db, experiment_id))
        self.trace_recorder = RedisTraceRecorder(rds, experiment_id, DatabaseTraceWriter(exp_db))

    def start(self):
        self.telemetry_recorder.start()
        self.event_recorder.start()
        self.trace_recorder.start()

    def join(self, timeout=None):
        self.telemetry_recorder.join(timeout)
        self.event_recorder.join(timeout)
        self.trace_recorder.join(timeout)

    def stop(self, timeout=None):
        self.telemetry_recorder.stop(timeout)
        self.event_recorder.stop(timeout)
        self.trace_recorder.stop(timeout)
