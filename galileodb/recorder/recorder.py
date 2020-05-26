from galileodb.recorder.events import ExperimentEventRecorderThread, ExperimentEventRecorder
from galileodb.recorder.telemetry import ExperimentTelemetryRecorder


class Recorder:

    def __init__(self, rds, exp_db, experiment_id) -> None:
        self.rds = rds
        self.exp_db = exp_db
        self.experiment_id = experiment_id

        self.telemetry_recorder = ExperimentTelemetryRecorder(rds, exp_db, experiment_id)
        self.event_recorder = ExperimentEventRecorderThread(ExperimentEventRecorder(rds, exp_db, experiment_id))

    def start(self):
        self.telemetry_recorder.start()
        self.event_recorder.start()

    def join(self):
        self.telemetry_recorder.join()
        self.event_recorder.join()

    def stop(self):
        self.telemetry_recorder.stop()
        self.event_recorder.stop()
