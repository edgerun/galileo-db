from telemc import Telemetry


class RedisTelemetryReporter:

    # TODO belongs in telemc

    def __init__(self, rds) -> None:
        self.rds = rds

    def report(self, telemetry: Telemetry):
        channel = f'telem/{telemetry.node}/{telemetry.metric}'

        if telemetry.subsystem:
            channel += '/' + telemetry.subsystem

        msg = f'{telemetry.timestamp} {telemetry.value}'

        self.rds.publish(channel, msg)
