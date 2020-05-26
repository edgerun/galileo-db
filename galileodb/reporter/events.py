from galileodb.model import Event


class RedisEventReporter:

    def __init__(self, rds) -> None:
        self.rds = rds

    def report(self, event: Event):
        channel = f'galileo/events'

        if event.value is None:
            msg = f'{event.timestamp} {event.name}'
        else:
            msg = f'{event.timestamp} {event.name} {event.value}'

        self.rds.publish(channel, msg)
