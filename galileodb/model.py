import uuid
from datetime import datetime
from typing import NamedTuple, List, Dict


def generate_experiment_id():
    prefix = datetime.strftime(datetime.now(), '%Y%m%d%H%M')
    suffix = str(uuid.uuid4())[:4]
    return prefix + '-' + suffix


class Experiment:
    id: str
    name: str
    creator: str
    start: float
    end: float
    created: float
    status: str

    def __init__(self, id: str = None, name: str = None, creator: str = None, start: float = None, end: float = None,
                 created: float = None, status: str = None) -> None:
        super().__init__()
        self.id = id
        self.name = name
        self.creator = creator
        self.start = start
        self.end = end
        self.created = created
        self.status = status

    def __repr__(self):
        return self.__str__()

    def __str__(self) -> str:
        return 'Experiment%s' % self.__dict__


class Telemetry(NamedTuple):
    timestamp: float
    metric: str
    node: str
    value: float
    exp_id: str
    subsystem: str = None


class NodeInfo(NamedTuple):
    node: str
    data: Dict[str, str]
    exp_id: str


class Event(NamedTuple):
    timestamp: float
    name: str
    value: str = None


class ExperimentEvent(NamedTuple):
    exp_id: str
    timestamp: float
    name: str
    value: str = None


class WorkloadConfiguration(NamedTuple):
    service: str
    ticks: List[int]
    clients_per_host: int
    arrival_pattern: str
    client: str = None
    client_parameters: dict = dict()


class ExperimentConfiguration(NamedTuple):
    duration: int
    interval: int
    workloads: List[WorkloadConfiguration]


class QueuedExperiment(NamedTuple):
    experiment: Experiment
    configuration: ExperimentConfiguration


class RequestTrace(NamedTuple):
    request_id: str
    client: str
    service: str
    created: float
    sent: float
    done: float
    status: int = -1
    server: str = None
    exp_id: str = None
    headers: str = None
    response: str = None  # should be the last field for easier line-based transmission
