import abc
import logging
import os
import threading
from typing import Tuple, List, Dict, Optional

from galileodb.db import ExperimentDatabase
from galileodb.model import Experiment, Telemetry, RequestTrace, NodeInfo, ExperimentEvent

logger = logging.getLogger(__name__)


class SqlAdapter(abc.ABC):
    placeholder = '?'

    _thread_local = threading.local()

    def __init__(self, *args, **kwargs) -> None:
        super().__init__()
        self._thread_local.connection = None
        self.connect_args = args
        self.connect_kwargs = kwargs
        self._lock = threading.RLock()

    @property
    def connection(self):
        with self._lock:
            if 'connection' not in self._thread_local.__dict__ or self._thread_local.connection is None:
                logger.info('%s connecting to database', threading.current_thread().name)
                self._thread_local.connection = self._connect(*self.connect_args, **self.connect_kwargs)
            return self._thread_local.connection

    def reconnect(self):
        with self._lock:
            self.close()
            self.open()

    @property
    def db(self):
        return self.connection

    def cursor(self):
        return self.db.cursor()

    def execute(self, *args, **kwargs):
        cur = self.cursor()
        try:
            logger.debug('executing SQL %s %s', args, kwargs)
            cur.execute(*args, **kwargs)
            self.db.commit()
        finally:
            cur.close()

    def executemany(self, *args, **kwargs):
        cur = self.cursor()
        try:
            cur.executemany(*args, **kwargs)
            self.db.commit()
        finally:
            cur.close()

    def executescript(self, *args, **kwargs):
        cur = self.cursor()
        try:
            cur.executescript(*args, **kwargs)
            self.db.commit()
        finally:
            cur.close()

    def fetchone(self, *args, **kwargs):
        cur = self.cursor()
        try:
            cur.execute(*args, **kwargs)
            return cur.fetchone()
        finally:
            cur.close()

    def fetchmany(self, *args, **kwargs):
        cur = self.cursor()
        try:
            cur.execute(*args, **kwargs)
            return cur.fetchmany()
        finally:
            cur.close()

    def fetchall(self, *args, **kwargs):
        cur = self.cursor()
        try:
            cur.execute(*args, **kwargs)
            return cur.fetchall()
        finally:
            cur.close()

    def open(self):
        with self._lock:
            assert self.connection is not None

    def close(self):
        with self._lock:
            if 'connection' in self._thread_local.__dict__ and self._thread_local.connection is not None:
                try:
                    self._thread_local.connection.close()
                finally:
                    self._thread_local.connection = None

    def insert_one(self, table: str, data: Dict[str, object]):
        columns = self.sql_field_list(data.keys())
        placeholders = ','.join([self.placeholder] * len(data))
        values = list(data.values())

        # TODO: sanitize table and column inputs
        sql = f'INSERT INTO `{table}` ({columns}) VALUES ({placeholders})'

        logger.debug('running insert sql: %s' % sql)

        self.execute(sql, values)

    def insert_many(self, table: str, keys, data: List):
        columns = self.sql_field_list(keys)
        placeholders = ','.join([self.placeholder] * len(keys))

        # TODO: sanitize table and column inputs
        sql = f'INSERT INTO `{table}` ({columns}) VALUES ({placeholders})'

        logger.debug('running insert many sql on %d items: %s', len(data), sql)

        self.executemany(sql, data)

    def update_by_id(self, table: str, identity: Tuple[str, object], data: Dict[str, object]):
        set_statements, values = list(), list()
        id_col, id_val = identity

        for key, value in data.items():
            set_statements.append('`%s` = %s' % (key.upper(), self.placeholder))
            values.append(value)

        values.append(id_val)

        # TODO: sanitize table and column inputs
        sql = 'UPDATE `{table}` SET {set_statements} WHERE `{id_col}` = ' + self.placeholder
        sql = sql.format(table=table, set_statements=','.join(set_statements), id_col=id_col)

        logger.debug('running update sql: %s', sql)
        self.execute(sql, values)

    def sql_field_list(self, fields, table_prefix: str = None, uppercase=True) -> str:
        return ', '.join([self.sql_field_name(field, table_prefix, uppercase) for field in fields])

    def sql_field_name(self, field, table_prefix: str = None, uppercase=True):
        f = field.upper() if uppercase else field
        if table_prefix:
            return f'`{table_prefix}`.`{f}`'
        else:
            return f'`{f}`'

    def _connect(self, *args, **kwargs):
        raise NotImplementedError


class ExperimentSQLDatabase(ExperimentDatabase):
    SCHEMA_FILE = os.path.join(os.path.dirname(__file__), 'schema.sql')

    def __init__(self, db: SqlAdapter) -> None:
        super().__init__()
        self.db = db

    def read_schema_file(self):
        with open(self.SCHEMA_FILE, 'r') as fd:
            return fd.read()

    def open(self):
        self.db.open()
        self.db.executescript(self.read_schema_file())

    def close(self):
        self.db.close()

    def save_experiment(self, experiment: Experiment):
        data = dict(experiment.__dict__)
        data['exp_id'] = data['id']
        del data['id']
        logger.debug('saving experiment with data %s', data)
        self.db.insert_one('experiments', data)

    def update_experiment(self, experiment: Experiment):
        data = dict(experiment.__dict__)
        del data['id']

        self.db.update_by_id('experiments', ('exp_id', experiment.id), data)

    def delete_experiment(self, exp_id: str):
        experiment = self.get_experiment(exp_id)
        if experiment is None:
            raise ValueError('No such experiment %s' % exp_id)

        stmts = [
            "DELETE FROM `telemetry` WHERE EXP_ID = " + self.db.placeholder,
            "DELETE FROM `traces` WHERE EXP_ID = " + self.db.placeholder,
            "DELETE FROM `events` WHERE EXP_ID = " + self.db.placeholder,
            "DELETE FROM `experiments` WHERE EXP_ID = " + self.db.placeholder,
        ]

        for sql in stmts:
            try:
                self.db.execute(sql, (exp_id,))
            except Exception as e:
                logger.exception('Exception while executing %s', sql, e)

    def get_experiment(self, exp_id: str) -> Experiment:
        sql = f'SELECT * FROM `experiments` WHERE EXP_ID = {self.db.placeholder}'

        entry = self.db.fetchone(sql, (exp_id,))

        if entry:
            row = tuple(entry)
            return Experiment(*row)
        else:
            return None

    def save_traces(self, traces: List[RequestTrace]):
        self.db.insert_many('traces', RequestTrace._fields, traces)

    def touch_traces(self, experiment: Experiment):
        sql = 'UPDATE `traces` SET `EXP_ID` = ? WHERE CREATED >= ? AND CREATED <= ?'
        sql = sql.replace('?', self.db.placeholder)
        self.db.execute(sql, (experiment.id, experiment.start, experiment.end))

    def get_traces(self, exp_id=None) -> List[RequestTrace]:
        fields = self.db.sql_field_list(RequestTrace._fields)

        if exp_id is None:
            sql = f'SELECT {fields} from `traces`'
            entries = self.db.fetchall(sql)
        else:
            sql = f'SELECT {fields} from `traces` WHERE EXP_ID = {self.db.placeholder}'
            entries = self.db.fetchall(sql, (exp_id,))

        return list(map(lambda x: RequestTrace(*(tuple(x))), entries))

    def save_telemetry(self, telemetry: List[Telemetry]):
        self.db.insert_many('telemetry', Telemetry._fields, telemetry)

    def get_telemetry(self, exp_id=None) -> List[Telemetry]:
        fields = self.db.sql_field_list(Telemetry._fields)

        if exp_id is None:
            sql = f'SELECT {fields} FROM `telemetry`'
            entries = self.db.fetchall(sql)
        else:
            sql = f'SELECT {fields} FROM `telemetry` WHERE EXP_ID = {self.db.placeholder}'
            entries = self.db.fetchall(sql, (exp_id,))

        return list(map(lambda x: Telemetry(*(tuple(x))), entries))

    def save_event(self, event: ExperimentEvent):
        self.db.insert_one('events', event._asdict())

    def save_events(self, events: List[ExperimentEvent]):
        self.db.insert_many('events', ExperimentEvent._fields, events)

    def get_events(self, exp_id=None) -> List[ExperimentEvent]:
        fields = self.db.sql_field_list(ExperimentEvent._fields)

        if exp_id is None:
            sql = f'SELECT {fields} FROM `events`'
            entries = self.db.fetchall(sql)
        else:
            sql = f'SELECT {fields} FROM `events` WHERE EXP_ID = {self.db.placeholder}'
            entries = self.db.fetchall(sql, (exp_id,))

        return list(map(lambda x: ExperimentEvent(*(tuple(x))), entries))

    def save_nodeinfos(self, infos: List[NodeInfo]):
        keys = ('exp_id', 'node', 'info_key', 'info_value')

        data = list()
        for info in infos:
            for k, v in info.data.items():
                data.append((info.exp_id, info.node, k, v))

        self.db.insert_many('nodeinfo', keys, data)

    def find_all(self) -> List[Experiment]:
        fields = ['exp_id', 'name', 'creator', 'start', 'end', 'created', 'status']
        fields = self.db.sql_field_list(fields)

        sql = f'SELECT {fields} FROM `experiments`'

        entries = self.db.fetchall(sql)

        return list(map(lambda x: Experiment(*(tuple(x))), entries))

    def get_running_experiment(self) -> Optional[Experiment]:
        fields = ['exp_id', 'name', 'creator', 'start', 'end', 'created', 'status']
        fields = self.db.sql_field_list(fields)
        sql = f"SELECT {fields} FROM `experiments` WHERE `STATUS` = 'RUNNING'"

        entry = self.db.fetchone(sql)

        if entry:
            row = tuple(entry)
            return Experiment(*row)
        else:
            return None
