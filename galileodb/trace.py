import csv
import logging
import os
from abc import ABC
from multiprocessing import Process
from multiprocessing.queues import Queue
from queue import Empty
from typing import List

from galileodb.db import ExperimentDatabase
from galileodb.model import RequestTrace
from galileodb.reporter.traces import RedisTraceReporter
from galileodb.sql.adapter import ExperimentSQLDatabase

logger = logging.getLogger(__name__)

POISON = "__POISON__"
START = "__START__"
PAUSE = "__PAUSE__"
FLUSH = '__FLUSH__'


class TraceWriter(ABC):
    def write(self, traces: List[RequestTrace]):
        raise NotImplementedError


class TraceLogger(Process):
    flush_interval = 20

    def __init__(self, trace_queue: Queue, writer: TraceWriter = None, start=True) -> None:
        super().__init__()
        self.traces = trace_queue
        self.closed = False
        self.buffer = list()
        self.writer = writer
        self.running = start

    def run(self):
        try:
            return self.listen()
        finally:
            self.flush()

    def flush(self):
        if not self.buffer:
            logger.debug('buffer empty, not flushing')
            return

        logger.debug('flushing trace buffer')

        if self.writer:
            try:
                self.writer.write(self.buffer)
            except Exception as e:
                if logger.isEnabledFor(logging.DEBUG):
                    logger.exception('error writing traces')
                else:
                    logger.error('error writing traces: %s', e)
        else:
            logger.debug('no writer to flush to')

        self.buffer.clear()

    def close(self):
        self.closed = True
        self.traces.put(POISON)

    def listen(self):
        timeout = None
        while True:
            if self.closed and timeout is None:
                logger.debug('setting read timeout to 2 seconds')
                timeout = 2

            try:
                trace = self.traces.get(timeout=timeout)

                if trace == POISON:
                    logger.debug('poison received, setting closed to true')
                    self.closed = True
                    break
                elif trace == FLUSH:
                    logger.debug('flush command received, flushing buffer')
                    self.flush()
                    continue
                elif trace == START:
                    logger.debug('start received')
                    self.running = True
                    continue
                elif trace == PAUSE:
                    logger.debug('pause received, flushing remaining traces')
                    self.running = False
                    self.flush()
                    continue

                if self.running:
                    self.buffer.append(trace)

                if len(self.buffer) >= self.flush_interval:
                    logger.debug('flush interval reached, flushing buffer')
                    self.flush()

            except KeyboardInterrupt:
                break
            except Empty:
                logger.debug('queue is empty, exiting')
                return


class RedisTopicTraceWriter(TraceWriter):

    def __init__(self, rds) -> None:
        super().__init__()
        self.reporter = RedisTraceReporter(rds)

    def write(self, traces: List[RequestTrace]):
        self.reporter.report_multiple(traces)


class DatabaseTraceWriter(TraceWriter):
    experiment_db: ExperimentDatabase

    def __init__(self, experiment_db: ExperimentDatabase) -> None:
        self.experiment_db = experiment_db
        self.connected = False

    def write(self, traces: List[RequestTrace]):
        self._assert_connection()
        self.experiment_db.save_traces(traces)

    def _assert_connection(self):
        # this is a terrible hack due to multiprocessing issues:
        # close() will delete the threadlocal (which is not actually accessible from the process) and create a new
        # connection. The SqlAdapter adapter design may be broken. or python multiprocessing...
        if self.connected:
            return

        if isinstance(self.experiment_db, ExperimentSQLDatabase):
            self.experiment_db.db.reconnect()
            self.connected = True


class FileTraceWriter(TraceWriter):

    def __init__(self, host_name, target_dir='/tmp/mc2/exp') -> None:
        self.target_dir = target_dir
        self.file_name = 'traces-%s.csv' % host_name
        self.file_path = os.path.join(self.target_dir, self.file_name)
        self.mkdirp(self.target_dir)
        self.init_file()

    def init_file(self):
        logger.debug('initializing trace file logger to log into %s', self.file_path)
        if os.path.exists(self.file_path):
            return

        logger.debug('initializing %s with header', self.file_path)
        with open(self.file_path, 'w') as fd:
            csv.writer(fd).writerow(RequestTrace._fields)

    def write(self, buffer: List[RequestTrace]):
        with open(self.file_path, 'a') as fd:
            writer = csv.writer(fd)
            for row in buffer:
                writer.writerow(row)

    @staticmethod
    def mkdirp(path):
        if not os.path.exists(path):
            os.makedirs(path)

        if os.path.isfile(path):
            raise FileExistsError("%s is an existing file" % path)
