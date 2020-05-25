import argparse
import logging
import os
import signal
import time

import redis

from galileodb.factory import create_experiment_database_from_env
from galileodb.model import Experiment, generate_experiment_id
from galileodb.telemetry import ExperimentTelemetryRecorder

logger = logging.getLogger(__name__)


def main():
    logging.basicConfig(level=logging.INFO)
    signal.signal(signal.SIGTERM, handle_sigterm)

    parser = argparse.ArgumentParser()
    parser.add_argument('--name', required=False, help='set name of experiment', default='')
    parser.add_argument('--creator', required=False, help='set name of creator', default='')
    args = parser.parse_args()

    experiment_id = generate_experiment_id()
    if args.name:
        name = args.name
    else:
        name = experiment_id

    if args.creator:
        creator = args.creator
    else:
        creator = 'galileodb-recorder-' + str(os.getpid())

    rds = redis.Redis(
        host=os.getenv('galileo_redis_host', 'localhost'),
        port=int(os.getenv('galileo_redis_port', 6379)),
        decode_responses=True
    )

    exp_db = create_experiment_database_from_env()
    exp_db.open()

    exp = Experiment(id=experiment_id, name=name, creator=creator, start=time.time(), created=time.time(), status='RUNNING')
    exp_db.save_experiment(exp)

    recorder = None
    try:
        logger.info('Start TelemetryRecorder')
        recorder = ExperimentTelemetryRecorder(rds, exp_db, experiment_id)
        recorder.save_nodeinfos()
        recorder.run()
    except KeyboardInterrupt:
        pass
    finally:
        exp_db.finalize_experiment(exp, 'FINISHED')
        logger.info('Closing TelemetryRecorder')
        if recorder:
            recorder.close()
        exp_db.close()


def handle_sigterm(*args):
    raise KeyboardInterrupt


if __name__ == '__main__':
    main()
