import logging
import os
from typing import MutableMapping

from galileodb.db import ExperimentDatabase
from galileodb.mixed.db import MixedExperimentDatabase
from galileodb.sql.adapter import ExperimentSQLDatabase

logger = logging.getLogger(__name__)


def create_experiment_database_from_env(env: MutableMapping = os.environ) -> ExperimentDatabase:
    driver = env.get('galileo_expdb_driver', 'sqlite')
    return create_experiment_database(driver)


def create_experiment_database(driver: str, env: MutableMapping = os.environ) -> ExperimentDatabase:
    if driver == 'influxdb':
        return create_influxdb_from_env(env)

    from galileodb.sql.adapter import ExperimentSQLDatabase

    if driver == 'mixed':
        return create_mixeddb_from_env(env)

    if driver == 'sqlite':
        db_adapter = create_sqlite_from_env(env)

    elif driver == 'mysql':
        db_adapter = create_mysql_from_env(env)

    else:
        raise ValueError('unknown database driver %s' % driver)

    return ExperimentSQLDatabase(db_adapter)


def create_influxdb_from_env(env: MutableMapping = os.environ):
    from galileodb.influx.db import InfluxExperimentDatabase
    from influxdb_client import InfluxDBClient

    params = {
        'url': env.get('galileo_expdb_influxdb_url', 'http://localhost:8086'),
        'token': env.get('galileo_expdb_influxdb_token', 'my-token'),
        'timeout': int(env.get('galileo_expdb_influxdb_timeout', '10000')),
        'org': env.get('galileo_expdb_influxdb_org', 'galileo'),
        'org_id': env.get('galileo_expdb_influxdb_org_id', 'org-id')
    }

    return InfluxExperimentDatabase(InfluxDBClient(**params), org_name=params['org'], org_id=params['org_id'])


def create_mysql_from_env(env: MutableMapping = os.environ):
    from galileodb.sql.driver.mysql import MysqlAdapter
    params = {
        'host': env.get('galileo_expdb_mysql_host', 'localhost'),
        'port': int(env.get('galileo_expdb_mysql_port', '3307')),
        'user': env.get('galileo_expdb_mysql_user', None),
        'password': env.get('galileo_expdb_mysql_password', None),
        'db': env.get('galileo_expdb_mysql_db', None)
    }

    logger.info('read mysql adapter parameters from environment %s', params)
    return MysqlAdapter(**params)


def create_sqlite_from_env(env: MutableMapping = os.environ):
    from galileodb.sql.driver.sqlite import SqliteAdapter
    db_file = env.get('galileo_expdb_sqlite_path', './galileodb.sqlite')

    logger.info('creating db adapter to SQLite %s', os.path.realpath(db_file))
    return SqliteAdapter(db_file)


def create_mixeddb_from_env(env: MutableMapping = os.environ):
    influxdb = create_influxdb_from_env(env)
    mysql_adapter = create_mysql_from_env(env)
    sqldb = ExperimentSQLDatabase(mysql_adapter)

    return MixedExperimentDatabase(influxdb, sqldb)
