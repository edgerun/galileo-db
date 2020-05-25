import logging
import os
from typing import MutableMapping

from galileodb.db import ExperimentDatabase

logger = logging.getLogger(__name__)


def create_experiment_database_from_env(env: MutableMapping = os.environ) -> ExperimentDatabase:
    driver = env.get('galileo_expdb_driver', 'sqlite')
    return create_experiment_database(driver)


def create_experiment_database(driver: str, env: MutableMapping = os.environ) -> ExperimentDatabase:
    from galileodb.sql.adapter import ExperimentSQLDatabase

    if driver == 'sqlite':
        db_adapter = create_sqlite_from_env(env)

    elif driver == 'mysql':
        db_adapter = create_mysql_from_env(env)

    else:
        raise ValueError('unknown database driver %s' % driver)

    return ExperimentSQLDatabase(db_adapter)


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
