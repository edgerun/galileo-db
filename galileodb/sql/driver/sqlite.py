import sqlite3

from galileodb.sql.adapter import SqlAdapter


class SqliteAdapter(SqlAdapter):
    placeholder = '?'

    def _connect(self, *args, **kwargs):
        return sqlite3.connect(*args, **kwargs)
