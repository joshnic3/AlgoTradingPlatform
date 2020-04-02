import os

import sqlite3

from library.file_utils import get_environment_specific_path
from library.file_utils import read_json_file


class Database:

    def __init__(self, root_path, environment, schema=None):
        db_file_path = os.path.join(get_environment_specific_path(root_path, environment),
                                    'algo_trading_platform.db')
        if not os.path.exists(db_file_path):
            raise Exception('Database not found in path: {}'.format(root_path))
        self._connection = sqlite3.connect(db_file_path)
        self._cursor = self._connection.cursor()
        self.tables = [i[0] for i in
                       self._execute_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")]

        if schema:
            for required_table in schema:
                if required_table not in self.tables:
                    sql = schema[required_table].replace('%table%', required_table)
                    self._execute_sql(sql)

    def _execute_sql(self, sql):
        self._cursor.execute(sql)
        results = [list(i) for i in self._cursor.fetchall()]
        self._connection.commit()
        return results

    def insert_row(self, table, values):
        if table not in self.tables:
            return None
        sql = 'INSERT INTO {0} VALUES ("{1}");'.format(table, '", "'.join(values))
        self._execute_sql(sql)

    def query_table(self, table, condition=None, columns=None):
        if table not in self.tables:
            return None
        sql = 'SELECT {0} FROM {1}{2}'.format(', '.columns if columns else '*',
                                              table,
                                              ' WHERE {};'.format(condition) if condition else ';')
        return self._execute_sql(sql)
