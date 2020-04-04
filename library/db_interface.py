import os

import sqlite3

from library.file_utils import get_environment_specific_path
from library.file_utils import read_json_file


class Database:

    def __init__(self, root_path, name, auto_create=False, environment='prod'):
        # Read db configs.
        _db_path = os.path.join(get_environment_specific_path(root_path, environment))
        _db_configs = read_json_file(os.path.join(root_path, 'databases.json'))
        _databases = dict(_db_configs['databases'])
        if name not in _databases:
            raise Exception('Database {0} not found in configs.'.format(name))
        self._name = name
        db_file_path = os.path.join(_db_path, '{}.db'.format(self._name))
        if not os.path.exists(db_file_path):
            raise Exception('Database not found in path: {}'.format(root_path))
        self._connection = sqlite3.connect(db_file_path)
        self._cursor = self._connection.cursor()
        self._environment = environment
        self.tables = [i[0] for i in
                       self.execute_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")]
        if auto_create:
            schema = _databases[name]['schema']
            for required_table in schema:
                if required_table not in self.tables:
                    sql = schema[required_table].replace('%table%', required_table)
                    self.execute_sql(sql)

    def execute_sql(self, sql):
        self._cursor.execute(sql)
        results = [list(i) for i in self._cursor.fetchall()]
        self._connection.commit()
        return results

    def insert_row(self, table, values):
        values = [str(v) for v in values]
        if table not in self.tables:
            return None
        sql = 'INSERT INTO {0} VALUES ("{1}");'.format(table, '", "'.join(values))
        self.execute_sql(sql)

    def query_table(self, table, condition=None, columns=None):
        if isinstance(columns, list):
            columns = ', '.join(columns)
        if columns is None:
            columns = '*'
        if table not in self.tables:
            return None
        sql = 'SELECT {0} FROM {1}{2}'.format(columns,
                                              table,
                                              ' WHERE {};'.format(condition) if condition else ';')
        return self.execute_sql(sql)

    def log_status(self, log):
        status = 'Connected to database: {0}, Environment: {1}'.format(self._name, self._environment)
        log.info(status)