import os
import datetime
import sqlite3

from library.file_utils import get_environment_specific_path, parse_wildcards


def generate_unique_id(seed):
    return str(abs(hash(str(seed) + datetime.datetime.now().strftime('%Y%m%d%H%M%S'))))


def initiate_database(db_root_path, name, schema, environment):
    db_path = get_environment_specific_path(db_root_path, environment)
    db_file_path = os.path.join(db_path, '{0}.db'.format(name))

    # Create db file if it doesnt already exist.
    with open(db_file_path, 'w') as db_file:
        pass

    # Create tables.
    db = Database(db_root_path, name, environment)
    for table in schema:

        columns = ['{} TEXT'.format(c) for c in schema[table]]
        sql = 'CREATE TABLE {0} ({1});'.format(table, ', '.join(columns))
        db.add_table(table, sql)
    return db


class Database:

    def __init__(self, db_root_path, name, environment):
        self._name = name

        # Check database file exists.
        db_path = get_environment_specific_path(db_root_path, environment)
        db_file_path = os.path.join(db_path, '{0}.db'.format(self._name))
        if not os.path.exists(db_file_path):
            raise Exception('Database not found in path: {}'.format(db_path))

        self._connection = sqlite3.connect(db_file_path)
        self._cursor = self._connection.cursor()
        self._environment = environment
        self.tables = [i[0] for i in
                       self.execute_sql("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name;")]

    def __str__(self):
        return '[name: {0}, environment: {1}]'.format(self._name, self._environment)

    def execute_sql(self, sql):
        self._cursor.execute(sql)
        results = [list(i) for i in self._cursor.fetchall()]
        self._connection.commit()
        return results

    def insert_row(self, table, values):
        values = [str(v) for v in values]
        if table.lower() not in self.tables:
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

    def update_value(self, table, column, value,condition):
        if table.lower() not in self.tables:
            return None
        sql = 'UPDATE {0} SET {1}="{2}"{3}'.format(table,
                                                     column,
                                                     value,
                                                     ' WHERE {};'.format(condition) if condition else ';')
        self.execute_sql(sql)

    def get_one_row(self, table, condition, columns=None):
        results = self.query_table(table, condition, columns)
        if len(results) > 1:
            raise Exception('Database query expected only one row, got {0}.'.format(len(results)))
        return results[0]

    def add_table(self, table, sql):
        self.tables.append(table)
        self.execute_sql(sql)

    def log(self, log):
        log.info('Connected to database: {0}'.format(self.__str__()))
