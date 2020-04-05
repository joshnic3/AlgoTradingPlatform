import os

from library.file_utils import get_environment_specific_path, add_dir, read_json_file
from library.db_interface import Database

# Seperated as may be used by flask service in future.


def setup_database_environments(db_root_path):
    db_configs = read_json_file(os.path.join(db_root_path, 'databases.json'))
    databases = dict(db_configs['databases'])
    environments = list(db_configs['environments'])
    for environment in environments:
        path = os.path.join(db_root_path, environment.lower())
        add_dir(path, overwrite=True)
        for database in databases:
            db_file = os.path.join(db_root_path, environment, '{0}.db'.format(database))
            with open(db_file, 'w') as fp:
                pass
            Database(db_root_path, database, auto_create=True, environment=environment.lower())


def add_twap_required_tickers(environment, required_tickers):
    root_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/data'
    db = Database(root_path, 'algo_trading_platform', True, environment=environment.lower())
    for required_ticker in required_tickers:
        db.insert_row('twap_required_tickers', required_ticker)


def add_data_source(environment, name, config):
    root_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/data'
    db = Database(root_path, 'data_sources', True, environment=environment.lower())
    values = [0, name, config]
    db.insert_row('data_sources', values)
