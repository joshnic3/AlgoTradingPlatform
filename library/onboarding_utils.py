from library.file_utils import add_dir, get_environment_specific_path, read_json_file
from library.db_interface import Database, initiate_database


def setup_database_environment_path(db_root_path, app_config_path, databases, environment):
    path = get_environment_specific_path(db_root_path, environment.lower())
    schema = read_json_file(app_config_path)['schema']
    # Back up feature should not be used in case there are multiple applications.
    add_dir(path, overwrite=True)
    for database in databases:
        initiate_database(db_root_path, database, schema, environment)


def add_twap_required_tickers(db, required_tickers):
    # db = Database(db_root_path, 'algo_trading_platform', environment=environment.lower())
    for required_ticker in required_tickers:
        db.insert_row('twap_required_tickers', required_ticker)


def add_data_source(db, name, config):
    # db = Database(db_root_path, 'data_sources', environment=environment.lower())
    values = ['0', name, config]
    db.insert_row('data_sources', values)
