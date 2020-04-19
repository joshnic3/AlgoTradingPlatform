from library.file_utils import add_dir, get_environment_specific_path, read_json_file
from library.db_utils import initiate_database, generate_unique_id


def setup_database_environment_path(db_root_path, schema, environment):
    path = get_environment_specific_path(db_root_path, environment.lower())
    # Back up feature should not be used in case there are multiple applications.
    add_dir(path, overwrite=True)
    return [initiate_database(db_root_path, d, schema[d], environment) for d in schema]


def add_twap_required_tickers(db, required_tickers):
    for required_ticker in required_tickers:
        twap_required_ticker_id = generate_unique_id(''.join(str(required_ticker)))
        db.insert_row('twap_required_tickers', [twap_required_ticker_id] + required_ticker)


def add_strategy(db, name, risk_profile, args, function):
    strategy_id = generate_unique_id(name)
    values = [strategy_id, name.lower(), risk_profile, args, function.lower(), True]
    db.insert_row('strategies', values)
    return strategy_id


def add_data_source(db, name, config):
    data_source_id = generate_unique_id(name)
    db.insert_row('data_sources', [data_source_id, name, config])


def add_risk_profile(db, values):
    risk_profile_id = generate_unique_id(''.join(str(values)))
    db.insert_row('risk_profiles', [risk_profile_id] + values)
    return risk_profile_id


def add_portfolio(db, name, exchange_name, capital):
    portfolio_id = generate_unique_id(name)
    db.insert_row('portfolios', [portfolio_id, name, exchange_name, capital, None])
    return portfolio_id


def add_assets(db, portfolio_id, symbol, units):
    asset_id = generate_unique_id(symbol)
    db.insert_row('assets', [asset_id, portfolio_id, symbol, units])
