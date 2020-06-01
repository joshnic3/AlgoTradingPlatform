from hashlib import md5

from library.interfaces.sql_database import generate_unique_id


def generate_unique_id(seed):
    hash_object = md5(seed.encode())
    return hash_object.hexdigest()


def add_strategy(db, name, porfolio_id):
    strategy_id = generate_unique_id(name)
    values = [strategy_id, name.lower(), porfolio_id, None]
    db.insert_row('strategies', values)
    return strategy_id


def add_data_source(db, name, config):
    data_source_id = generate_unique_id(name)
    db.insert_row('data_sources', [data_source_id, name, config])


def add_portfolio(db, name, allocation, cash=0.0):
    portfolio_id = generate_unique_id(name)
    db.insert_row('portfolios', [portfolio_id, 'alpaca', cash, allocation])
    return portfolio_id


def add_assets(db, portfolio_id, symbol):
    asset_id = generate_unique_id(symbol)
    db.insert_row('assets', [asset_id, portfolio_id, symbol, 0, 0.0])
