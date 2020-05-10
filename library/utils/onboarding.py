from library.database_interface import generate_unique_id


def add_strategy(db, name, porfolio_id):
    strategy_id = generate_unique_id(name)
    values = [strategy_id, name.lower(), porfolio_id]
    db.insert_row('strategies', values)
    return strategy_id


def add_data_source(db, name, config):
    data_source_id = generate_unique_id(name)
    db.insert_row('data_sources', [data_source_id, name, config])


def add_portfolio(db, name, exchange_name, capital, weighting):
    portfolio_id = generate_unique_id(name)
    db.insert_row('portfolios', [portfolio_id, exchange_name, capital, weighting, None])
    return portfolio_id


def add_assets(db, portfolio_id, symbol, units):
    asset_id = generate_unique_id(symbol)
    db.insert_row('assets', [asset_id, portfolio_id, symbol, units])
