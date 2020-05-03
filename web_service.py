import json
import optparse
import os
import sys

from flask import Flask, request

from library.database_interface import Database
from library.exchange_interface import AlpacaInterface
from library.utils.file import parse_configs_file

app = Flask(__name__)


def response(status, data=None):
    if data:
        return app.response_class(response=json.dumps(data), status=status, mimetype='application/json')
    return app.response_class(status=status, mimetype='application/json')


@app.route('/exchange_open')
def exchange_open():
    # Create an exchange instance. Always use simulator mode as we don't need/want to execute any real world trades.
    exchange = AlpacaInterface(configs['API_ID'], configs['API_SECRET_KEY'], simulator=True)
    return response(200, str(exchange.is_exchange_open()))


@app.route('/twaps')
def twaps():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(configs['db_root_path'], 'algo_trading_platform', configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    # Query TWAP table.
    if 'symbol' in params:
        if 'before' in params and 'after' in params:
            condition = 'symbol="{0}" AND start_time>"{1}" AND end_time<"{2}"'.format(params['symbol'], params['after'],
                                                                                      params['before'])
        else:
            condition = 'symbol="{0}"'.format(params['symbol'])

        result = db.query_table('twaps', condition)
    else:
        result = db.query_table('twaps')

    start_times = [r[1] for r in result]
    values = [r[4] for r in result]
    return response(200, list(zip(start_times, values)))


@app.route('/twaps/symbols')
def symbols():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(configs['db_root_path'], 'algo_trading_platform', configs['environment'])

    # Query TWAP table.
    result = db.execute_sql('SELECT DISTINCT symbol FROM twaps;')
    return response(200, [s[0] for s in result])


@app.route('/strategies')
def strategies():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    database_name = 'algo_trading_platform'
    db = Database(configs['db_root_path'], database_name, configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        # Get portfolio data.
        strategy_row = db.get_one_row('strategies', 'id="{0}"'.format(params['id']))
        if strategy_row is None:
            return response(400, 'Strategy does not exist.')
        data = {
            'name': strategy_row[1],
            'portfolio_id': strategy_row[3],
        }
        return response(200, data)

    strategy_ids = [r[0] for r in db.query_table('strategies')]
    return response(200, strategy_ids)


@app.route('/portfolios')
def portfolios():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    database_name = 'algo_trading_platform'
    db = Database(configs['db_root_path'], database_name, configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        # Get portfolio data.
        portfolio_row = db.get_one_row('portfolios', 'id="{0}"'.format(params['id']))
        if portfolio_row is None:
            return response(400, 'Portfolio does not exist.')
        asset_rows = db.query_table('assets', 'portfolio_id="{0}"'.format(portfolio_row[0]))
        historical_valuations_rows = db.query_table('historical_portfolio_valuations', 'portfolio_id="{0}"'.format(
            params['id']))
        valuation_date_times = [r[2] for r in historical_valuations_rows]
        valuation_values = [float(r[3]) for r in historical_valuations_rows]

        data = {"assets": {r[2]: int(r[3]) for r in asset_rows},
                "cash": float(portfolio_row[2]),
                "historical_valuations": list(zip(valuation_date_times, valuation_values)),
                "updated_by": portfolio_row[4]}
        return response(200, data)

    portfolio_ids = [r[0] for r in db.query_table('portfolios')]
    return response(200, portfolio_ids)


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
    })


if __name__ == '__main__':
    # Setup configs.
    global configs
    configs = parse_cmdline_args('algo_trading_platform')

    app.run('0.0.0.0')
    # app.run()
