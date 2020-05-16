import datetime
import json
import optparse
import os
import sys

from flask import Flask, request

from library.bootstrap import Constants
from library.data_loader import DataLoader
from library.interfaces.exchange import AlpacaInterface
from library.interfaces.sql_database import Database, query_result_to_dict
from library.utilities.file import parse_configs_file
from library.utilities.job import is_script_new
from strategy_executor import TradeExecutor

app = Flask(__name__)


def response(status, data=None):
    if data:
        return app.response_class(response=json.dumps(data), status=status, mimetype='application/json')
    return app.response_class(status=status, mimetype='application/json')


@app.route('/exchange_open')
def exchange_open():
    # Return "True" if exchange is open and "False" if it is closed.
    exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], simulator=True)
    return response(200, str(exchange.is_exchange_open()))


@app.route('/market_data')
def market_data():
    # Returns a time series data for a ticker with provided symbol and within a given datetime range.
    # Cannot return as dict as element order is not guaranteed.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'symbol' in params:
        symbol = params['symbol'].upper()

    if 'before' in params:
        before = params['before']
    else:
        before = datetime.datetime.now()

    if 'after' in params:
        after = params['after']
    else:
        after = datetime.datetime.now() - datetime.timedelta(hours=24)

    # Return data.
    data_loader = DataLoader()
    data_loader.load_tickers(symbol, before, after)
    data = data_loader.data['ticker'][symbol]
    data = [(d[0].strftime(Constants.pp_time_format), d[1]) for d in data]
    return response(200, data)


@app.route('/strategies')
def strategies():
    # Returns strategy row as dict, will return for all strategies for for one specified by provided strategy_id

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        # Get portfolio data.
        strategy_row = db.get_one_row('strategies', 'id="{0}"'.format(params['id']))
        if strategy_row is None:
            return response(400, 'Strategy does not exist.')
        strategy_dict = query_result_to_dict([strategy_row], Constants.configs['tables']['algo_trading_platform']['strategies'])
        return response(200, strategy_dict)

    all_rows = db.query_table('strategies')
    all_rows_as_dict = query_result_to_dict(all_rows, Constants.configs['tables']['algo_trading_platform']['strategies'])
    return response(200, all_rows_as_dict)




@app.route('/portfolio')
def portfolio():
    # Returns portfolio row as dictionary for provided portfolio id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        # Get portfolio data.
        portfolio_row = db.get_one_row('portfolios', 'id="{0}"'.format(params['id']))
        if portfolio_row is None:
            return response(400, 'Portfolio does not exist.')

        portfolio_dict = query_result_to_dict([portfolio_row], Constants.configs['tables']['algo_trading_platform']['portfolios'])[0]

        # Add current and historical value to portfolio data.
        historical_valuations_rows = db.query_table('historical_portfolio_valuations', 'portfolio_id="{0}"'.format(
                    params['id']))
        historical_date_times = [r[2] for r in historical_valuations_rows]
        historical_values = [float(r[3]) for r in historical_valuations_rows]

        historical_valuations = [historical_date_times, historical_values]
        portfolio_dict['historical_valuations'] = historical_valuations
        if len(historical_valuations) > 1:
            if historical_values:
                portfolio_dict['value'] = historical_values[-1]
            else:
                portfolio_dict['value'] = '-'
        else:
            if historical_values:
                portfolio_dict['value'] = historical_valuations[0]
            else:
                portfolio_dict['value'] = '-'

        # Calculate total exposure.
        portfolio_dict['exposure'] = 0
        for asset in db.query_table('assets', 'portfolio_id="{0}"'.format(params['id'])):
            asset_dict = query_result_to_dict([asset], Constants.configs['tables']['algo_trading_platform']['assets'])[0]
            portfolio_dict['exposure'] += float(asset_dict['current_exposure'])

        # Add 24hr PnL to portfolio data.
        twenty_four_hrs_ago = datetime.datetime.now() - datetime.timedelta(hours=24)
        valuations = []
        for row in historical_valuations_rows:
            if datetime.datetime.strptime(row[2], Constants.date_time_format) > twenty_four_hrs_ago:
                valuations.append([row[2], row[3]])
        if len(valuations) > 1:
            portfolio_dict['pnl'] = round(float(valuations[0][1]) - float(valuations[-1][1]), 2)
        else:
            portfolio_dict['pnl'] = '-'

        return response(200, portfolio_dict)

    return response(401, 'Portfolio id required.')


@app.route('/portfolio/assets')
def assets():
    # Returns asset rows as a dictionary for a given portfolio id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        # Needs abstracting out, but ok like this for now. This takes time so look at caching it.
        exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], simulator=True)
        trade_executor = TradeExecutor(db, params['id'], exchange)
        trade_executor.sync_portfolio_with_exchange()
        if trade_executor is None:
            return response(400, 'Portfolio does not exist.')
        return response(200, trade_executor.portfolio['assets'])

    return response(401, 'Portfolio id required.')


@app.route('/job')
def job():
    # Returns job row as dictionary for provided job id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        job_row = db.get_one_row('jobs', 'id="{0}"'.format(params['id']))
        job_dict = query_result_to_dict([job_row], Constants.configs['tables']['algo_trading_platform']['jobs'])[0]
        new_script = is_script_new(db, job_dict['script'])
        job_dict['version'] = '{0}{1}'.format(job_dict['version'], '(NEW)' if new_script else '')

        # Extract phase data.
        phase_row = db.query_table('phases', 'job_id="{0}"'.format(job_dict['id']))
        phase_dict = query_result_to_dict(phase_row, Constants.configs['tables']['algo_trading_platform']['phases'])[-1]
        job_dict['phase_name'] = phase_dict['name']
        phase_datetime = datetime.datetime.strptime(phase_dict['datetime'], Constants.date_time_format)
        job_dict['phase_datetime'] = phase_datetime.strftime(Constants.pp_time_format)
        return response(200, job_dict)

    return response(401, 'Job id required.')


@app.route('/job/log')
def log():
    # Returns log text for provided job id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        job_row = db.get_one_row('jobs', 'id="{0}"'.format(params['id']))
        if job_row is None:
            return response(401, 'Job doesnt exist.')
        job_dict = query_result_to_dict([job_row], Constants.configs['tables']['algo_trading_platform']['jobs'])[0]
        with open(job_dict['log_path'], 'r') as file:
            data = file.read().replace('\n', '<br>')
        return response(200, data)

    return response(401, 'Job id required.')


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
    Constants.configs = parse_cmdline_args('algo_trading_platform')

    app.run('0.0.0.0')
    app.run()
