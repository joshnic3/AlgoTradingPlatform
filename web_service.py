import datetime
import json
import os

from flask import Flask, request

import strategy_regression_tester
import strategy_executor

from library.bootstrap import Constants
from library.data_loader import MarketDataLoader, BreadCrumbsDataLoader
from library.interfaces.exchange import AlpacaInterface
from library.interfaces.sql_database import Database, query_result_to_dict
from library.strategy.bread_crumbs import BreadCrumbs, group_bread_crumbs_by_run_time
from library.strategy.portfolio import Portfolio
from library.utilities.authentication import public_key_from_private_key, secret_key
from library.utilities.job import is_script_new, Job
from library.utilities.script_runner import ScriptRunner

app = Flask(__name__)


def float_to_str(value_float):
    return '{:,.2f}'.format(value_float)


def int_to_str(value_int):
    return '{:,d}'.format(value_int)


def format_datetime_str(datetime_string, date_time_format=Constants.PP_TIME_FORMAT):
    if datetime_string is None:
        return None
    date_time = datetime.datetime.strptime(datetime_string, Constants.DATETIME_FORMAT)
    return date_time.strftime(date_time_format)


def response(status, data=None):
    if data:
        return app.response_class(response=json.dumps(data), status=status, mimetype='application/json')
    return app.response_class(status=status, mimetype='application/json')


@app.route('/handshake')
def handshake():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'key' in params:
        print('received private key: {}'.format(params['key']))
        Constants.secret_key = secret_key(int(params['key']), Constants.private_key)
        print('secret key: {}'.format(Constants.secret_key))

        return response(200, str(public_key_from_private_key(Constants.private_key)))

    return response(401, 'Expects public key.')


@app.route('/exchange_open')
def exchange_open():
    # Return "True" if exchange is open and "False" if it is closed.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], paper=True)
    return response(200, str(exchange.is_exchange_open()))


@app.route('/tick_capture_job')
def tick_capture_job():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(name=Job.DB_NAME)
    start_time_string, job_id = db.get_one_row('jobs', 'script="{}"'.format('tick_capture'), 'max(start_time), id')
    data = {
        'start_time': format_datetime_str(start_time_string),
        'job_id': job_id
    }

    return response(200, data)


@app.route('/market_data')
def market_data():
    # Returns a time series data for a ticker with provided symbol and within a given datetime range.
    # Cannot return as dict as element order is not guaranteed.
    # TODO Offer multiple tickers using data loader.

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
    data_loader = MarketDataLoader()
    data_loader.load_tickers(symbol, before, after)
    if MarketDataLoader.TICKER in data_loader.data:
        data = data_loader.data[MarketDataLoader.TICKER][symbol]
        data = [[d[0].strftime(Constants.PP_TIME_FORMAT), float_to_str(d[1]), int_to_str(int(d[2]))] for d in data]
        return response(200, data)
    else:
        return response(401, 'Market data not available.')


@app.route('/strategies')
def strategies():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database()

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        strategies_rows = db.query_table('strategies', 'name="{}"'.format(params['id']))
    else:
        strategies_rows = db.query_table('strategies')

    crumb_timestamp = BreadCrumbsDataLoader.TIMESTAMP
    crumb_type = BreadCrumbsDataLoader.TYPE
    crumb_data = BreadCrumbsDataLoader.DATA

    strategy_table_schema = Constants.configs['tables'][Constants.DB_NAME]['strategies']
    strategies_as_dict = query_result_to_dict(strategies_rows, strategy_table_schema)
    for strategy in strategies_as_dict:
        # Get historical valuations from way points.
        bread_crumbs_data_loader = BreadCrumbsDataLoader()
        bread_crumbs_data_loader.load_bread_crumbs_time_series(strategy['name'])
        bread_crumbs = bread_crumbs_data_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][strategy['name']]
        valuation_type = BreadCrumbs.VALUATION
        valuations = [(b[crumb_timestamp], float(b[crumb_data])) for b in bread_crumbs if b[crumb_type] == valuation_type]

        strategy['historical_valuations'] = [[format_datetime_str(v[0]), v[1]] for v in valuations]
        strategy['pnl'] = float_to_str(float(valuations[-1][1] - valuations[0][1]))

    return response(200, strategies_as_dict)


# TODO, This is disgusting
@app.route('/strategy_bread_crumbs')
def strategy_bread_crumbs():
    # Returns data as time series.
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        bread_crumb_data_loader = BreadCrumbsDataLoader()
        bread_crumb_data_loader.load_bread_crumbs_time_series(params['id'])
        if BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES in bread_crumb_data_loader.data:
            # Extract data from data loader.
            bread_crumbs = bread_crumb_data_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][params['id']]

            # Group data by runtime.
            grouped_by_run_time_series = group_bread_crumbs_by_run_time(bread_crumbs, replace_blanks=True)

            # Sort by datetime descending.
            bread_crumb_time_series = sorted(list(grouped_by_run_time_series), key=lambda x: x[0], reverse=True)

            # Format datetime strings.
            for run_time in grouped_by_run_time_series:
                run_time[0] = format_datetime_str(run_time[0], date_time_format=Constants.PP_DATETIME_FORMAT)

            # Return result.
            return response(200, bread_crumb_time_series)
        else:
            return response(401, 'No way point data found.')

    return response(401, 'Strategy id required.')


@app.route('/portfolio')
def portfolio():
    # Returns portfolio row as dictionary for provided portfolio id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database()

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        # Get portfolio data.
        portfolio_row = db.get_one_row('portfolios', 'id="{0}"'.format(params['id']))
        if portfolio_row is None:
            return response(400, 'Portfolio does not exist.')

        # Initiate portfolio object.
        portfolio_obj = Portfolio(params['id'], db)

        # Package portfolio data.
        data = {
            Portfolio.ID: portfolio_obj.id,
            Portfolio.CASH: float_to_str(portfolio_obj.cash),
            Portfolio.VALUE: float_to_str(portfolio_obj.valuate()),
        }

        return response(200, data)

    return response(401, 'Portfolio id required.')


@app.route('/portfolio/assets')
def assets():
    # Returns asset rows as a dictionary for a given portfolio id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        db = Database()
        exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], paper=True)
        portfolio_obj = Portfolio(params['id'], db)
        portfolio_obj.sync_with_exchange(exchange)
        for asset in portfolio_obj.assets:
            exposure_as_string = float_to_str(portfolio_obj.assets[asset][Portfolio.EXPOSURE])
            portfolio_obj.assets[asset][Portfolio.EXPOSURE] = exposure_as_string
        return response(200, portfolio_obj.assets)

    return response(401, 'Portfolio id required.')


@app.route('/job')
def job():
    # Returns job row as dictionary for provided job id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        job_obj = Job(job_id=params['id'])

        data = {
            'name': job_obj.name,
            'script': job_obj.script,
            'log_path': job_obj.log_path,
            'start_time': format_datetime_str(job_obj.start_time),
            'elapsed_time': job_obj.elapsed_time,
            'finish_state': job_obj.STATUS_MAP[int(job_obj.finish_state)],
            'version': '{0}{1}'.format(job_obj.version, ' (NEW)' if is_script_new(job_obj.script) else ''),
            'phase_name': job_obj.phase_name
        }

        return response(200, data)

    return response(401, 'Job id required.')


@app.route('/job/log')
def log():
    # Returns log text for provided job id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database(name=Job.DB_NAME)

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        job_row = db.get_one_row('jobs', 'id="{0}"'.format(params['id']))
        if job_row is None:
            return response(401, 'Job doesnt exist.')
        job_dict = query_result_to_dict([job_row], Constants.configs['tables'][Job.DB_NAME]['jobs'])[0]
        with open(job_dict['log_path'], 'r') as file:
            data = file.read().replace('\n', '<br>')
        return response(200, data)

    return response(401, 'Job id required.')


@app.route('/run/regression')
def run_regression():
    # Returns log text for provided job id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        script_runner = ScriptRunner()
        arguments = {
            'xml_file': os.path.join(Constants.root_path, Constants.environment, 'strategies',
                                     '{}.xml'.format(params['id'])),
            strategy_regression_tester.START_DATE: '20200601',
            strategy_regression_tester.END_DATE: '20200612',
            strategy_regression_tester.TIMES: '1600,1730,1800,1830,1900,1930,2000,2030',
            strategy_regression_tester.EXPORT: True,
        }
        script_runner.run_asynchronously(ScriptRunner.REGRESSION_TESTER, 'regression', arguments)
        return response(200, 0)

    return response(401, 'Job id required.')


@app.route('/run/dry_run')
def run_dry_run():
    # Returns log text for provided job id.

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        script_runner = ScriptRunner()
        arguments = {
            'xml_file': os.path.join(Constants.root_path, Constants.environment,
                                     'strategies', '{}.xml'.format(params['id'])),
            strategy_executor.SUPPRESS_TRADES: True,
            strategy_executor.EXPORT_CSV: True
        }
        script_runner.run_asynchronously(ScriptRunner.STRATEGY_EXECUTOR, 'dry_run', arguments)
        return response(200, 0)

    return response(401, 'Job id required.')


if __name__ == '__main__':
    # Setup parse options, imitate global constants and logs.
    Constants.parse_arguments(configs_file_name=Constants.APP_NAME)

    # Temp
    Constants.secret_key = None
    Constants.private_key = 420

    # Start server.
    app.run('0.0.0.0')
