import datetime
import json

from flask import Flask, request

from library.bootstrap import Constants
from library.data_loader import MarketDataLoader, BreadCrumbsDataLoader
from library.interfaces.exchange import AlpacaInterface
from library.interfaces.sql_database import Database, query_result_to_dict
from library.strategy.portfolio import Portfolio
from library.strategy.bread_crumbs import BreadCrumb
from library.utilities.job import is_script_new, Job
from library.utilities.authentication import public_key_from_private_key, secret_key
from library.data_loader import DataLoader

app = Flask(__name__)


def float_to_string(value_float):
    return '{:,.2f}'.format(value_float)


def format_datetime_sting(datetime_string):
    if datetime_string is None:
        return None
    date_time = datetime.datetime.strptime(datetime_string, Constants.DATETIME_FORMAT)
    return date_time.strftime(Constants.PP_DATETIME_FORMAT)


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

    exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], simulator=True)
    return response(200, str(exchange.is_exchange_open()))


@app.route('/tick_capture_job')
def tick_capture_job():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database()
    start_time_string, job_id = db.get_one_row('jobs', 'script="{}"'.format('tick_capture'), 'max(start_time), id')
    data = {
        'start_time': format_datetime_sting(start_time_string),
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
        data = [[d[0].strftime(Constants.PP_DATETIME_FORMAT), float_to_string(d[1]), int(d[2])] for d in data]
        return response(200, data)
    else:
        return response(401, 'Market data not available.')


@app.route('/strategies')
def strategies():
    # Returns strategy row as dict, will return for all strategies for for one specified by provided strategy_id

    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in Constants.configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    db = Database()

    strategies_rows = db.query_table('strategies')
    strategy_table_schema = Constants.configs['tables'][Constants.DB_NAME]['strategies']
    strategies_as_dict = query_result_to_dict(strategies_rows, strategy_table_schema)
    for strategy in strategies_as_dict:
        # Get historical valuations from way points.
        way_point_data_loader = BreadCrumbsDataLoader()
        way_point_data_loader.load_bread_crumbs_time_series(strategy['name'])
        if BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES in way_point_data_loader.data:
            data = way_point_data_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][strategy['name']]

            if BreadCrumb.VALUATION in data:
                # Extract historical valuations.
                valuations = [[datetime.datetime.strptime(v[0], Constants.DATETIME_FORMAT), float(v[1])] for v in data[BreadCrumb.VALUATION]]

                # Calculate 24hr pnl.
                now = datetime.datetime.now()
                twenty_four_hrs_ago = now - datetime.timedelta(hours=24)
                twenty_four_hour_valuations = [d[1] for d in valuations if twenty_four_hrs_ago < d[0] < now]

                # Format data.
                if twenty_four_hour_valuations:
                    formatted_pnl = float_to_string(sum(twenty_four_hour_valuations)/len(twenty_four_hour_valuations))
                else:
                    formatted_pnl = '-'
                formatted_valuations = [[v[0].strftime(Constants.PP_DATETIME_FORMAT), v[1]] for v in valuations]
            else:
                formatted_pnl = 0
                formatted_valuations = None

            strategy['historical_valuations'] = formatted_valuations
            strategy['pnl'] = formatted_pnl

    return response(200, strategies_as_dict)


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
            data = bread_crumb_data_loader.data[BreadCrumbsDataLoader.BREAD_CRUMBS_TIME_SERIES][params['id']]

            # Group data by timestamp.
            time_series = {}
            for data_type in data:
                for element in data[data_type]:
                    data_point = float_to_string(float(element[1])) if data_type in DataLoader.VALUE_DATA_TYPES else element[1]
                    if element[0] not in time_series:
                        time_series[element[0]] = {data_type: data_point}
                    else:
                        time_series[element[0]][data_type] = data_point

            # Sort and format.
            # TODO, sort by time desc.
            time_series = [[format_datetime_sting(r), time_series[r]['signal'], time_series[r]['trade'], time_series[r]['valuation']] for r in time_series]
            return response(200, time_series)
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
            Portfolio.CASH: float_to_string(portfolio_obj.cash),
            Portfolio.VALUE: float_to_string(portfolio_obj.valuate()),
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
        exchange = AlpacaInterface(Constants.configs['API_ID'], Constants.configs['API_SECRET_KEY'], simulator=True)
        portfolio_obj = Portfolio(params['id'], db)
        portfolio_obj.sync_with_exchange(exchange)
        for asset in portfolio_obj.assets:
            exposure_as_string = float_to_string(portfolio_obj.assets[asset][Portfolio.EXPOSURE])
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
            'start_time': format_datetime_sting(job_obj.start_time),
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
    db = Database()

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    if 'id' in params:
        job_row = db.get_one_row('jobs', 'id="{0}"'.format(params['id']))
        if job_row is None:
            return response(401, 'Job doesnt exist.')
        job_dict = query_result_to_dict([job_row], Constants.configs['tables'][Constants.DB_NAME]['jobs'])[0]
        with open(job_dict['log_path'], 'r') as file:
            data = file.read().replace('\n', '<br>')
        return response(200, data)

    return response(401, 'Job id required.')


if __name__ == '__main__':
    # Setup parse options, imitate global constants and logs.
    Constants.parse_arguments(configs_file_name=Constants.APP_NAME)

    # Temp
    Constants.secret_key = None
    Constants.private_key = 420

    # Start server.
    app.run('0.0.0.0')
