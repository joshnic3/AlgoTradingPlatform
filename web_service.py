import optparse
import json
import os
import sys

from flask import Flask, request

from library.db_utils import Database, query_to_dict
from library.file_utils import parse_configs_file

app = Flask(__name__)


def response(status, data=None):
    if data:
        return app.response_class(response=json.dumps(data), status=status, mimetype='application/json')
    return app.response_class(status=status, mimetype='application/json')


@app.route('/twaps')
def twaps():
    # Authenticate.
    client_ip = request.environ.get('HTTP_X_REAL_IP', request.remote_addr)
    if client_ip not in configs['authorised_ip_address']:
        return response(401, 'Client is not authorised.')

    # Initiate database connection.
    database_name = 'algo_trading_platform'
    table_name = 'twaps'
    db = Database(configs['db_root_path'], database_name, configs['environment'])

    # Extract any parameters from url.
    params = {x: request.args[x] for x in request.args if x is not None}

    # Query TWAP table.
    if 'symbol' in params:
        if 'before' in params and 'after' in params:
            condition = 'symbol="{0}" AND start_time>"{1}" AND end_time<"{2}"'.format(params['symbol'], params['after'], params['before'])
        else:
            condition = 'symbol="{0}"'.format(params['symbol'])

        result = db.query_table(table_name, condition)
    else:
        result = db.query_table(table_name)

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
    database_name = 'algo_trading_platform'
    table_name = 'twaps'
    db = Database(configs['db_root_path'], database_name, configs['environment'])

    # Query TWAP table.
    result = db.execute_sql('SELECT DISTINCT symbol FROM {0};'.format(table_name))
    return response(200, [s[0] for s in result])


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
