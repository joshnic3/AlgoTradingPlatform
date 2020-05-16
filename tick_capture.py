import optparse
import os
import sys
import xml.etree.ElementTree as et

from library.bootstrap import Constants
from library.interfaces.sql_database import Database
from library.interfaces.market_data import TickerDataSource
from library.utilities.file import parse_configs_file, get_xml_element_attribute
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs
import datetime


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-x', '--xml_file', dest="xml_file")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)
    parser.add_option('--debug', dest="debug", action="store_true", default=False)

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "xml_file": options.xml_file,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "debug": options.debug
    })


def main():
    # Setup configs.
    Constants.configs = parse_cmdline_args('algo_trading_platform')

    # Setup logging.
    log_path = get_log_file_path(Constants.configs['logs_root_path'], Constants.configs['job_name'])
    Constants.log = setup_log(log_path, True if Constants.configs['environment'] == 'dev' else False)
    log_configs(Constants, Constants.log)

    # Setup db connection.
    db = Database(Constants.configs['db_root_path'], 'market_data', Constants.configs['environment'])
    db.log()

    # Initiate Job.
    job = Job()
    job.log()

    # Load required symbols.
    job.update_phase('loading requirements')
    ticks = []
    for tick_requirement in et.parse(Constants.configs['xml_file']).getroot():
        symbol = get_xml_element_attribute(tick_requirement, 'symbol', required=True)
        stale_tick_limit = get_xml_element_attribute(tick_requirement, 'stale_tick_limit')
        if stale_tick_limit:
            ticks.append({'symbol': symbol.upper(), 'stale_tick_limit': int(stale_tick_limit)})
        else:
            ticks.append({'symbol': symbol.upper()})
    Constants.log.info('Loaded {0} required tickers.'.format(len(ticks)))

    # Request data.
    job.update_phase('requesting data')
    ticker_data_source = TickerDataSource()
    data_source_values = ticker_data_source.request_tickers([t['symbol'] for t in ticks])
    Constants.log.info('Recorded {0} ticks.'.format(len(data_source_values)))

    # Process ticks
    job.update_phase('processing data')
    for tick in ticks:
        if tick['symbol'] in data_source_values:
            tick['value'] = data_source_values[tick['symbol']]

        #  Test this is working.
        if 'stale_tick_limit' in tick:
            previous_ticks = db.query_table('ticks', 'symbol="{0}"'.format(tick['symbol']))[:tick['stale_tick_limit']+1]
            if previous_ticks:
                tick_value_to_compare = float(previous_ticks[0][3])
                if tick_value_to_compare == tick['value']:
                    db.insert_row('data_warnings', [0, 'tick', 'stale', tick['symbol']])
                    print('stale data')

        # Save tick to database.
        now = datetime.datetime.strftime(datetime.datetime.now(), Constants.date_time_format)
        db.insert_row('ticks', [0, now, tick['symbol'], tick['value']])

    job.finished(status=0)


if __name__ == "__main__":
    sys.exit(main())
