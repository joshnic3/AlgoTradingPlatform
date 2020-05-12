import datetime
import multiprocessing
import optparse
import os
import sys
import threading
import time
import xml.etree.ElementTree as et
from multiprocessing.pool import ThreadPool

from library.bootstrap import Constants
from library.interfaces.market_data import TickerDataSource
from library.interfaces.twitter import TwitterDataSource
from library.interfaces.sql_database import Database, generate_unique_id
from library.utilities.file import parse_configs_file, get_xml_element_attributes, get_xml_element_attribute
from library.utilities.job import Job
from library.utilities.log import get_log_file_path, setup_log, log_configs

NS = {
    'XML_DATA_LABEL': 'data',
    'XML_TWAP_LABEL': 'data/twap',
    'XML_TWITTER_LABEL': 'data/twitter',
    'XML_TWAP_ATTRIBUTES': {
        'NAME': 'name',
        'TOLERANCE': 'tolerance',
        'COUNT': 'count',
        'INTERVAL': 'interval'
    },
    'XML_TICKER_LABEL': 'ticker',
    'XML_TICKER_ATTRIBUTES': {
        'SYMBOL': 'symbol'
    }
}


class DataWarning:

    def __init__(self, symbol, data_group, warning_type):
        _types = ['STALE_TICKER']
        self.symbol = symbol
        self.data_group = data_group
        warning_type = warning_type.upper()
        if warning_type in _types:
            self.type = warning_type
        else:
            raise Exception('Invalid data warning type "{0}". Valid types: {1}'.format(warning_type, _types))

    def __str__(self):
        return '{0}: "{1}"'.format(self.type, self.symbol)

    def save_to_db(self, db):
        warning_id = generate_unique_id(self.symbol)
        values = [warning_id, self.data_group, self.type, self.symbol]
        db.insert_row('data_warnings', values)


class TWAP:

    def __init__(self, symbol, data_group):
        self.symbol = symbol
        self.ticks = []
        self.data_warnings = []
        self._data_group = data_group
        self.value = None

    def __str__(self):
        return '[Symbol: {0}, Value: {1}]'.format(self.symbol, self.value)

    def _is_stale(self, value):
        if not self.ticks:
            return False
        if value == self.ticks[-1][1]:
            return True
        return False

    def add_tick(self, date_time, value):
        self.ticks.append((date_time, float(value)))
        if self._is_stale(float(value)) and len(self.ticks) > 1:
            self.data_warnings.append(DataWarning(self.symbol, self._data_group, 'STALE_TICKER'))
        else:
            return None

    def calculate_twap(self):
        self.value = 0
        if self.ticks:
            self.value = sum([float(t[1]) for t in self.ticks]) / len(self.ticks)
        return self.value

    def save_to_db(self, db):
        # TODO save to market data table.
        twap_id = generate_unique_id(self.symbol)
        times = [t[0] for t in self.ticks]
        start_time = min(times)
        end_time = max(times)
        data_warning_flag = len(self.data_warnings) > 0
        values = [twap_id, start_time.strftime('%Y%m%d%H%M%S'), end_time.strftime('%Y%m%d%H%M%S'), self.symbol,
                  self.value, data_warning_flag]
        db.insert_row('twaps', values)
        [d.save_to_db(db) for d in self.data_warnings]

    def log_twap(self, log):
        log.info(self.__str__())


class TWAPGenerator:

    def __init__(self, tickers, data_group, tolerance=None):
        self.twaps = [TWAP(s, data_group) for s in tickers]
        self.tolerance = tolerance

    def __str__(self):
        return self.source

    def get_ticker_values(self, data_source):
        now = datetime.datetime.now()
        symbols = [t.symbol for t in self.twaps]
        results = data_source.request_tickers(symbols)
        for twap in self.twaps:
            twap.add_tick(now, results[twap.symbol])

    def calculate(self):
        for twap in self.twaps:
            twap.calculate_twap()
            if self.tolerance:
                twap.value = round(twap.value, self.tolerance)

    def save_to_db(self, db):
        for twap in self.twaps:
            twap.save_to_db(db)


class TwapDataLoader:

    def __init__(self, requirements_xml, data_source):
        self._data_source = data_source
        self.requirements_xml = requirements_xml
        self.required_data = []
        self.data_objects = []
        self.results = []

    @staticmethod
    def _tick_recorder(data_source, twap_generator, count, interval):
        completed = 0
        multiplier = 00 if Constants.configs['environment'] == 'dev' else 60
        while completed < int(count):
            twap_generator.get_ticker_values(data_source)
            completed += 1
            time.sleep(int(interval) * multiplier)
        thread_id = threading.get_ident()
        symbols = ', '.join([t.symbol for t in twap_generator.twaps])
        Constants.log.info('TWAP data loader: Tick recorder finished. pid: {0}, symbols: {1} '.format(thread_id, symbols))
        return twap_generator

    @staticmethod
    def _log(msg):
        Constants.log.info('TWAP data loader: {0}'.format(str(msg)))

    def parse_required_data(self):
        # Generate twap data loader groups.
        for twap in self.requirements_xml:
            # Extract attributes.
            group_attributes = list(NS['XML_TWAP_ATTRIBUTES'].keys())
            attributes = get_xml_element_attributes(twap, require=group_attributes)

            # Extract list of symbols.
            ticker_symbols = [get_xml_element_attribute(t, NS['XML_TICKER_ATTRIBUTES']['SYMBOL'])
                              for t in twap.findall(NS['XML_TICKER_LABEL'])]
            tolerance = int(attributes[NS['XML_TWAP_ATTRIBUTES']['TOLERANCE']])
            self.required_data.append((
                TWAPGenerator(ticker_symbols, attributes[NS['XML_TWAP_ATTRIBUTES']['NAME']], tolerance=tolerance),
                int(attributes[NS['XML_TWAP_ATTRIBUTES']['COUNT']]),
                int(attributes[NS['XML_TWAP_ATTRIBUTES']['INTERVAL']]))
            )

    def record_data(self):
        self._log('Starting {0} tick recorder(s).'.format(len(self.required_data)))
        # Prepare multiprocessing pool.
        cpu_count = multiprocessing.cpu_count()
        pool = ThreadPool(cpu_count)

        # Do work asynchronously.
        workers = [pool.apply_async(self._tick_recorder, args=(self._data_source, *g,)) for g in self.required_data]
        pool.close()
        pool.join()

        # Return data loader objects.
        self.data_objects = [w.get() for w in workers]

    def process(self, db=None):
        self._log('Processing and saving TWAPs.')
        # Record twaps to database.
        for twap_generator in self.data_objects:
            twap_generator.calculate()
            if db:
                twap_generator.save_to_db(db)
            self.results += twap_generator.twaps
        return self.results


class TwitterDataLoader:

    def __init__(self, requirements_xml, data_source):
        self._data_source = data_source
        self.requirements_xml = requirements_xml
        self.required_data = []
        self.data_objects = []
        self.results = []

    #     will have data_warning and tweets list of objects,
    # will save to tweets table in market data db

    @staticmethod
    def _log(msg):
        Constants.log.info('Twitter data loader: {0}'.format(str(msg)))

    def parse_required_data(self):
        for mention in self.requirements_xml:
            self.required_data.append(mention)

    def record_data(self):
        self.data_objects = None

    def process(self, db=None):
        self.results = None
        return self.results


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-x', '--xml_file', dest="xml_file")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)
    parser.add_option('--debug', dest="debug", action="store_true", default=False)
    parser.add_option('--dry_run', action="store_true", default=False)

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "xml_file": options.xml_file,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "debug": options.debug,
        "dry_run": options.dry_run
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
    job = Job(Constants.configs)
    job.log()

    # Parse data requirements from XML file supplied in parameters.
    job.update_status('PROCESSING_DATA_REQUIREMENTS')
    data_requirements = et.parse(Constants.configs['xml_file']).getroot()

    # Initiate data loader(s).
    data_loaders = []

    # Load any Twitter requirements.
    if data_requirements.findall(Constants.xml.twitter):
        twitter_data_loader = TwitterDataLoader(data_requirements.findall(Constants.xml.twitter), TwitterDataSource())
        twitter_data_loader.parse_required_data()
        data_loaders.append(twitter_data_loader)

    # Load any TWAP requirements, loading TWAPs last in-case there are multiple loaders as they are holding.
    if data_requirements.findall(Constants.xml.twap):
        twap_data_loader = TwapDataLoader(data_requirements.findall(Constants.xml.twap), TickerDataSource())
        twap_data_loader.parse_required_data()
        data_loaders.append(twap_data_loader)

    # Record then process any data requirements.
    data_warnings = 0
    no_of_required_data_sets = sum([len(d.required_data) for d in data_loaders if d.required_data])
    if no_of_required_data_sets:
        Constants.log.info('Read {0} set(s) of required data.'.format(no_of_required_data_sets))

        # Record data, these can be holding.
        job.update_status('RECORDING_DATA')
        [data_loader.record_data() for data_loader in data_loaders]

        # Process results
        job.update_status('PROCESSING_RESULTS')
        for data_loader in data_loaders:
            results = data_loader.process(db)
            if results:
                Constants.log.info('{0}'.format([str(t) for t in results]))
                data_warnings += len([t.data_warnings for t in results])
    else:
        # Early termination because there is no required data.
        job.terminate(condition='NO_REQUIRED_DATA')
        return 0

    if data_warnings:
        Constants.log.info('Total data warning(s): {0}'.format(data_warnings))

    # Finish job.
    status = 2 if data_warnings else 0
    job.finished(status=status)
    return status


if __name__ == "__main__":
    sys.exit(main())
