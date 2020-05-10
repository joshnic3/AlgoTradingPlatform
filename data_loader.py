import datetime
import multiprocessing
import optparse
import os
import sys
import time
import xml.etree.ElementTree as et
from multiprocessing.pool import ThreadPool
import threading

import library.bootstrap as globals
from library.data_source_interface import TickerDataSource
from library.database_interface import Database, generate_unique_id
from library.utils.file import parse_configs_file, get_xml_element_attributes, get_xml_element_attribute
from library.utils.job import Job
from library.utils.log import get_log_file_path, setup_log, log_configs

NS = {
    'XML_TWAP_LABEL': 'data/twap',
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


class DataLoader:

    def __init__(self, data_source):
        self._data_source = data_source

    @staticmethod
    def _tick_recorder(data_source, twap_generator, count, interval):
        completed = 0
        multiplier = 0 if globals.configs['environment'] == 'dev' else 60
        while completed < int(count):
            twap_generator.get_ticker_values(data_source)
            completed += 1
            time.sleep(int(interval) * multiplier)
        thread_id = threading.get_ident()
        symbols = ', '.join([t.symbol for t in twap_generator.twaps])
        globals.log.info('Tick recorder finished. pid: {0}, symbols: {1} '.format(thread_id, symbols))
        return twap_generator

    @staticmethod
    def parse_required_data(xml_path):
        # Get XML root.
        root = et.parse(xml_path).getroot()

        # Generate twap data loader groups.
        twap_generator_groups = []
        for twap in root.findall(NS['XML_TWAP_LABEL']):
            # Extract attributes.
            group_attributes = list(NS['XML_TWAP_ATTRIBUTES'].keys())
            attributes = get_xml_element_attributes(twap, require=group_attributes)

            # Extract list of symbols.
            ticker_symbols = [get_xml_element_attribute(t, NS['XML_TICKER_ATTRIBUTES']['SYMBOL'])
                              for t in twap.findall(NS['XML_TICKER_LABEL'])]
            tolerance = int(attributes[NS['XML_TWAP_ATTRIBUTES']['TOLERANCE']])
            twap_generator_groups.append((
                TWAPGenerator(ticker_symbols, attributes[NS['XML_TWAP_ATTRIBUTES']['NAME']], tolerance=tolerance),
                int(attributes[NS['XML_TWAP_ATTRIBUTES']['COUNT']]),
                int(attributes[NS['XML_TWAP_ATTRIBUTES']['INTERVAL']]))
            )
        return twap_generator_groups

    def record_ticks(self, twap_generator_groups):
        # Prepare multiprocessing pool.
        cpu_count = multiprocessing.cpu_count()
        pool = ThreadPool(cpu_count)

        # Do work asynchronously.
        workers = [pool.apply_async(self._tick_recorder, args=(self._data_source, *g,)) for g in twap_generator_groups]
        pool.close()
        pool.join()

        # Return data loader objects.
        twap_generator_objects = [w.get() for w in workers]
        return twap_generator_objects

    @staticmethod
    def process(twap_generator_objects, db=None):
        twaps = []
        # Record twaps to database.
        for twap_generator in twap_generator_objects:
            twap_generator.calculate()
            if db:
                twap_generator.save_to_db(db)
            twaps += twap_generator.twaps
        return twaps


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
    globals.configs = parse_cmdline_args('algo_trading_platform')

    # Setup logging.
    log_path = get_log_file_path(globals.configs['logs_root_path'], globals.configs['job_name'])
    globals.log = setup_log(log_path, True if globals.configs['environment'] == 'dev' else False)
    log_configs(globals, globals.log)

    # Setup db connection.
    db = Database(globals.configs['db_root_path'], 'algo_trading_platform', globals.configs['environment'])
    db.log()

    # Initiate Job.
    job = Job(globals.configs, db)
    job.log()

    # Initialise data loader.
    data_loader = DataLoader(TickerDataSource(db, 'FML'))

    # Parse data requirements from XML file supplied in parameters.
    job.update_status('PROCESSING_DATA_REQUIREMENTS')
    globals.log.info('Reading in data requirements.')
    twap_generator_groups = data_loader.parse_required_data(globals.configs['xml_file'])

    # Record ticks.
    job.update_status('RECORDING_TICKS')
    globals.log.info('Starting {0} tick recorder(s).'.format(len(twap_generator_groups)))
    twap_generator_objects = data_loader.record_ticks(twap_generator_groups)

    # Process results.
    job.update_status('CALCULATING_TWAPS')
    globals.log.info('Processing and saving TWAPs.')
    twaps = data_loader.process(twap_generator_objects, db)

    # Pretty print to log.
    data_warnings = 0
    for twap in twaps:
        globals.log.info('{0}'.format(str(twap)))
        if twap.data_warnings:
            data_warnings += len(twap.data_warnings)

    if data_warnings:
        globals.log.info('Total data warning(s): {0}'.format(data_warnings))

    # Finish job.
    status = 2 if data_warnings else 0
    job.finished(status=status)


if __name__ == "__main__":
    sys.exit(main())
