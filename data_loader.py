import sys
import os
import datetime
import time
import multiprocessing
import optparse
from multiprocessing.pool import ThreadPool
from itertools import chain

from library.db_interface import Database
from library.file_utils import read_json_file, parse_configs_file
from library.log_utils import get_log_file_path, setup_log, log_configs, log_hr, log_end_status
from library.data_source_utils import get_data_source_configs, DataSource

configs = {}


class TickerDataSource(DataSource):

    def __init__(self, name):
        DataSource.__init__(self, name, configs['db_root_path'], configs['environment'])

    def _extract_data(self, result):
        # Takes [{symbol_key: symbol}, {value_key, value}] and returns {symbol: value}.
        return dict(zip([r[self._configs['symbol_key']] for r in result], [r[self._configs['value_key']] for r in result]))

    def request_tickers(self, symbols):
        symbols_str = self._configs['delimiter'].join(symbols) if len(symbols) > 1 else symbols[0]
        wildcard = {self._configs['wildcards']['symbols']: symbols_str}
        url = self._prepare_api_call_url(self._configs['request_template'], wildcard)
        result = self._call_api_return_as_dict(url)
        return self._extract_data(result['companiesPriceList'])


class TWAP:

    def __init__(self, symbol):
        self.symbol = symbol
        self.ticks = []
        self.twap = None

    def __str__(self):
        formatted_ticks = [(t[0], t[1]) for t in self.ticks]
        values = [t[1] for t in self.ticks]
        times = [t[0] for t in self.ticks]
        spread = max(values) - min(values)
        return '[Symbol: {0}, TWAP: {1}, Start Time: {2}, Ticks: {3}, Spread: {4}]'.format(self.symbol,
                                                                                          self.twap,
                                                                                          min(times).strftime(
                                                                                              '%H:%M.%S'),
                                                                                          len(formatted_ticks), spread)

    def _is_stale(self, value):
        if not self.ticks or float(value) != self.ticks[-1][1]:
            return False
        return True

    def add_tick(self, value):
        now = datetime.datetime.now()
        if self._is_stale(value):
            self.ticks.append((now, float(value)))
            return self.symbol, 'STALE_TICKER'
        self.ticks.append((now, float(value)))
        return None

    def calculate_twap(self):
        self.twap = 0
        if self.ticks:
            self.twap = sum([float(t[1]) for t in self.ticks])/len(self.ticks)
        return self.twap

    def save_to_db(self, db):
        times = [t[0] for t in self.ticks]
        values = [0, min(times).strftime('%Y%m%d%H%M%S'), max(times).strftime('%Y%m%d%H%M%S'), self.symbol, self.twap]
        db.insert_row('twaps', values)

    def log_twap(self, log):
        log.info(self.__str__())


class TWAPDataLoader:

    def __init__(self, source, tickers, db):
        self.source = TickerDataSource(source)
        self.twaps = [TWAP(t[1]) for t in tickers]
        self.data_warnings = []
        self._db = db

    def __str__(self):
        return self.source

    def get_ticker_values(self):
        symbols = [t.symbol for t in self.twaps]
        results = self.source.request_tickers(symbols)
        for twap in self.twaps:
            data_warnings = twap.add_tick(results[twap.symbol])
            if data_warnings:
                self.data_warnings.append(data_warnings)

    def calculate_twaps(self):
        for twap in self.twaps:
            twap.calculate_twap()

    def save_twaps_to_db(self, log=None):
        for twap in self.twaps:
            twap.save_to_db(self._db)
            if log:
                log.info(twap.__str__())


def worker_func(log, worker_id, group, required_tickers, db):
    # Create a new worker process for each group.
    interval, count, source = group
    data_loader = TWAPDataLoader(source, required_tickers, db)
    completed = 0
    multiplier = 60
    while completed < int(count):
        data_loader.get_ticker_values()
        completed += 1
        time.sleep(int(interval) * multiplier)
    data_loader.calculate_twaps()
    log.info('Loader {0} completed {1} [Source: {2}, Tickers: {3}, DataWarnings: {4}]'.format(worker_id,
                                                                                  'with WARNINGS.' if data_loader.data_warnings else 'SUCCESSFULLY!',
                                                                                  data_loader.source,
                                                                                  len(data_loader.twaps),
                                                                                  len(data_loader.data_warnings)))
    return data_loader


def parse_cmdline_args():
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-c', '--config_file', dest="config_file")
    parser.add_option('--dry_run', action="store_true", default=False)
    options, args = parser.parse_args()
    cmdline_args = {
        "environment": options.environment.lower(),
        "config_file": options.config_file,
        "dry_run": options.dry_run
    }
    return cmdline_args


def required_tickers_for_group(db, group):
    condition = 'interval="{0}" AND count="{1}" AND source="{2}"'.format(*group)
    return db.query_table('twap_required_tickers', condition)


def main():
    global configs
    cmdline_args = parse_cmdline_args()
    configs = parse_configs_file(cmdline_args)

    # Setup logging.
    script_name = str(os.path.basename(sys.argv[0]))
    log_path = get_log_file_path(configs['root_path'], script_name.split('.')[0])
    log = setup_log(log_path, True if configs['environment'].lower() == 'dev' else False)
    log_configs(cmdline_args, log)
    if configs != cmdline_args:
        log.info('Imported {0} additional config items from script config file'.format(len(configs)-len(cmdline_args)))

    # Setup db connection.
    db = Database(configs['db_root_path'], 'algo_trading_platform', True, configs['environment'])
    db.log_status(log)

    # Prepare multiprocessing pool.
    cpu_count = multiprocessing.cpu_count()
    pool = ThreadPool(cpu_count)

    # Count required tickers.
    required_tickers = db.execute_sql('SELECT DISTINCT id FROM twap_required_tickers;')
    log.info('Found {0} required ticker(s)'.format(len(required_tickers)))

    # Get TWAPS.
    groups = [r for r in db.execute_sql('SELECT DISTINCT interval, count, source FROM twap_required_tickers;')]
    log.info('Grouped into {0} data loader(s)'.format(len(groups)))
    log_hr(log)
    workers = [pool.apply_async(worker_func, args=(log, groups.index(g), g, required_tickers_for_group(db, g), db, ))
               for g in groups]
    pool.close()
    pool.join()

    # Read results.
    data_loaders = [w.get() for w in workers]
    retrieved_twaps = [i for i in chain.from_iterable([dl.twaps for dl in data_loaders])]
    data_warnings = [i for i in chain.from_iterable([dl.data_warnings for dl in data_loaders])]

    # Log results.
    log_hr(log)
    log.info('Retrieved {0} TWAPS'.format(len(retrieved_twaps)))
    if data_warnings:
        unique_data_warnings = list(set(data_warnings))
        log.info('{0} Data Warnings:'.format(len(data_warnings)))
        for udw in unique_data_warnings:
            log.info('[Symbol: {0}, Warning: {1}, Occurrences: {2}]'.format(udw[0], udw[1], data_warnings.count(udw)))

    # Save results to database.
    if not configs['dry_run']:
        log.info('Saving {0} TWAPS to database:'.format(len(retrieved_twaps)))
        for data_loader in data_loaders:
            data_loader.save_twaps_to_db(log)

    else:
        log.info('This is a dry run so TWAPS where not saved')

    # Log summary.
    log_hr(log)
    success = len(required_tickers) == len(retrieved_twaps)
    if success:
        status = 2 if data_warnings else 0
    else:
        status = 1
    log_end_status(log, script_name, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
