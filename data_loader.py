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
from library.log_utils import get_log_file_path, setup_log, log_configs, log_seperator, log_end_status
from library.data_source_utils import get_data_source_configs, DataSource

configs = {}


# TODO add error handling and read from db, should be as generic as possible.
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
        spread = max(values) - min(values)
        return '[Symbol: {0}, TWAP: {1}, Ticks:{2}, Spread: {3}]'.format(self.symbol, self.twap, len(formatted_ticks), spread)

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

    def save_twaps_to_db(self):
        for twap in self.twaps:
            twap.save_to_db(self._db)


def worker_func(log, worker_id, group, required_tickers, db):
    # Create a new worker process for each group.
    data_loader = TWAPDataLoader(group[2], required_tickers, db)
    completed = 0
    while completed < int(group[1]):
        data_loader.get_ticker_values()
        completed += 1
        # time.sleep(int(group[0]) * 60)
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


def main():
    status = -1
    global configs
    cmdline_args = parse_cmdline_args()
    configs = parse_configs_file(cmdline_args)

    # Setup logging.
    script_name = str(os.path.basename(sys.argv[0]))
    log_path = get_log_file_path(configs['root_path'], script_name.split('.')[0])
    log = setup_log(log_path)
    log_configs(cmdline_args, log)

    # Setup db connection.
    db = Database(configs['db_root_path'], 'algo_trading_platform', True, configs['environment'])
    db.log_status(log)

    # Prepare multiprocessing pool.
    cpu_count = multiprocessing.cpu_count()
    pool = ThreadPool(cpu_count)

    # Count required tickers.
    results = db.execute_sql('SELECT DISTINCT id FROM twap_required_tickers;')
    log.info('Found {0} required ticker(s)'.format(len(results)))

    # Group TWAPS by DISTINCT interval, resolution and source
    results = db.execute_sql('SELECT DISTINCT interval, resolution, source FROM twap_required_tickers;')
    groups = [r for r in results]
    workers = []
    log.info('Grouped into {0} data loader(s)'.format(len(groups)))
    log_seperator(log)
    for group in groups:
        condition = 'interval="{0}" AND resolution="{1}" AND source="{2}"'.format(group[0], group[1], group[2])
        required_tickers = db.query_table('twap_required_tickers', condition)
        workers.append(pool.apply_async(worker_func, args=(log, groups.index(group), group, required_tickers, db, )))
    pool.close()
    pool.join()

    # Read results.
    data_loaders = [w.get() for w in workers]
    twaps = [i for i in chain.from_iterable([dl.twaps for dl in data_loaders])]
    data_warnings = [i for i in chain.from_iterable([dl.data_warnings for dl in data_loaders])]

    # Log results.
    log_seperator(log)
    log.info('Retrieved {0} TWAPS'.format(len(twaps)))
    for twap in twaps:
        twap.log_twap(log)
    if data_warnings:
        unique_data_warnings = list(set(data_warnings))
        log.info('{0} Data Warnings:'.format(len(data_warnings)))
        for udw in unique_data_warnings:
            log.info('[Symbol: {0}, Warning: {1}, Occurrences: {2}]'.format(udw[0], udw[1], data_warnings.count(udw)))

    # Save results to database.
    if not configs['dry_run']:
        for data_loader in data_loaders:
            data_loader.save_twaps_to_db()
        log.info('Saved {0} TWAPS to database'.format(len(twaps)))
    else:
        log.info('This is a dry run so TWAPS where not saved')

    # Log summary.
    log_seperator(log)
    requested = len(db.query_table('twap_required_tickers'))
    successful = len(twaps)
    if requested == successful:
        if data_warnings:
            status = 2
            log_end_status(log, script_name, status)
        else:
            status = 0
            log_end_status(log, script_name, status)
    else:
        status = 1
        log_end_status(log, script_name, status)
    return status


if __name__ == "__main__":
    sys.exit(main())
