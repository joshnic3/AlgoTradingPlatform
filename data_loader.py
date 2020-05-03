import datetime
import multiprocessing
import optparse
import os
import sys
import time
from itertools import chain
from multiprocessing.pool import ThreadPool

import library.bootstrap as globals
from library.data_source_interface import TickerDataSource
from library.database_interface import Database, generate_unique_id
from library.utils.file import parse_configs_file
from library.utils.job import Job
from library.utils.log import get_log_file_path, setup_log, log_configs, log_hr


class TWAPDataLoader:

    def __init__(self, source, tickers, db):
        self.source = TickerDataSource(db, source)
        self.twaps = [TWAP(t[0]) for t in tickers]
        self.data_warnings = []
        self._db = db

    def __str__(self):
        return self.source

    def get_ticker_values(self):
        now = datetime.datetime.now()
        symbols = [t.symbol for t in self.twaps]
        results = self.source.request_tickers(symbols)
        for twap in self.twaps:
            data_warnings = twap.add_tick(now, results[twap.symbol])
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
        if not self.ticks:
            return False
        if value == self.ticks[-1][1]:
            return True
        return False

    def add_tick(self, date_time, value):
        self.ticks.append((date_time, float(value)))
        if self._is_stale(float(value)):
            return self.symbol, 'STALE_TICKER'
        return None

    def calculate_twap(self):
        self.twap = 0
        if self.ticks:
            self.twap = sum([float(t[1]) for t in self.ticks])/len(self.ticks)
        return self.twap

    def save_to_db(self, db):
        twap_id = generate_unique_id(self.symbol)
        times = [t[0] for t in self.ticks]
        start_time = min(times)
        # This is the time of the last tick, so will be interval * count minutes early.
        end_time = max(times)
        values = [twap_id, start_time.strftime('%Y%m%d%H%M%S'), end_time.strftime('%Y%m%d%H%M%S'), self.symbol, self.twap]
        db.insert_row('twaps', values)

    def log_twap(self, log):
        log.info(self.__str__())


def worker_func(log, worker_id, group, data_loader):
    # Create a new worker process for each group.
    interval, count, source = group
    completed = 0
    multiplier = 0 if globals.configs['environment'] == 'dev' else 60
    while completed < int(count):
        # TODO should not wait after last value is recorded.
        data_loader.get_ticker_values()
        completed += 1
        time.sleep(int(interval) * multiplier)
    data_loader.calculate_twaps()
    log.info('Loader {0} completed {1} [Source: {2}, Tickers: {3}, DataWarnings: {4}]'
             .format(worker_id,
                     'with WARNINGS.' if data_loader.data_warnings else 'SUCCESSFULLY!',
                     data_loader.source,
                     len(data_loader.twaps),
                     len(data_loader.data_warnings)))
    return data_loader


def required_tickers_for_group(db, group):
    condition = 'interval="{0}" AND count="{1}" AND source="{2}"'.format(*group)
    return db.query_table('twap_required_tickers', condition)


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-s', '--strategy', dest="strategy")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)
    parser.add_option('--debug', dest="debug", action="store_true", default=False)
    parser.add_option('--dry_run', action="store_true", default=False)

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "strategy": options.strategy,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "debug": options.debug,
        "dry_run": options.dry_run
    })


def main():
    # Setup configs.
    # global configs
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

    # Prepare multiprocessing pool.
    cpu_count = multiprocessing.cpu_count()
    pool = ThreadPool(cpu_count)

    # Count required tickers.
    strategy_id = db.get_one_row('strategies', 'name="{0}"'.format(globals.configs['strategy']))[0]
    required_tickers = db.execute_sql('SELECT symbol FROM twap_required_tickers WHERE strategy_id="{0}";'.format(strategy_id))
    globals.log.info('Found {0} required ticker(s)'.format(len(required_tickers)))

    # Get TWAPS.
    job.update_status('Generating required TWAPS')
    groups = [r for r in db.execute_sql('SELECT DISTINCT interval, count, source FROM twap_required_tickers where strategy_id="{0}";'.format(strategy_id))]
    globals.log.info('Grouped into {0} data loader(s)'.format(len(groups)))
    log_hr()
    workers = [pool.apply_async(worker_func,
                                args=(globals.log, groups.index(g), g, TWAPDataLoader(g[2], required_tickers, db), )
                                ) for g in groups]
    pool.close()
    pool.join()

    # Read results.
    job.update_status('Processing TWAPS')
    data_loaders = [w.get() for w in workers]
    retrieved_twaps = [i for i in chain.from_iterable([dl.twaps for dl in data_loaders])]
    data_warnings = [i for i in chain.from_iterable([dl.data_warnings for dl in data_loaders])]

    # Log results.
    globals.log.info('Retrieved {0} TWAPS'.format(len(retrieved_twaps)))
    if data_warnings:
        unique_data_warnings = list(set(data_warnings))
        globals.log.info('{0} Data Warnings:'.format(len(data_warnings)))
        for udw in unique_data_warnings:
            globals.log.info('[Symbol: {0}, Warning: {1}, Occurrences: {2}]'.format(udw[0], udw[1], data_warnings.count(udw)))

    # Save results to database.
    if not globals.configs['dry_run']:
        globals.log.info('Saving {0} TWAPS to database:'.format(len(retrieved_twaps)))
        for data_loader in data_loaders:
            data_loader.save_twaps_to_db(globals.log)

    else:
        globals.log.info('This is a dry run so TWAPS where not saved')

    success = len(required_tickers) == len(retrieved_twaps)
    if success:
        status = 2 if data_warnings else 0
    else:
        status = 1

    job.finished(status=status)
    return status


if __name__ == "__main__":
    sys.exit(main())
