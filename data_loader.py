import sys
import os
import datetime
import time
import multiprocessing
import requests
from multiprocessing.pool import ThreadPool

from library.db_interface import Database
from library.file_utils import read_json_file
from library.log_utils import get_log_file_path, setup_log, log_configs_as_string, log_seperator
from library.data_source_utils import get_data_source_configs

configs = {}


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
            self.ticks.append((datetime.datetime.now(), float(value)))
            return self.symbol, 'STALE_TICKER'
        self.ticks.append((datetime.datetime.now(), float(value)))
        return None

    def calculate_twap(self):
        self.twap = 0
        if self.ticks:
            self.twap = sum([float(t[1]) for t in self.ticks])/len(self.ticks)
        return self.twap

    def save_to_db(self, db):
        times = [t[0] for t in self.ticks]
        # start_time TEXT, end_time TEXT, value TEXT
        values = [0, min(times).strftime('%Y%m%d%H%M%S'), max(times).strftime('%Y%m%d%H%M%S'), self.symbol, self.twap]
        db.insert_row('twaps', values)

    def log_twap(self, log):
        log.info(self.__str__())


# TODO add error handling and read from db, should be as generic as possible.
class DataSource:

    def __init__(self, name):
        self.name = name
        _ds_config_file = os.path.join(configs['root_path'], get_data_source_configs(self.name, configs)['configs'])
        self._ds_configs = read_json_file(_ds_config_file)
        self._request_template = self._ds_configs['request_template']

    def __str__(self):
        return self.name

    def _extract_data(self, response):
        data = response.json()
        # Somehow get this from config, how can it be generic?
        data = data['companiesPriceList']
        return dict(zip([d[self._ds_configs['symbol_key']] for d in data], [d[self._ds_configs['value_key']] for d in data]))

    def request_tickers(self, symbols):
        # Request data.
        symbols_str = ",".join(symbols) if len(symbols) > 1 else symbols[0]
        request = self._request_template.replace('%symbols%', symbols_str)
        response = requests.get(request)

        # Extract data.
        return self._extract_data(response)


class TWAPDataLoader:

    def __init__(self, source, tickers):
        self.source = DataSource(source)
        self.twaps = [TWAP(t[1]) for t in tickers]
        self.data_warnings = []

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


def worker(log, worker_id, group, required_tickers):
    # IN OWN PROCESS ------------
    # Create a new worker process for each group.
    data_loader = TWAPDataLoader(group[2], required_tickers)
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


def parse_configs():
    # Read script configurations into dict.
    script_config_file = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs/data_loader_config.json'
    configs = read_json_file(script_config_file)

    # Read parameters into configurations dict.
    configs['dry_run'] = False
    configs['environment'] = 'dev'
    return configs


def main():
    # Read configs
    global configs
    configs = parse_configs()

    # Setup logging.
    script_name = str(os.path.basename(sys.argv[0]))
    log_path = get_log_file_path(configs['root_path'], script_name.split('.')[0])
    log = setup_log(log_path)
    log.info('Configs: {}'.format(log_configs_as_string(configs)))

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
    data_loaders = []
    log.info('Grouped into {0} data loader(s)'.format(len(groups)))
    log_seperator(log)
    for group in groups:
        required_tickers = db.query_table('twap_required_tickers'
                                          , 'interval="{0}" AND resolution="{1}" AND source="{2}"'.format(group[0],
                                                                                                          group[1],
                                                                                                          group[2]))
        data_loaders.append(pool.apply_async(worker, args=(log, groups.index(group), group, required_tickers, )))
    pool.close()
    pool.join()

    # Read results.
    twaps = []
    data_warnings = []
    for data_loader in data_loaders:
        result = data_loader.get()
        twaps += result.twaps
        data_warnings += result.data_warnings

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

    if configs['dry_run']:
        log.info('This is a dry run so will not save TWAPS to Database')
    else:
        log.info('Saving {0} TWAPS to Database'.format(len(twaps)))
        for twap in twaps:
            twap.save_to_db(db)

    requested = len(db.query_table('twap_required_tickers'))
    successful = len(twaps)

    log_seperator(log)
    if requested == successful:
        if data_warnings:
            log.info('{0} finished with WARNINGS'.format(script_name))
            return 2
        log.info('{0} finished with SUCCESSFULLY!'.format(script_name))
        return 0
    log.info('{0} finished with ERRORS'.format(script_name))
    return 1


if __name__ == "__main__":
    sys.exit(main())
