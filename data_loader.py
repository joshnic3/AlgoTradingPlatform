import sys
import datetime
import time
import multiprocessing
import requests
from multiprocessing.pool import ThreadPool

from library.db_interface import Database
from library.file_utils import read_json_file


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
            self.ticks.append((now.strftime('%Y%m%d%H%M%S'), float(value)))
            return self.symbol, 'STALE_TICKER'
        self.ticks.append((now.strftime('%Y%m%d%H%M%S'), float(value)))
        return None

    def calculate_twap(self):
        self.twap = 0
        if self.ticks:
            self.twap = sum([float(t[1]) for t in self.ticks])/len(self.ticks)
        return self.twap


class DataSource:

    def __init__(self, name):
        self.name = name
        # Read from db
        self.request_template = "https://financialmodelingprep.com/api/v3/stock/real-time-price/%symbols%"

    def __str__(self):
        return self.name

    @staticmethod
    def _extract_data(response):
        data = response.json()
        # if len(symbols) > 1:
        data = data['companiesPriceList']
        return dict(zip([d['symbol'] for d in data], [d['price'] for d in data]))

    def request_tickers(self, symbols):
        # Request data.
        symbols_str = ",".join(symbols) if len(symbols) > 1 else symbols[0]
        request = self.request_template.replace('%symbols%', symbols_str)
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

    def pprint_twaps(self):
        for twap in self.twaps:
            print(twap)

    def calculate_twaps(self):
        for twap in self.twaps:
            twap.calculate_twap()


def worker(worker_id, group, required_tickers):
    # IN OWN PROCESS ------------
    # Create a new worker process for each group.
    data_loader = TWAPDataLoader(group[2], required_tickers)
    completed = 0
    while completed < int(group[1]):
        data_loader.get_ticker_values()
        completed += 1
        time.sleep(int(group[0]))
    data_loader.calculate_twaps()
    print('Worker {0} completed {1} [Source: {2}, Tickers: {3}, DataWarnings: {4}]'.format(worker_id,
                                                                                  'with WARNINGS.' if data_loader.data_warnings else 'SUCCESSFULLY!',
                                                                                  data_loader.source,
                                                                                  len(data_loader.twaps),
                                                                                  len(data_loader.data_warnings)))

    data_loader.pprint_twaps()
    print('--------------\n')
    return data_loader


def pp_results(twaps, data_warnings):
    print('TWAPS:')
    [print(t) for t in twaps]
    print('\nData Warnings:')
    if data_warnings:
        unique_data_warnings = list(set(data_warnings))
        for udw in unique_data_warnings:
            print('Symbol: {0}, Warning: {1}, Occurrences: {2}'.format(udw[0], udw[1], data_warnings.count(udw)))


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
    configs = parse_configs()

    # Setup db connection.
    db = Database(configs['db_root_path'], configs['environment'], configs['schema'])

    # Testing db
    # rows = [['2', 'NMR', 'Bloomberg', '20', '5'], ['2', 'MSFT', 'Bloomberg', '20', '5']]
    # for row in rows:
    #     db.insert_row('twap_required_tickers', row)

    # Prepare multiprocessing pool.
    cpu_count = multiprocessing.cpu_count()
    pool = ThreadPool(cpu_count)

    # Group TWAPS by DISTINCT interval, resolution and source
    results = db.execute_sql('SELECT DISTINCT interval, resolution, source FROM twap_required_tickers;')
    groups = [r for r in results]
    data_loaders = []
    for group in groups:
        required_tickers = db.query_table('twap_required_tickers'
                                          , 'interval="{0}" AND resolution="{1}" AND source="{2}"'.format(group[0],
                                                                                                          group[1],
                                                                                                          group[2]))
        data_loaders.append(pool.apply_async(worker, args=(groups.index(group), group, required_tickers, )))

    pool.close()
    pool.join()

    # Process results.
    twaps = []
    data_warnings = []
    for data_loader in data_loaders:
        result = data_loader.get()
        twaps += result.twaps
        data_warnings += result.data_warnings

    pp_results(twaps, data_warnings)

    requested = len(db.query_table('twap_required_tickers'))
    successful = len(twaps)

    if requested == successful:
        if data_warnings:
            return 2
        return 0

    return 1


if __name__ == "__main__":
    sys.exit(main())
