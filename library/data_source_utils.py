import requests

from library.db_interface import Database
from library.file_utils import read_json_file


# Data source db is constant so can be initiated in a function.
def initiate_data_source_db(db_root_path, environment):
    return Database(db_root_path, 'data_sources', environment)


def get_data_source_configs(name, db_root_path, environment):
    ds_db = initiate_data_source_db(db_root_path, environment)
    condition = 'name="{0}"'.format(name)
    values = ['name', 'configs']
    results = ds_db.query_table('data_sources', condition, values)
    if results:
        config_files = dict(results)
        return config_files[name]
    raise Exception('No data source "{0}" found in database.'.format(name))


class DataSource:

    def __init__(self, name, db_root_path, environment):
        self.name = name
        data_source_configs_file = get_data_source_configs(self.name, db_root_path, environment)
        self._configs = read_json_file(data_source_configs_file)

    def __str__(self):
        return self.name

    # TODO Handle common and else errors.
    def _catch_errors(self, response):
        response_code = response.status_code
        if response_code == 200:
            return response
        raise Exception('Bad response from source: {0}, code: {1}'.format(self.name, response_code))

    def _call_api_return_as_dict(self, url):
        results = None
        try:
            response = requests.get(url)
            self._catch_errors(response)
            results = response.json()
        except requests.HTTPError:
            raise Exception('Could not connect to source: {0}'.format(self.name))

        return results

    @staticmethod
    def _prepare_api_call_url(template, wildcards_dict):
        url = template
        for wildcard in wildcards_dict:
            url = url.replace(wildcard, wildcards_dict[wildcard])
        return url


class TickerDataSource(DataSource):

    def __init__(self, name, db_root_path, environment):
        DataSource.__init__(self, name, db_root_path, environment)

    def _extract_data(self, result):
        # Takes [{symbol_key: symbol}, {value_key, value}] and returns {symbol: value}.
        return dict(zip([r[self._configs['symbol_key']] for r in result], [r[self._configs['value_key']] for r in result]))

    def request_tickers(self, symbols):
        symbols_str = self._configs['delimiter'].join(symbols) if len(symbols) > 1 else symbols[0]
        wildcard = {self._configs['wildcards']['symbols']: symbols_str}
        url = self._prepare_api_call_url(self._configs['request_template'], wildcard)
        result = self._call_api_return_as_dict(url)
        return self._extract_data(result['companiesPriceList'])
