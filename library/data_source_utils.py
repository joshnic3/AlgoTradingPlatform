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


