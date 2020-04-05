import requests

from library.db_interface import Database
from library.file_utils import read_json_file


def get_data_source_configs(name, db_root_path, environment):
    ds_db = Database(db_root_path, 'data_sources', True, environment)
    condition = 'name="{0}"'.format(name)
    values = ['name', 'configs']
    results = ds_db.query_table('data_sources', condition, values)
    config_files = dict(results)
    return config_files[name]


class DataSource:

    def __init__(self, name, db_root_path, environment):
        self.name = name
        self._configs = read_json_file(get_data_source_configs(self.name, db_root_path, environment))

    def __str__(self):
        return self.name

    @staticmethod
    def _call_api_return_as_dict(url):
        response = requests.get(url)
        return response.json()

    @staticmethod
    def _prepare_api_call_url(template, wildcards_dict):
        url = template
        for wildcard in wildcards_dict:
            url = url.replace(wildcard, wildcards_dict[wildcard])
        return url


