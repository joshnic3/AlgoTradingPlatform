import sys
import os

from library.onboarding_utils import setup_database_environments_paths, add_twap_required_tickers, add_data_source
from library.file_utils import read_json_file
from library.db_interface import Database

# Database(db_root_path, database, auto_create=True, environment=environment.lower())
# class DataSourceOnboarder:


class ApplicationOnboarder:

    def __init__(self, configs_path, application_name, environment):
        self.name = application_name
        self._configs = read_json_file(os.path.join(configs_path, '{0}_config.json'.format(application_name)))
        self._db = None
        self._environment = environment.lower()

    def setup_db(self):
        db_root_path = os.path.join(self._configs['db_root_path'])
        self._db = Database(db_root_path, self.name, auto_create=True, environment=self._environment)
        self._write_required_rows()

    def _write_required_rows(self):
        if self._db is None:
            raise Exception('Database has not been setup yet.')

        # Setup test for data_loader
        required_tickers = [['0', 'NMR', 'FML', '60', '4'],
                            ['1', 'MSFT', 'FML', '60', '4'],
                            ['2', 'MS', 'FML', '15', '3'],
                            ['3', 'JPM', 'FML', '15', '3']]
        add_twap_required_tickers(self._configs['db_root_path'], self._environment, required_tickers)


def main():
    # should be able to set everything up with just "db_root_path" and "root_path" (including data_loader_config and crontab jobs)

    # Parse configs.
    configs = read_json_file(os.path.join('/home/robot/drive/configs', '{0}_config.json'.format('data_loader')))

    configs['application_name'] = 'algo_trading_platform'
    configs['configs_path'] = '/home/robot/drive/configs'
    configs['environment'] = 'dev'

    # Setup database environments.
    setup_database_environments_paths(configs['db_root_path'])

    # Initiate each application onboarder.
    applications = configs['application_name'].split(',')
    onboarders = [ApplicationOnboarder(configs['configs_path'], a, configs['environment']) for a in applications]

    # On board each application.
    for onboarder in onboarders:
        onboarder.setup_db()
        onboarder.setup_db(configs['environment'])


    # TODO datasources
    # add_data_source(configs['db_root_path'], environment, 'FML', os.path.join(configs_path, 'fml_data_source_config.json'))

    return 0


if __name__ == "__main__":
    sys.exit(main())
