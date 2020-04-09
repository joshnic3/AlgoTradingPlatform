import sys
import os
import optparse

from library.onboarding_utils import setup_database_environment_path, add_twap_required_tickers, add_data_source
from library.file_utils import read_json_file, edit_config_file, parse_configs_file
from library.db_interface import Database


class ApplicationOnboarder:

    def __init__(self, configs_path, application_name, environment):
        self.name = application_name
        self._configs_file_path = os.path.join(configs_path, '{0}_config.json'.format(self.name ))
        self._configs = parse_configs_file({'config_file': self._configs_file_path})

        self._db = None
        self._environment = environment.lower()

    def deploy(self):
        self._db = Database(self._configs['db_root_path'], self.name, auto_create=True, environment=self._environment)
        self._write_setup_data_to_db()
        # self._generate_deployment_script()
        self._add_environment_to_app_config()
        # self._setup_cron_jobs()

    def _write_setup_data_to_db(self):
        if self._db is None:
            raise Exception('Database has not been setup yet.')

        # Setup test for data_loader
        required_tickers = [['0', 'NMR', 'FML', '15', '4'],
                            ['1', 'MSFT', 'FML', '15', '4'],
                            ['2', 'MS', 'FML', '5', '3'],
                            ['3', 'JPM', 'FML', '5', '3']]
        add_twap_required_tickers(self._configs['db_root_path'], self._environment, required_tickers)

    def _generate_deployment_script(self):
        file_path = 'deploy_{}.sh'.format(self._environment)
        deploy_template = [
            '#!/bin/sh',
            'echo Deploying %e%'
            'exec git stash',
            'exec git pull origin %e%',
            'exec git checkout %e%',
            'if [ $(git rev-parse --abbrev-ref HEAD) = %e% ]; then',
            '   echo Successfully deployed %e%!',
            'else',
            '   echo Failed to deployed %e%.',
            'fi'
        ]
        with open(file_path, 'w') as df:
            for line in deploy_template:
                line_str = line.replace('%e%', self._environment) + '\n'
                df.write(line_str)

    def _add_environment_to_app_config(self):
        environments = self._configs['environments']
        if self._environment not in environments:
            edit_config_file(self._configs_file_path, 'environments', environments.append(self._environment))

    # TODO Implement
    def _setup_cron_jobs(self):
        # Use venv python, its included in the repo.
        python_path = '/Users/joshnicholls/PycharmProjects/algo_trading_platform/repo/venv/bin/python3.7'
        pass


def parse_cmdline_args():
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-c', '--configs_path', dest="configs_path")
    parser.add_option('-a', '--applications', dest="applications", default=None)
    parser.add_option('-d', '--data_sources', dest="data_sources", default=None)

    options, args = parser.parse_args()
    return {
        "environment": options.environment.lower(),
        "configs_path": options.configs_path,
        "applications": options.applications,
        "data_sources": options.data_sources
    }


def main():
    configs = parse_cmdline_args()

    # if configs['applications']:
    #     for app in configs['applications'].split(','):
    #         app = app.lower()
    #         app_configs_file_path = os.path.join(configs['configs_path'], '{0}_config.json'.format(app))
    #         app_configs = parse_configs_file({'config_file': app_configs_file_path})
    #
    #         setup_database_environment_path(app_configs['db_root_path'], configs['environment'])
    #         onboarder = ApplicationOnboarder(configs['configs_path'], app, configs['environment'])
    #         onboarder.deploy()
    #
    # # TODO on-boarding data sources
    # if configs['data_sources']:
    #     add_data_source('/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/data', configs['environment'], 'FML', os.path.join('/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs', 'fml_data_source_config.json'))
    add_data_source('/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/data', configs['environment'], 'FML', os.path.join('/Users/joshnicholls/PycharmProjects/algo_trading_platform/drive/configs', 'fml_data_source_config.json'))

    return 0


if __name__ == "__main__":
    sys.exit(main())
