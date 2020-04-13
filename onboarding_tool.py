import sys
import os
import optparse

from crontab import CronTab

from library.onboarding_utils import setup_database_environment_path, add_twap_required_tickers, add_data_source, add_strategy
from library.file_utils import edit_config_file, parse_configs_file, parse_wildcards
from library.db_interface import initiate_database
from library.data_source_utils import initiate_data_source_db


class ApplicationOnboarder:

    def __init__(self, app_configs, environment):
        self.name = app_configs['app_name']
        self._app_configs_file_path = os.path.join(app_configs['configs_root_path'], '{0}_config.json'.format(self.name))
        self._app_configs = app_configs
        self._environment = environment.lower()

    def deploy(self):
        db = initiate_database(self._app_configs['db_root_path'], self.name, self._app_configs['schema'], self._environment)
        self._write_setup_data_to_db(db)
        self._add_environment_to_app_config()
        if not self._environment == 'dev':
            self._generate_deployment_script()
            self._setup_cron_jobs()

    def _write_setup_data_to_db(self, db):
        # Setup test for data_loader
        required_tickers = [['0', 'NMR', 'FML', '15', '2'],
                            ['1', 'MSFT', 'FML', '15', '2'],
                            ['2', 'MS', 'FML', '10', '3'],
                            ['3', 'JPM', 'FML', '10', '3']]

        # DB can be passed in here, will be far neater.
        add_twap_required_tickers(db, required_tickers)
        add_strategy(db, 'basic_test', 'NMR', 'basic', 15)
        add_strategy(db, 'pairs_test', 'NMR', 'pairs', 4)
        ds_db = initiate_data_source_db(self._app_configs['db_root_path'], self._environment.lower())
        add_data_source(ds_db, 'FML',
                        os.path.join(self._app_configs['configs_root_path'], 'fml_data_source_config.json'))

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
        environments = self._app_configs['environments']
        if self._environment not in environments:
            edit_config_file(self._app_configs_file_path, 'environments', environments.append(self._environment))

    def _setup_cron_jobs(self):
        # Will re-write over existing jobs for now.
        cron = CronTab(user=os.getlogin())
        cron.remove_all()

        # Get environment information.
        repo_path = os.path.dirname(__file__)
        interpreter = 'python3'

        # Create jobs.
        jobs = self._app_configs['jobs']
        for job in jobs:
            # Get and format information.
            schedule = jobs[job]['schedule']
            script_name = '{0}.py'.format(jobs[job]['script'])
            script_path = os.path.join(repo_path, script_name)
            args_template = jobs[job]['args']
            args = parse_wildcards(args_template,
                                   {'%e%': self._environment,
                                    '%j%': job,
                                    '%r%': self._app_configs['root_path']})
            cron_job_template = [interpreter, script_path, args]
            command = ' '.join(cron_job_template)

            # Add job.
            job = cron.new(command=command)
            job.setall(schedule)
            cron.write()


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-j', '--job_name', dest="job_name", default=None)
    parser.add_option('--dry_run', action="store_true", default=False)

    # Add custom option.
    parser.add_option('-a', '--applications', dest="applications", default=None)

    options, args = parser.parse_args()
    return parse_configs_file({
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "job_name": options.job_name,
        "script_name": str(os.path.basename(sys.argv[0])).split('.')[0],
        "dry_run": options.dry_run,

        "applications": options.applications
    })


def main():
    configs = parse_cmdline_args('algo_trading_platform')

    if configs['applications']:
        for app in configs['applications'].split(','):
            app = app.lower()
            app_configs = parse_configs_file({'root_path': configs['root_path'], 'app_name': app})
            app_configs_file_path = os.path.join(app_configs['configs_root_path'], '{0}_config.json'.format(app))
            setup_database_environment_path(app_configs['db_root_path'], app_configs_file_path, app_configs['schema'], configs['environment'])
            onboarder = ApplicationOnboarder(app_configs, configs['environment'])
            onboarder.deploy()

    return 0


if __name__ == "__main__":
    sys.exit(main())
