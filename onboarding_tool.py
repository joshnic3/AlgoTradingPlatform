import optparse
import os
import sys

from crontab import CronTab

from library.file_utils import add_dir, parse_configs_file, parse_wildcards, get_environment_specific_path, copy_file
from library.onboarding_utils import add_twap_required_tickers, add_data_source, \
    add_strategy, add_risk_profile, add_portfolio, add_assets
from library.db_utils import initiate_database


class StrategyOnboarder:

    def __init__(self, configs, name, risk_profile, portfolio_id, method, args, data_loader_schedule, strategy_schedule, required_tickers, environment):
        self.configs = configs
        self.name = name
        self.environment = environment
        self.risk_profile = risk_profile
        self.method = method
        self.args = args
        self.data_loader_schedule = data_loader_schedule
        self.strategy_schedule = strategy_schedule
        self.required_tickers = required_tickers
        self.portfolio_id = portfolio_id

    def deploy(self, db):
        # Add strategy.
        self.setup_strategy(db, self.name, self.risk_profile, self.portfolio_id, self.method, self.args, self.required_tickers)

        # Environment specific setup.
        if self.environment != 'dev':
            self.create_data_loader_job(self.name, self.data_loader_schedule)
            self.create_strategy_executor_job([self.name], self.strategy_schedule)
            self._generate_deployment_script()

    @staticmethod
    def setup_strategy(db, strategy_name, risk_profile, portfolio_id, method, args, required_tickers):
        # Add strategy and its required twaps.
        strategy_id = add_strategy(db, strategy_name, risk_profile, portfolio_id, args, method)
        [r.append(strategy_id) for r in required_tickers]

        # Setup test for data_loader
        add_twap_required_tickers(db, required_tickers)



        # Returns strategy name.
        return strategy_name

    def _generate_script_args(self, script_name, strategy_name):
        script_templates = self.configs['script_details']
        script_args_template = script_templates[script_name]['args']
        script_args = parse_wildcards(script_args_template, {'%e%': self.environment,
                                                             '%j%': '{0}_{1}'.format(strategy_name, script_name),
                                                             '%r%': self.configs['root_path'],
                                                             '%s%': strategy_name})
        return script_args

    def create_data_loader_job(self, strategy_name, schedule):
        # TODO Use venv interpreter.
        # should beable to set up paths in venv for use in stdizin args
        interpreter = 'python3'
        code_path = '/home/robot/projects/AlgoTradingPlatform'

        # Each strategy has one data_loader, each run = 1 twap for each required ticker for that strategy.
        script_name = 'data_loader'
        script_path = os.path.join(code_path, '{0}.py'.format(script_name))
        data_loader_args = self._generate_script_args(script_name, strategy_name)
        cron_job_template = [interpreter, script_path, data_loader_args]
        self._reset_cron_jobs()
        self._create_cron_job(cron_job_template, schedule)

    def create_strategy_executor_job(self, strategy_names, schedule):
        # TODO Use venv interpreter.
        # should beable to set up paths in venv for use in stdizin args
        interpreter = 'python3'
        code_path = '/home/robot/projects/AlgoTradingPlatform'

        # Each strategy_batch can run multiple strategies, schedule jobs at required runtime e.g. EOD
        script_name = 'strategy_batch'
        script_path = os.path.join(code_path, '{0}.py'.format(script_name))
        data_loader_args = self._generate_script_args('strategy_executor', ','.join(strategy_names))
        cron_job_template = [interpreter, script_path, data_loader_args]
        self._reset_cron_jobs()
        self._create_cron_job(cron_job_template, schedule)

    @staticmethod
    def _create_cron_job(template, schedule):
        # TODO Creating duplicate jobs.
        cron = CronTab(user=os.getlogin())
        command = ' '.join(template)
        job = cron.new(command=command)
        job.setall(schedule)
        cron.write()

    @staticmethod
    def _reset_cron_jobs():
        cron = CronTab(user=os.getlogin())
        cron.remove_all()

    def _generate_deployment_script(self):
        file_path = 'deploy_{}.sh'.format(self.environment)
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
                line_str = line.replace('%e%', self.environment) + '\n'
                df.write(line_str)


def parse_cmdline_args():
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-c', '--config_file', dest="config_file")

    options, args = parser.parse_args()
    return {
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "config_file": options.config_file,
    }


def main():
    configs = parse_cmdline_args()

    # Generate resource directories.
    resource_directories = ['logs', 'data', 'configs']
    environment_path = get_environment_specific_path(configs['root_path'], configs['environment'])
    add_dir(environment_path, backup=True)
    for directory in resource_directories:
        resource_path = os.path.join(environment_path, directory)
        add_dir(resource_path, backup=True)

    # Add in environment specific paths to system path.
    # TODO implement add in environment specific paths to system path.

    # Move config file to environment specific config path.
    environment_config_path = os.path.join(environment_path, 'configs', os.path.basename(configs['config_file']))
    copy_file(configs['config_file'], environment_config_path)

    # Read application configs.
    application_name = 'algo_trading_platform'
    app_configs = parse_configs_file({'root_path': configs['root_path'],
                                      'app_name': application_name,
                                      'environment': configs['environment']
                                      })

    # Initiate database.
    dbos = [initiate_database(app_configs['db_root_path'], d, app_configs['tables'][d], configs['environment'])
            for d in app_configs['tables']]

    db = dbos[0]

    # Setup data source.
    add_data_source(db, 'FML', os.path.join(app_configs['configs_root_path'], 'fml_data_source_config.json'))

    # Add risk profile.
    risk_profile_id = add_risk_profile(db, [1000.0, 1000000.0])

    # Setup basic limit strategy.
    # Add portfolio.
    strategy_name = 'basic_limit'
    portfolio_id = add_portfolio(db, '{0}_portfolio'.format(strategy_name), 'alpaca', '750.00')
    add_assets(db, portfolio_id, 'JPM', 0)
    onboarder = StrategyOnboarder(app_configs,
                                  strategy_name,
                                  risk_profile_id,
                                  portfolio_id,
                                  'basic',
                                  'JPM',
                                  "30 14-21 * * 1-5",
                                  "30 20 * * 1-5",
                                  [['JPM', '15', '4', 'FML']],
                                  configs['environment'])
    onboarder.deploy(db)

    # Setup frequent pairs strategy.
    strategy_name = 'pairs_frequent'
    portfolio_id = add_portfolio(db, '{0}_portfolio'.format(strategy_name), 'alpaca', '3844.05')
    add_assets(db, portfolio_id, 'TGT', 5)
    add_assets(db, portfolio_id, 'WMT', 5)
    onboarder = StrategyOnboarder(app_configs,
                                  strategy_name,
                                  risk_profile_id,
                                  portfolio_id,
                                  'pairs',
                                  'TGT,WMT',
                                  "*/15 14-21 * * 1-5",
                                  "30 15-20 * * 1-5",
                                  [['TGT', '5', '3', 'FML'], ['WMT', '5', '3', 'FML']],
                                  configs['environment'])
    onboarder.deploy(db)
    return 0


if __name__ == "__main__":
    sys.exit(main())
