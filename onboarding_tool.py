import os
import sys

from crontab import CronTab

from library.bootstrap import Constants, log_hr
from library.interfaces.sql_database import initiate_database, Database
from library.strategy.strategy import parse_strategy_from_xml, parse_strategy_setup_from_xml
from library.utilities.file import add_dir, parse_wildcards, get_environment_specific_path, \
    copy_file, read_json_file
from library.utilities.onboarding import add_strategy, add_portfolio, add_assets

CONFIG_FILE = 'configs'
FUNCTIONS = 'functions'

INITIATE_ENVIRONMENT = 'environment'
ON_BOARD_STRATEGIES = 'strategy'
SETUP_CRON_JOBS = 'jobs'
ON_BOARDING_FUNCTIONS = [
    INITIATE_ENVIRONMENT,
    ON_BOARD_STRATEGIES,
    SETUP_CRON_JOBS
]


def main():
    # Setup parse options, imitate global constants and logs.
    args = [CONFIG_FILE, FUNCTIONS]
    Constants.parse_arguments(custom_args=args)

    # Which functions will be doe.
    if Constants.configs['functions']:
        functions_to_do = [f.lower() for f in Constants.configs[FUNCTIONS].split(',') if f in ON_BOARDING_FUNCTIONS]
    else:
        functions_to_do = FUNCTIONS

    if INITIATE_ENVIRONMENT in functions_to_do:
        Constants.log.info('Initiating environment.')
        # Generate resource directories.
        environment_path = get_environment_specific_path(Constants.root_path, Constants.environment)
        add_dir(environment_path, backup=True)
        for directory in Constants.RESOURCE_DIRS:
            resource_path = os.path.join(environment_path, directory)
            add_dir(resource_path, backup=True)

        # Move config file to environment specific config path.
        environment_config_path = os.path.join(environment_path, 'configs',
                                               os.path.basename(Constants.configs[CONFIG_FILE]))
        copy_file(Constants.configs[CONFIG_FILE], environment_config_path)

        # Read application configs.
        app_configs = read_json_file(Constants.configs[CONFIG_FILE])

        # Initiate database.
        dbos = [initiate_database(Constants.db_path, d, app_configs['tables'][d], Constants.environment)
                for d in app_configs['tables']]
        db = dbos[0]
    else:
        # Initiate database
        db_path = os.path.join(Constants.root_path, Constants.environment, 'data')
        db = Database(db_path, Constants.environment)
        # Load application configs.
        app_configs = read_json_file(Constants.configs[CONFIG_FILE])

    if Constants.xml:
        strategy_setup_dict = parse_strategy_setup_from_xml(Constants.xml.path)
        strategy_dict = parse_strategy_from_xml(Constants.xml.path)
    else:
        strategy_setup_dict = None
        strategy_dict = None

    if ON_BOARD_STRATEGIES in functions_to_do:
        if not strategy_setup_dict or not strategy_dict:
            raise Exception('XML file is required to on board a strategy.')

        Constants.log.info('Loading strategy "{}".'.format(strategy_dict['name']))

        # Initiate strategy if it does not exist.
        if not db.get_one_row('strategies', 'name="{0}"'.format(strategy_dict['name'])):
            # Add portfolio and strategy.
            portfolio_id = add_portfolio(db, '_{0}_portfolio'.format(strategy_dict['name']),
                                         strategy_setup_dict['allocation'], strategy_setup_dict['cash'])
            add_strategy(db, strategy_dict['name'], portfolio_id)

            # Add any assets.
            for asset in strategy_setup_dict['assets']:
                add_assets(db, portfolio_id, asset['symbol'])

        # Copy XML file to strategy directory.
        environment_path = get_environment_specific_path(Constants.root_path, Constants.environment)
        strategies_path = os.path.join(environment_path, 'strategies', '{0}.xml'.format(strategy_dict['name']))
        copy_file(Constants.xml.path, strategies_path)

    if SETUP_CRON_JOBS in functions_to_do:
        Constants.log.info('Setting up cron jobs.')
        if not strategy_setup_dict or not strategy_dict:
            raise Exception('XML file is required to add cron jobs.')

        # Only existing reset jobs when initialising the environment.
        reset = True if INITIATE_ENVIRONMENT in functions_to_do else False
        # interpreter = '/home/robot/projects/AlgoTradingPlatform/venv/bin/python3'
        interpreter = 'python3'
        code_path = '/home/robot/projects/AlgoTradingPlatform'

        # Initiate cron object.
        cron = CronTab(user=os.getlogin())
        if reset:
            cron.remove_all()

        # Create cron jobs from strategy schedule.
        for job in strategy_setup_dict['jobs']:
            # Extract details.
            name = job['name']
            script = job['script']
            schedule = job['schedule']

            # Parse script arguments.
            environment_path = get_environment_specific_path(Constants.root_path, Constants.environment)
            strategies_path = os.path.join(environment_path, 'strategies', '{0}.xml'.format(strategy_dict['name']))
            script_args_template = app_configs['script_details'][script]['args']
            script_args = parse_wildcards(script_args_template, {'%e%': Constants.environment,
                                                                     '%j%': '{0}_scheduled'.format(name),
                                                                     '%r%': Constants.root_path,
                                                                     '%x%': strategies_path})

            # Generate command.
            command_template = [interpreter, os.path.join(code_path, '{0}.py'.format(script)), script_args]
            command = ' '.join(command_template)

            # Create cron jobs.
            job = cron.new(command=command)
            job.setall(schedule)
            cron.write()

    log_hr()
    Constants.log.info('On-boarding finished.')
    return 0


if __name__ == "__main__":
    sys.exit(main())
