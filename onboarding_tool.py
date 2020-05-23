import optparse
import os
import sys

from crontab import CronTab

from library.bootstrap import Constants
from library.interfaces.sql_database import initiate_database, Database
from library.strategy import parse_strategy_from_xml, parse_strategy_setup_from_xml
from library.utilities.file import add_dir, parse_configs_file, parse_wildcards, get_environment_specific_path, \
    copy_file
from library.utilities.onboarding import add_strategy, add_portfolio, add_assets

INITIATE_ENVIRONMENT = 'environment'
ON_BOARD_STRATEGIES = 'strategy'
SETUP_CRON_JOBS = 'jobs'


def parse_cmdline_args(app_name):
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-c', '--config_file', dest="config_file")
    parser.add_option('-f', '--functions', dest="functions")
    parser.add_option('-x', '--xml_file', dest="xml_file")

    options, args = parser.parse_args()
    return {
        "app_name": app_name,
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "config_file": options.config_file,
        "functions": options.functions,
        "xml_file": options.xml_file
    }


def main():
    function = [INITIATE_ENVIRONMENT, ON_BOARD_STRATEGIES, SETUP_CRON_JOBS]
    # Parser on boarding tool parameters.
    Constants.configs = parse_cmdline_args('algo_trading_platform')

    # Which functions will be doe.
    if Constants.configs['functions']:
        functions_to_do = [f.lower() for f in Constants.configs['functions'].split(',') if f in function]
    else:
        functions_to_do = function



    if INITIATE_ENVIRONMENT in functions_to_do:
        # Generate resource directories.
        resource_directories = ['logs', 'data', 'configs', 'strategies']
        environment_path = get_environment_specific_path(Constants.configs['root_path'], Constants.configs['environment'])
        add_dir(environment_path, backup=True)
        for directory in resource_directories:
            resource_path = os.path.join(environment_path, directory)
            add_dir(resource_path, backup=True)

        # TODO Copy all configs to env dir.
        # Move config file to environment specific config path.
        environment_config_path = os.path.join(environment_path, 'configs', os.path.basename(Constants.configs['config_file']))
        copy_file(Constants.configs['config_file'], environment_config_path)

        # Read application configs.
        app_configs = parse_configs_file({'root_path': Constants.configs['root_path'],
                                          'app_name': 'algo_trading_platform',
                                          'environment': Constants.configs['environment']
                                          })

        # Initiate database.
        dbos = [initiate_database(app_configs['db_root_path'], d, app_configs['tables'][d], Constants.configs['environment'])
                for d in app_configs['tables']]
        db = dbos[0]
    else:
        # Initiate database
        db = Database(Constants.configs['db_root_path'], Constants.configs['environment'])
        # Load application configs.
        app_configs = parse_configs_file({'root_path': Constants.configs['root_path'],
                                          'app_name': 'algo_trading_platform',
                                          'environment': Constants.configs['environment']
                                          })

    if Constants.configs['xml_file']:
        strategy_setup_dict = parse_strategy_setup_from_xml(Constants.configs['xml_file'])
        strategy_dict = parse_strategy_from_xml(Constants.configs['xml_file'])
    else:
        strategy_setup_dict = None
        strategy_dict = None

    if ON_BOARD_STRATEGIES in functions_to_do:
        if not strategy_setup_dict or not strategy_dict:
            raise Exception('XML file is required to on board a strategy.')

        # Initiate strategy if it does not exist.
        if not db.get_one_row('strategies', 'name="{0}"'.format(strategy_dict['name'])):
            # Add portfolio and strategy.
            portfolio_id = add_portfolio(db, '_{0}_portfolio'.format(strategy_dict['name']), strategy_setup_dict['cash'])
            add_strategy(db, strategy_dict['name'], portfolio_id)

            # Add any assets.
            for asset in strategy_setup_dict['assets']:
                add_assets(db, portfolio_id, asset['symbol'])

        # Copy XML file to strategy directory.
        environment_path = get_environment_specific_path(Constants.configs['root_path'], Constants.configs['environment'])
        strategies_path = os.path.join(environment_path, 'strategies', '{0}.xml'.format(strategy_dict['name']))
        copy_file(Constants.configs['xml_file'], strategies_path)

    if SETUP_CRON_JOBS in functions_to_do:
        if not strategy_setup_dict or not strategy_dict:
            raise Exception('XML file is required to add cron jobs.')

        # Only existing reset jobs when initialising the environment.
        reset = True if INITIATE_ENVIRONMENT in functions_to_do else False
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
            environment_path = get_environment_specific_path(Constants.configs['root_path'], Constants.configs['environment'])
            strategies_path = os.path.join(environment_path, 'strategies', '{0}.xml'.format(strategy_dict['name']))
            script_args_template = app_configs['script_details'][script]['args']
            script_args = parse_wildcards(script_args_template, {'%e%': Constants.configs['environment'],
                                                                     '%j%': '{0}_scheduled'.format(name),
                                                                     '%r%': Constants.configs['root_path'],
                                                                     '%x%': strategies_path})

            # Generate command.
            command_template = [interpreter, os.path.join(code_path, '{0}.py'.format(script)), script_args]
            command = ' '.join(command_template)

            # Create cron jobs.
            job = cron.new(command=command)
            job.setall(schedule)
            cron.write()
    return 0


if __name__ == "__main__":
    sys.exit(main())
