import optparse
import os
import sys
import xml.etree.ElementTree as et

from library.bootstrap import Constants
from library.interfaces.sql_database import initiate_database, Database
from library.utilities.file import add_dir, parse_configs_file, parse_wildcards, get_environment_specific_path, \
    copy_file, get_xml_element_attribute
from library.utilities.onboarding import add_strategy, add_portfolio, add_assets
from crontab import CronTab

NS = {
    'XML_RISK_PROFILE_LABEL': 'execution/risk_profile',
    'XML_CHECK_LABEL': 'execution/risk_profile/check',
    'XML_CHECK_ATTRIBUTES': {
        'NAME': 'name',
        'THRESHOLD': 'threshold'
    },
    'XML_PARAMETER_LABEL': 'execution/function/parameter',
    'XML_PARAMETER_ATTRIBUTES': {
        'KEY': 'key',
        'VALUE': 'value'
    },
    'XML_FUNCTION_LABEL': 'execution/function',
    'XML_FUNCTION_ATTRIBUTES': {
        'FUNC': 'func'
    }
}


def parse_cmdline_args():
    parser = optparse.OptionParser()
    parser.add_option('-e', '--environment', dest="environment")
    parser.add_option('-r', '--root_path', dest="root_path")
    parser.add_option('-c', '--config_file', dest="config_file")
    parser.add_option('-x', '--xml_file', dest="xml_file")

    options, args = parser.parse_args()
    return {
        "environment": options.environment.lower(),
        "root_path": options.root_path,
        "config_file": options.config_file,
        "xml_file": options.xml_file
    }


def main():
    # Parser on boarding tool parameters.
    Constants.configs = parse_cmdline_args()

    # Which functions will be doe.
    if 'functions' in Constants.configs:
        functions = [f.lower() for f in Constants.configs['functions'].split(',')]
    else:
        functions = ['init_env', 'onboard_strat', 'cron_jobs']

    if 'init_env' in functions:
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
        db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])
        # Load application configs.
        app_configs = parse_configs_file({'root_path': Constants.configs['root_path'],
                                          'app_name': 'algo_trading_platform',
                                          'environment': Constants.configs['environment']
                                          })

    if 'onboard_strat' in functions:
        if 'xml_file' not in Constants.configs:
            raise Exception('XML file is required to onboard a strategy.')
        # Extract strategy name.
        strategy = et.parse(Constants.configs['xml_file']).getroot()
        strategy_name = get_xml_element_attribute(strategy, 'name').lower()

        # Initiate strategy if it does not exist.
        portfolio = strategy.findall(Constants.xml.portfolio)[0]

        if not db.get_one_row('strategies', 'name="{0}"'.format(strategy_name)):
            # Add portfolio and strategy.
            portfolio_weighting = float(get_xml_element_attribute(portfolio, 'weighting'))
            portfolio_id = add_portfolio(db, '_{0}_portfolio'.format(strategy_name), 'alpaca', portfolio_weighting)
            add_strategy(db, strategy_name, portfolio_id)

            # Add any assets.
            for asset in portfolio.findall('asset'):
                symbol = get_xml_element_attribute(asset, 'symbol')
                units = int(get_xml_element_attribute(asset, 'units'))
                add_assets(db, portfolio_id, symbol, units=units)

        # Copy XML file to strategy directory.
        environment_path = get_environment_specific_path(Constants.configs['root_path'], Constants.configs['environment'])
        environment_strategies_path = os.path.join(environment_path, 'strategies',
                                                   os.path.basename(Constants.configs['xml_file']))
        copy_file(Constants.configs['xml_file'], environment_strategies_path)

    if 'cron_jobs' in functions:
        if 'xml_file' not in Constants.configs:
            raise Exception('XML file is required to add cron jobs.')

        reset = False
        interpreter = 'python3'
        code_path = '/home/robot/projects/AlgoTradingPlatform'

        # Initiate cron object.
        cron = CronTab(user=os.getlogin())
        if reset:
            cron.remove_all()

        # Create cron jobs from strategy schedule.
        strategy = et.parse(Constants.configs['xml_file']).getroot()
        for job in strategy.findall(Constants.xml.job):
            # Extract details.
            name = get_xml_element_attribute(job, 'name')
            script = get_xml_element_attribute(job, 'script')
            schedule = get_xml_element_attribute(job, 'schedule')

            # Parse script arguments.
            environment_path = get_environment_specific_path(Constants.configs['root_path'],
                                                             Constants.configs['environment'])
            environment_strategies_path = os.path.join(environment_path, 'strategies',
                                                       os.path.basename(Constants.configs['xml_file']))
            script_args_template = app_configs['script_details'][script]['args']
            script_args = parse_wildcards(script_args_template, {'%e%': Constants.configs['environment'],
                                                                     '%j%': '{0}_scheduled'.format(name),
                                                                     '%r%': Constants.configs['root_path'],
                                                                     '%x%': environment_strategies_path})

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
