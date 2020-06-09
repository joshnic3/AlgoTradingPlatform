import datetime
import logging
import optparse
import os
import sys
import pytz

from library.utilities.file import read_json_file


def get_log_file_path():
    today_str = datetime.datetime.now().strftime(Constants.DATETIME_FORMAT)
    if os.path.isdir(Constants.logs_path):
        return os.path.join(Constants.logs_path, '{0}_{1}.log'.format(Constants.job_name, today_str))
    else:
        temp_path = os.path.join(Constants.root_path, 'setuptemp')
        os.mkdir(temp_path)
        return os.path.join(temp_path, '{0}_{1}.log'.format(Constants.job_name, today_str))


def setup_log(show_in_console=False):
    # Setup logging to file.
    logging.root.handlers = []
    log_format = '%(asctime)s|%(levelname)s : %(message)s'
    if Constants.debug:
        logging.basicConfig(level='DEBUG', format=log_format, filename=Constants.log_path)
    else:
        logging.basicConfig(level='INFO', format=log_format, filename=Constants.log_path)
    log = logging.getLogger('')

    # Setup logging to console.
    if show_in_console:
        console = logging.StreamHandler()
        formatter = logging.Formatter(log_format)
        console.setFormatter(formatter)
        logging.getLogger('').addHandler(console)
    return log


def log_hr(width=100, new_line=False):
    width = 100 if not isinstance(width, int) or width > 100 or width < 0 else width
    line_parts = ['-' for i in range(int(Constants.DEFAULT_LOG_WIDTH * (width/100)))]
    Constants.log.info(''.join(line_parts))
    if new_line:
        Constants.log.info('')


class Constants:
    _RELATIVE_DB_PATH = 'data'
    _RELATIVE_CONFIGS_PATH = 'configs'
    _RELATIVE_LOGS_PATH = 'logs'
    _RELATIVE_STRATEGIES_PATH = 'strategies'
    _RELATIVE_REGRESSION_PATH = 'regressions'
    _DEVELOPMENT_ENVIRONMENT = 'dev'
    _CONFIG_FILE_EXTENSION = 'json'
    _MANUAL_RUN = 'manual_run'
    _PP_ARGUMENT_TEMPLATE = '{}: "{}"'

    TIME_ZONE = 'Europe/London'
    DATETIME_FORMAT = '%Y%m%d%H%M%S'
    PP_DATETIME_FORMAT = '%d/%m/%Y, %H:%M.%S'
    PP_DATE_FORMAT = '%d/%m/%Y'
    PP_TIME_FORMAT = '%H:%M.%S'
    DEFAULT_LOG_WIDTH = 80
    APP_NAME = 'algo_trading_platform'
    DB_NAME = APP_NAME
    RESOURCE_DIRS = [
        _RELATIVE_DB_PATH,
        _RELATIVE_CONFIGS_PATH,
        _RELATIVE_LOGS_PATH,
        _RELATIVE_STRATEGIES_PATH,
        _RELATIVE_REGRESSION_PATH
    ]

    def __init__(self):
        # Initiate global objects
        self.log = None
        self.xml = None
        self.configs = {}

        # Initiate global constants.
        self.environment = None
        self.root_path = None
        self.job_name = None
        self.debug = None
        self.log_path = None
        self.regression_path = None
        self.script = None
        self.run_time = datetime.datetime.now(pytz.timezone(self.TIME_ZONE))

        # Initiate environment specific paths.
        self.db_path = None
        self.configs_path = None
        self.logs_path = None

    def parse_arguments(self, configs_file_name=None, custom_args=[]):
        # TODO Swap out for arg parse, opt parse is no longer supported.
        # Initiate options parser.
        parser = optparse.OptionParser()
        parser.add_option('-e', '--environment', dest="environment")
        parser.add_option('-r', '--root_path', dest="root_path")
        parser.add_option('-x', '--xml_file', dest="xml_file")
        parser.add_option('-j', '--job_name', dest="job_name", default=None)
        parser.add_option('--debug', dest="debug", action="store_true", default=False)

        # Add custom options.
        for custom_arg in custom_args:
            parser.add_option('--{}'.format(custom_arg.lower()), dest=custom_arg.lower())

        # Parse options.
        options, args = parser.parse_args()

        # Setup global constants.
        self.environment = options.environment.lower() if options.environment else self._DEVELOPMENT_ENVIRONMENT
        self.root_path = options.root_path if options.root_path else None
        self.script = str(os.path.basename(sys.argv[0])).split('.')[0]
        self.job_name = options.job_name if options.job_name else '{}_{}'.format(self.script, self._MANUAL_RUN)
        self.debug = options.debug if options.debug else False

        # Setup environment specific paths.
        self.db_path = os.path.join(self.root_path, self.environment, self._RELATIVE_DB_PATH)
        self.configs_path = os.path.join(self.root_path, self.environment, self._RELATIVE_CONFIGS_PATH)
        self.logs_path = os.path.join(self.root_path, self.environment, self._RELATIVE_LOGS_PATH)
        self.regression_path = os.path.join(self.root_path, self.environment, self._RELATIVE_REGRESSION_PATH)

        # Initiate XML namespace.
        self.xml = StrategyXMLNameSpace(options.xml_file) if options.xml_file else None

        # Initiate logger.
        self.log_path = get_log_file_path()
        self.log = setup_log(True if self.environment == self._DEVELOPMENT_ENVIRONMENT else False)

        # Read in json configs if required.
        if configs_file_name and self.configs_path:
            configs_file_name = '{}.{}'.format(configs_file_name.lower(), self._CONFIG_FILE_EXTENSION)
            configs_file_path = os.path.join(self.configs_path, configs_file_name)
            self.log.info('Reading configs file: {}'.format(configs_file_path))
            self.configs = read_json_file(configs_file_path)
            config_strings = [self._PP_ARGUMENT_TEMPLATE.format(str(k), str(self.configs[k]))
                              for k in self.configs.keys() if self.configs[k]]
            for config_string in config_strings:
                self.log.info(config_string)
            log_hr()

        # Log custom arguments.
        if custom_args:
            self.log.info('Parsed {} custom argument(s).'.format(len(custom_args)))
            for custom_arg in custom_args:
                option_value = eval('options.{}'.format(custom_arg))
                self.configs[custom_arg] = option_value
                self.log.info(self._PP_ARGUMENT_TEMPLATE.format(custom_arg, option_value))
            log_hr()


class XMLNameSpace:
    _ELEMENT_PATH_TEMPLATE = '{}/{}'

    def __init__(self, root, path=None):
        self.root = root
        self.path = path

    def __repr__(self):
        return self.root


class StrategySetupXMLNameSpace(XMLNameSpace):
    _SETUP = 'setup'
    PORTFOLIO = 'portfolio'
    CASH = 'cash'
    ASSET = 'asset'
    JOB = 'job'
    ALLOCATION = 'allocation'

    def __init__(self):
        XMLNameSpace.__init__(self, self._SETUP)
        self.portfolio = self._ELEMENT_PATH_TEMPLATE.format(self.root, self.PORTFOLIO)
        self.cash = self._ELEMENT_PATH_TEMPLATE.format(self.portfolio, self.CASH)
        self.asset = self._ELEMENT_PATH_TEMPLATE.format(self.portfolio, self.ASSET)
        self.job = self._ELEMENT_PATH_TEMPLATE.format(self.root, self.JOB)
        self.allocation = self._ELEMENT_PATH_TEMPLATE.format(self.root, self.ALLOCATION)


class StrategyDataRequirementsXMLNameSpace(XMLNameSpace):
    _DATA_REQUIREMENTS = 'data_requirements'
    TICKER = 'ticker'

    def __init__(self):
        XMLNameSpace.__init__(self, self._DATA_REQUIREMENTS)
        self.ticker = self._ELEMENT_PATH_TEMPLATE.format(self.root, self.TICKER)


class StrategyExecutionXMLNameSpace(XMLNameSpace):
    _EXECUTION = 'execution'
    RISK_PROFILE = 'risk_profile'
    CHECK = 'check'
    FUNCTION = 'function'
    PARAMETER = 'parameter'

    def __init__(self):
        XMLNameSpace.__init__(self, self._EXECUTION)
        self.risk_profile = self._ELEMENT_PATH_TEMPLATE.format(self.root, self.RISK_PROFILE)
        self.check = self._ELEMENT_PATH_TEMPLATE.format(self.risk_profile, self.CHECK)
        self.function = self._ELEMENT_PATH_TEMPLATE.format(self.root, self.FUNCTION)
        self.parameter = self._ELEMENT_PATH_TEMPLATE.format(self.function, self.PARAMETER)


class StrategyXMLNameSpace(XMLNameSpace):
    _STRATEGY = 'strategy'

    def __init__(self, path):
        XMLNameSpace.__init__(self, self._STRATEGY, path=path)
        self.setup = StrategySetupXMLNameSpace()
        self.data_requirements = StrategyDataRequirementsXMLNameSpace()
        self.execution = StrategyExecutionXMLNameSpace()


# Initiate global constants object.
Constants = Constants()
