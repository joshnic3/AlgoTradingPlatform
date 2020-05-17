class Constants:

    def __init__(self):
        self.log = None
        self.configs = None
        self.date_time_format = '%Y%m%d%H%M%S'
        self.pp_time_format = '%H:%M.%S'
        self.xml = XMLNameSpace()


class XMLNameSpace:

    def __init__(self):
        # Strategy setup.
        self.setup = 'setup'
        self.portfolio = '{0}/portfolio'.format(self.setup)
        self.cash = '{0}/cash'.format(self.portfolio)
        self.asset = '{0}/asset'.format(self.portfolio)
        self.job = '{0}/job'.format(self.setup)

        # Data requirements.
        self.data_requirements = 'data_requirements'
        self.ticker = '{0}/ticker'.format(self.data_requirements)

        # Strategy execution.
        self.execution = 'execution'

        # Risk profile.
        self.risk_profile = '{0}/risk_profile'.format(self.execution)
        self.check = '{0}/check'.format(self.risk_profile)

        # Strategy execution function.
        self.function = '{0}/function'.format(self.execution)
        self.parameter = '{0}/parameter'.format(self.function)


Constants = Constants()
