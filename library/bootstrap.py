class Constants:

    def __init__(self):
        self.log = None
        self.configs = None
        self.date_time_format = '%Y%m%d%H%M%S'
        self.pp_time_format = '%H:%M.%S'
        self.xml = XMLNameSpace()


class XMLNameSpace:

    def __init__(self):
        # Setup.
        self.portfolio = 'setup/portfolio'
        self.job = 'setup/schedule/job'

        # Tick capture.
        self.ticker = 'data/set/ticker'

        # Data requirements.
        self.data_set = 'data/set'
        self.tick = 'data/tick'
        self.twap = 'data/twap'
        self.twitter = 'data/twitter'

        # Strategy Execution.
        self.risk_profile = 'execution/risk_profile'
        self.check = 'execution/risk_profile/check'
        self.function = 'execution/function'
        self.parameter = 'execution/function/parameter'


Constants = Constants()
