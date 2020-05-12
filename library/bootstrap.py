class Constants:

    def __init__(self):
        self.log = None
        self.configs = None
        self.xml = XMLNameSpace()


class XMLNameSpace:

    def __init__(self):
        # Setup.
        self.portfolio = 'setup/portfolio'
        self.job = 'setup/schedule/job'

        # Data requirements.
        self.twap = 'data/twap'
        self.twitter = 'data/twitter'

        # Strategy Execution.
        self.risk_profile = 'execution/risk_profile'
        self.check = 'execution/risk_profile/check'
        self.function = 'execution/function'
        self.parameter = 'execution/function/parameter'


Constants = Constants()
