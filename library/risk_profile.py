from library.bootstrap import Constants
from library.portfolio import Portfolio


class RiskProfile:

    CHECKS = 'checks'
    CHECK = 'name'
    THRESHOLD = 'threshold'

    EXPOSURE_LIMIT = 'max_exposure'
    LIQUIDITY_LIMIT = 'min_liquidity'

    def __init__(self, profile_dict):
        self.checks = profile_dict[RiskProfile.CHECKS]

    @staticmethod
    def _log_warning(text):
        Constants.log.warning('Risk Profile: {0}'.format(text))

    def _check_exposure_limit(self, portfolio):
        exposure = sum([portfolio.assets[a][Portfolio.EXPOSURE] for a in portfolio.assets])
        exposure_overflow = exposure - self.checks[RiskProfile.EXPOSURE_LIMIT]
        if exposure_overflow > 0:
            self._log_warning('Maximum exposure limit exceeded by {0}.'.format(abs(exposure_overflow)))
            return False
        return True

    def _check_liquidity_limit(self, portfolio):
        return True

    def assess_portfolio(self, portfolio):
        passes = True
        if RiskProfile.EXPOSURE_LIMIT in self.checks and not self._check_exposure_limit(portfolio):
            passes = False

        if RiskProfile.LIQUIDITY_LIMIT in self.checks and not self._check_liquidity_limit(portfolio):
            passes = False

        return passes
