from library.bootstrap import Constants
from library.strategy.portfolio import Portfolio


class RiskProfile:

    CHECKS = 'checks'
    CHECK = 'name'
    THRESHOLD = 'threshold'

    EXPOSURE_LIMIT = 'max_exposure'
    NEGATIVE_UNITS = 'negative_units'

    def __init__(self, profile_dict):
        self.checks = profile_dict[self.CHECKS]

    @staticmethod
    def _log_warning(text):
        Constants.log.warning('Risk Profile: {0}'.format(text))

    def _check_exposure_limit(self, portfolio):
        exposure = sum([portfolio.assets[a][Portfolio.EXPOSURE] for a in portfolio.assets])
        exposure_overflow = exposure - float(self.checks[self.EXPOSURE_LIMIT])
        if exposure_overflow > 0:
            self._log_warning('Maximum exposure limit exceeded by {0}.'.format(abs(exposure_overflow)))
            return False
        return True

    def _check_negative_units(self, portfolio):
        for asset in portfolio.assets:
            if portfolio.assets[asset][Portfolio.UNITS] < 0:
                self._log_warning('Negative units held of "{}" in proposed portfolio.'.format(asset))
                return False
        return True

    def assess_portfolio(self, portfolio):
        passes = True

        # Standard checks.
        if not self._check_negative_units(portfolio):
            passes = False

        # Custom checks.
        if self.EXPOSURE_LIMIT in self.checks and not self._check_exposure_limit(portfolio):
            passes = False

        return passes
