from library.bootstrap import Constants
from library.strategy import Portfolio, Signal
from library.data_loader import MarketDataLoader
from library.strategy import RiskProfile


class ExposureManager:

    def __init__(self, strategy, default_units=1):
        self._risk_profile = strategy.risk_profile
        self._portfolio = strategy.portfolio
        self._data_loader = MarketDataLoader()
        self._default_units = default_units

    def _update_portfolio_units(self, symbol, difference_in_units):
        asset = self._portfolio.assets[symbol]
        asset[Portfolio.UNITS] += difference_in_units
        asset[Portfolio.EXPOSURE] = self._portfolio.calculate_exposure(symbol, portfolio=self._portfolio)

    def _units_to_balance_exposure(self, target_exposure, symbol):
        self._data_loader.load_latest_ticker(symbol)
        current_value = self._data_loader.data[MarketDataLoader.LATEST_TICKER][symbol]
        current_exposure = self._portfolio.assets[symbol][Portfolio.EXPOSURE]
        target_value = abs(target_exposure - current_exposure)
        return int(target_value / current_value)

    def units_to_trade(self, signal):
        # Calculate common for values portfolio, each section will return after it updates the portfolio tracker.
        portfolio_exposure = sum([self._portfolio.assets[a][Portfolio.EXPOSURE] for a in self._portfolio.assets])
        portfolio_exposure_limit = float(self._risk_profile.checks[RiskProfile.EXPOSURE_LIMIT])
        portfolio_mean_exposure = portfolio_exposure / len(self._portfolio.assets)

        # Calculate common for values asset.
        units_held = self._portfolio.assets[signal.symbol][Portfolio.UNITS]
        if units_held < 0:
            Constants.log.warning('Portfolio has negative units of "{}", defaulting trade to 0 units. '.format(
                signal.symbol))
            return 0
        exposure = self._portfolio.assets[signal.symbol][Portfolio.EXPOSURE]

        # If portfolio is over exposed sell as much as you can to get under the limit.
        if RiskProfile.EXPOSURE_LIMIT in self._risk_profile.checks and portfolio_exposure > portfolio_exposure_limit:
            if signal.signal == Signal.SELL:
                # Load data
                self._data_loader.load_latest_ticker(signal.symbol)
                current_value = self._data_loader.data[MarketDataLoader.LATEST_TICKER][signal.symbol]

                # Will sell as many units as it can to get below exposure limit.
                units = int(exposure / current_value)
                units = units if units <= units_held else units_held

                # Update object instance of portfolio.
                self._update_portfolio_units(signal.symbol, -units)
                Constants.log.warning('Exposure Manager: Portfolio over exposed, selling {0} {1} units to compensate.'
                                      .format(units, signal.symbol))
                return units

        # Balance exposure over multiple assets. Will limit to selling now. I want to be in full control of buying.
        if signal.signal == Signal.SELL and portfolio_exposure > portfolio_mean_exposure:
            # How many units do I need to sell to match exposure with mean?
            units = self._units_to_balance_exposure(portfolio_mean_exposure, signal.symbol)

            # Floor to default units if proposes less.
            units = units if units > self._default_units else self._default_units

            # Only sell units held if more required.
            units = units if units < units_held else units_held

            # Update object instance of portfolio.
            self._update_portfolio_units(signal.symbol, -units)

            if units != self._default_units:
                Constants.log.info('Exposure Manager: Selling more units ({0}) of {1} to balance exposure'
                                   .format(units, signal.symbol))
            return units

        return self._default_units
