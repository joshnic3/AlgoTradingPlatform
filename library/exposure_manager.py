from library.bootstrap import Constants
from library.strategy import Portfolio, Signal
from library.data_loader import DataLoader


class ExposureManager:

    def __init__(self, strategy, default_units=1):
        self._risk_profile = strategy.risk_profile
        self._portfolio = strategy.portfolio
        self._data_loader = DataLoader()
        self._default_units = default_units

    def _update_portfolio_units(self, symbol, difference_in_units):
        asset = self._portfolio.assets[symbol]
        asset[Portfolio.UNITS] += difference_in_units
        asset[Portfolio.EXPOSURE] = self._portfolio.calculate_exposure(symbol, portfolio=self._portfolio)

    def _balance_exposure(self, mean_exposure, symbol):
        self._data_loader.load_latest_ticker(symbol)
        current_value = self._data_loader.data[DataLoader.LATEST_TICKER][symbol]
        portfolio_exposure = sum([self._portfolio.assets[a][Portfolio.EXPOSURE] for a in self._portfolio.assets])

        current_exposure = self._portfolio.assets[symbol][Portfolio.EXPOSURE]
        target_value = abs(current_exposure - mean_exposure)
        units = int(target_value / current_value)
        return units if units > self._default_units else self._default_units

    def suggest_units_to_trade(self, signal):
        # If portfolio is over exposed sell as much as you can to get under the limit.
        portfolio_exposure = sum([self._portfolio.assets[a][Portfolio.EXPOSURE] for a in self._portfolio.assets])
        if 'max_exposure' in self._risk_profile and portfolio_exposure > float(self._risk_profile['max_exposure']):
            if signal.signal == Signal.SELL:
                # Load data
                self._data_loader.load_latest_ticker(signal.symbol)
                current_value = self._data_loader.data[DataLoader.LATEST_TICKER][signal.symbol]

                # Will sell as many units as it can to get below exposure limit.
                units_held = self._portfolio.assets[signal.symbol][Portfolio.UNITS]
                exposure = self._portfolio.assets[signal.symbol][Portfolio.EXPOSURE]
                units = int(exposure / current_value)
                units = units if units <= units_held else units_held

                # Update object instance of portfolio.
                self._update_portfolio_units(signal.symbol, -units)

                Constants.log.warning('Portfolio is over exposed, selling {0} units of {1} to compensate.'
                                      .format(units, signal.symbol))
                return units

        # Balance exposure over multiple assets.
        mean_exposure = portfolio_exposure / len(self._portfolio.assets)
        if signal.signal == Signal.SELL and self._portfolio.assets[signal.symbol][Portfolio.EXPOSURE] > mean_exposure:
            # How many units do I need to sell to match exposure with mean?
            units = self._balance_exposure(mean_exposure, signal.symbol)

            # Update object instance of portfolio.
            self._update_portfolio_units(signal.symbol, -units)

            if units != self._default_units:
                Constants.log.info('Selling more units ({0}) of {1} to balance exposure'.format(units, signal.symbol))
            return units

        if signal.signal == Signal.BUY and self._portfolio.assets[signal.symbol][Portfolio.EXPOSURE] < mean_exposure:
            # How many units can I buy with exposure remaining below the mean?
            units = self._balance_exposure(mean_exposure, signal.symbol)

            # Update object instance of portfolio.
            self._update_portfolio_units(signal.symbol, units)

            if units != self._default_units:
                Constants.log.info('Buying more units ({0}) of {1} to balance exposure'.format(units, signal.symbol))
            return units

        return self._default_units
