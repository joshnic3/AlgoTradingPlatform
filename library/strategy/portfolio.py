from library.bootstrap import Constants
from library.data_loader import MarketDataLoader
from library.interfaces.sql_database import query_result_to_dict


class Portfolio:
    _TABLE = 'portfolios'
    _ASSETS_TABLE = 'assets'

    # Portfolio constants.
    CASH = 'cash'
    VALUE = 'value'
    ALLOCATION = 'allocation'

    # Asset constants.
    SYMBOL = 'symbol'
    ID = 'id'
    UNITS = 'units'
    EXPOSURE = 'current_exposure'

    def __init__(self, portfolio_id, db):
        self._db = db

        # Load portfolio and asset data.
        portfolio_row = self._db.get_one_row(self._TABLE, '{}="{}"'.format(self.ID, portfolio_id))
        portfolios_schema = Constants.configs['tables'][Constants.DB_NAME][self._TABLE]
        portfolio_dict = query_result_to_dict([portfolio_row], portfolios_schema)[0]
        asset_rows = self._db.query_table(self._ASSETS_TABLE, 'portfolio_id="{0}"'.format(portfolio_dict[self.ID]))

        self.id = portfolio_dict[Portfolio.ID]
        self.assets = {r[2]: {self.SYMBOL: r[2], Portfolio.UNITS: int(r[3]), self.EXPOSURE: float(r[4])}
                       for r in asset_rows}
        self.cash = float(portfolio_dict[self.CASH])
        self.allocation_percentage = float(portfolio_dict[self.ALLOCATION])

    def calculate_exposure(self, symbol, portfolio=None):
        # Assume exposure == maximum possible loss from current position.
        data_loader = MarketDataLoader()
        data_loader.load_latest_ticker(symbol)
        if Constants.debug:
            data_loader.report_warnings()
        if MarketDataLoader.LATEST_TICKER in data_loader.data:
            assets = portfolio.assets if portfolio else self.assets
            return assets[symbol][Portfolio.UNITS] * data_loader.data[MarketDataLoader.LATEST_TICKER][symbol]
        else:
            return 0

    def update_db(self):
        # Update portfolio cash.
        condition = '{}="{}"'.format(self.ID, self.id)
        self._db.update_value(self._TABLE, self.CASH, self.cash, condition)

        # Update assets.
        for symbol in self.assets:
            units = int(self.assets[symbol][self.UNITS])
            self._db.update_value(self._ASSETS_TABLE, self.UNITS, units, '{}="{}"'.format(self.SYMBOL, symbol))

            # Calculate and update exposure.
            condition = 'symbol="{}"'.format(symbol)
            self._db.update_value(self._ASSETS_TABLE, self.EXPOSURE, self.calculate_exposure(symbol), condition)

    def sync_with_exchange(self, exchange):
        # Get total cash value on exchange profile.
        total_exchange_cash = exchange.get_cash()

        # Warn if cash value in database is greater than the cash value on the exchange.
        #   This is a "hail Mary" check as it does not consider multiple portfolios on one exchange profile.
        #   A lot has gone wrong if you see this error :/ .
        if self.cash > total_exchange_cash:
            Constants.log.warning('Portfolio cash value is greater than total cash value on exchange.')

        # Calculate cash allocated to strategy.
        allocated_cash = self.allocation_percentage * total_exchange_cash
        if self.cash > allocated_cash:
            Constants.log.warning('Portfolio cash value is greater than its allocation allows.')

        # Sync assets with exchange.
        for symbol in self.assets:
            asset = self.assets[symbol]
            position = exchange.get_position(symbol=asset[self.SYMBOL])
            if position and 'qty' in position:
                asset[Portfolio.UNITS] = int(position['qty'])
            else:
                asset[self.UNITS] = 0
            asset[self.EXPOSURE] = self.calculate_exposure(asset[self.SYMBOL])

    def valuate(self):
        total_asset_value = sum([self.calculate_exposure(s) for s in self.assets])
        return total_asset_value + self.cash
