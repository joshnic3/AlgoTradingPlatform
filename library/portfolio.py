from library.bootstrap import Constants
from library.data_loader import MarketDataLoader
from library.interfaces.sql_database import Database, query_result_to_dict


class Portfolio:

    SYMBOL = 'symbol'
    ID = 'id'
    UNITS = 'units'
    EXPOSURE = 'current_exposure'
    ASSETS = 'assets'
    PORTFOLIOS = 'portfolios'
    CASH = 'cash'

    def __init__(self, portfolio_id, db):
        self._db = db

        # Load portfolio and asset data.
        portfolio_row = self._db.get_one_row(Portfolio.PORTFOLIOS, '{}="{}"'.format(Portfolio.ID, portfolio_id))
        portfolios_schema = Constants.configs['tables'][Constants.db_name][Portfolio.PORTFOLIOS]
        portfolio_dict = query_result_to_dict([portfolio_row], portfolios_schema)[0]
        asset_rows = self._db.query_table(Portfolio.ASSETS, 'portfolio_id="{0}"'.format(portfolio_dict[Portfolio.ID]))

        self.id = portfolio_dict[Portfolio.ID]
        self.assets = {r[2]: {Portfolio.SYMBOL: r[2], Portfolio.UNITS: int(r[3]), Portfolio.EXPOSURE: float(r[4])}
                       for r in asset_rows}
        self.cash = float(portfolio_dict[Portfolio.CASH])

    def calculate_exposure(self, symbol, portfolio=None):
        # Assume exposure == maximum possible loss from current position.
        data_loader = MarketDataLoader()
        data_loader.load_latest_ticker(symbol)
        assets = portfolio.assets if portfolio else self.assets
        return assets[symbol][Portfolio.UNITS] * data_loader.data[MarketDataLoader.LATEST_TICKER][symbol]

    def update_db(self):
        # Update portfolio cash.
        condition = '{}="{}"'.format(Portfolio.ID, self.id)
        self._db.update_value(Portfolio.PORTFOLIOS, Portfolio.CASH, self.cash, condition)

        # Update assets.
        for symbol in self.assets:
            units = int(self.assets[symbol][Portfolio.UNITS])
            self._db.update_value(Portfolio.ASSETS, Portfolio.UNITS, units, '{}="{}"'.format(Portfolio.SYMBOL, symbol))

            # Calculate and update exposure.
            condition = 'symbol="{}"'.format(symbol)
            self._db.update_value(Portfolio.ASSETS, Portfolio.EXPOSURE, self.calculate_exposure(symbol), condition)

    def sync_with_exchange(self, exchange):
        # Not sure this works, atleast not when called from webservice/
        # Sync weighted cash value for strategy portfolio.
        cash = exchange.get_cash()
        if cash:
            self.cash = cash
        else:
            raise Exception('Could not sync portfolio with exchange.')

        # Sync with exchange too.
        for symbol in self.assets:
            asset = self.assets[symbol]
            position = exchange.get_position(symbol=asset[Portfolio.SYMBOL])
            if position and 'qty' in position:
                asset[Portfolio.UNITS] = int(position['qty'])
            else:
                asset[Portfolio.UNITS] = 0
            asset[Portfolio.EXPOSURE] = self.calculate_exposure(asset[Portfolio.SYMBOL])

    def valuate(self):
        total_asset_value = sum([self.calculate_exposure(s) for s in self.assets])
        return total_asset_value + self.cash