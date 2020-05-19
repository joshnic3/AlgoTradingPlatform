from library.interfaces.sql_database import query_result_to_dict, Database
from library.bootstrap import Constants
from library.data_loader import DataLoader


class Portfolio:

    SYMBOL = 'symbol'
    ID = 'id'
    UNITS = 'units'
    EXPOSURE = 'current_exposure'
    ASSETS = 'assets'
    PORTFOLIOS = 'portfolios'
    CASH = 'cash'

    def __init__(self, portfolio_id):
        self._db = Database(Constants.configs['db_root_path'], 'algo_trading_platform', Constants.configs['environment'])

        # Load portfolio and asset data.
        portfolio_row = self._db.get_one_row(Portfolio.PORTFOLIOS, 'id="{0}"'.format(portfolio_id))
        portfolios_schema = Constants.configs['tables']['algo_trading_platform'][Portfolio.PORTFOLIOS]
        portfolio_dict = query_result_to_dict([portfolio_row], portfolios_schema)[0]
        asset_rows = self._db.query_table(Portfolio.ASSETS, 'portfolio_id="{0}"'.format(portfolio_dict[Portfolio.ID]))

        self.id = portfolio_dict[Portfolio.ID]
        self.assets = {r[2]: {Portfolio.SYMBOL: r[2], Portfolio.UNITS: int(r[3]), Portfolio.EXPOSURE: float(r[4])}
                       for r in asset_rows}
        self.cash = float(portfolio_dict[Portfolio.CASH])

    def calculate_exposure(self, symbol, portfolio=None):
        # Assume exposure == maximum possible loss from current position.
        assets = portfolio.assets if portfolio else self.assets
        data_loader = DataLoader()
        data_loader.load_latest_ticker(symbol)
        units = assets[symbol][Portfolio.UNITS]
        return units * data_loader.data[DataLoader.LATEST_TICKER][symbol]

    def update_db(self):
        # Update portfolio cash.
        condition = 'id="{}"'.format(self.id)
        self._db.update_value(Portfolio.PORTFOLIOS, Portfolio.CASH, self.cash, condition)

        # Update assets.
        for symbol in self.assets:
            units = int(self.assets[symbol][Portfolio.UNITS])
            self._db.update_value(Portfolio.ASSETS, Portfolio.UNITS, units, 'symbol="{}"'.format(symbol))

            # Calculate and update exposure.
            condition = 'symbol="{}"'.format(symbol)
            self._db.update_value(Portfolio.ASSETS, Portfolio.EXPOSURE, self.calculate_exposure(symbol), condition)

    def sync_with_exchange(self, exchange):
        # Sync weighted cash value for strategy portfolio.
        cash = exchange.get_cash()
        if cash:
            self.cash = cash
        else:
            raise Exception('Could not sync portfolio with exchange.')

        # Sync with exchange too.
        for symbol in self.assets:
            position = exchange.get_position(symbol=self.assets[symbol])
            if position and 'qty' in position:
                self.assets[symbol][Portfolio.UNITS] = int(position['qty'])
            else:
                self.assets[symbol][Portfolio.UNITS] = 0
            self.assets[symbol][Portfolio.EXPOSURE] = self.calculate_exposure(self.assets[symbol][Portfolio.SYMBOL])

    def valuate(self):
        total_asset_value = sum([self.calculate_exposure(a[Portfolio.SYMBOL]) for a in self.assets])
        return total_asset_value + self.cash


