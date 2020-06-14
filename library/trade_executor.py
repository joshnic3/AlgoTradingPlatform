import copy
import time

from library.bootstrap import Constants
from library.interfaces.exchange import AlpacaInterface as Alpaca
from library.strategy.exposure_manager import ExposureManager
from library.strategy.portfolio import Portfolio
from library.strategy.signal import Signal


class TradeExecutor:

    def __init__(self, strategy, exchange):
        self._default_no_of_units = 1

        self.strategy = strategy
        self.risk_profile = strategy.risk_profile
        self.portfolio = self.strategy.portfolio
        self.exchange = exchange

    def _determine_units_to_trade(self, signal):
        # TODO Could also use a portfolio manager (maybe abstract from exposure manager?).
        #   keeps cash level within its allocation.
        #   maintain minimum position.

        # Dynamically suggest units based on exposure rules.
        if 'manage_exposure' in self.strategy.execution_options:
            exposure_manager = ExposureManager(self.strategy, default_units=self._default_no_of_units)
            units = exposure_manager.units_to_trade(signal) if exposure_manager else self._default_no_of_units
        else:
            units = self._default_no_of_units

        # Ensure we never propose trading negative trades.
        if units < 0:
            Constants.log.error('Trade executor accidentally proposed trading negative units, overriding to zero.')
            units = 0
        return units

    def generate_trades_from_signals(self, signals):
        # Generates trades from signals using the strategy's risk profile and and execution options.

        self.portfolio.sync_with_exchange(self.exchange)

        # Create a copy of portfolio.
        potential_portfolio = copy.copy(self.portfolio)

        # Propose trades.
        trades = []
        for signal in signals:
            if signal.signal != Signal.HOLD:
                # Decide how many units to trade.
                units = self._determine_units_to_trade(signal)

                # Make potential portfolio changes for sell order.
                # TODO May need to consider exchange commissions here.
                if signal.signal == Signal.SELL:
                    potential_portfolio.cash += units * signal.target_value
                    potential_portfolio.assets[signal.symbol][Portfolio.UNITS] -= units

                # Make potential portfolio changes for buy order.
                if signal.signal == Signal.BUY:
                    potential_portfolio.cash -= units * signal.target_value
                    potential_portfolio.assets[signal.symbol][Portfolio.UNITS] += units

                # Calculate total potential exposure.
                potential_exposure = self.portfolio.calculate_exposure(signal.symbol, potential_portfolio)
                potential_portfolio.assets[signal.symbol][Portfolio.EXPOSURE] = potential_exposure

                # Only append trade if current state of the potential portfolio meets the strategy's risk profile.
                if self.risk_profile.assess_portfolio(potential_portfolio):
                    trades.append((signal.signal, signal.symbol, units, signal.target_value))

        if trades:
            Constants.log.info('Generated {0} trade(s) from {1} signals.'.format(len(trades), len(signals)))
        return trades

    def execute_trades(self, requested_trades):
        # Return actual achieved trades, Not all trades will be fulfilled.
        executed_trade_ids = []
        for trade in requested_trades:
            signal, symbol, units, target_value = trade
            if units:
                if signal == Signal.SELL:
                    executed_trade_ids.append(self.exchange.ask(symbol, units))
                if signal == Signal.BUY:
                    executed_trade_ids.append(self.exchange.bid(symbol, units))
            else:
                Constants.log.warning('Rejecting invalid trade.'.format(trade))
        return executed_trade_ids

    def process_executed_trades(self, executed_trade_ids, suppress_log=False):
        processed_trades = []
        for order_id in executed_trade_ids:
            data = self.exchange.get_order_data(order_id)
            status = data[Alpaca.STATUS]

            # Wait for order to fill.
            while status == Alpaca.NEW_ORDER or status == Alpaca.PARTIALLY_FILLED_ORDER:
                time.sleep(0.5)
                data = self.exchange.get_order_data(order_id)
                status = data[Alpaca.STATUS]

            # Create order tuple with trade results.
            if status == Alpaca.FILLED_ORDER:
                trade = (data[Alpaca.ORDER_SIDE], data[Portfolio.SYMBOL], int(data[Alpaca.FILLED_UNITS]),
                         float(data[Alpaca.FILLED_MEAN_PRICE]))

                # Add to processed trades list.
                processed_trades.append(trade)
            else:
                if not suppress_log:
                    Constants.log.warning('Order {0} [{1} * {2}] failed. status: {3}'.format(order_id,
                                                                                             data[Alpaca.UNITS],
                                                                                             data[Alpaca.SYMBOL],
                                                                                             status))
        return processed_trades

    def update_portfolio_db(self):
        self.portfolio.sync_with_exchange(self.exchange)
        self.portfolio.update_db()
        Constants.log.info('Updated portfolio in database.')
