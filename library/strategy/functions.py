import datetime
import pytz

from statistics import mean
from library.data_loader import MarketDataLoader
from library.bootstrap import Constants


TEST = 'test'
PAIRS = 'pairs'
LIST = [TEST, PAIRS]


def _get_latest_value(context, symbol):
    return float(context.data[MarketDataLoader.TICKER][symbol][-1][1])


def _get_values_in_datetime_range(context, symbol, from_after, until_before):
    data = []
    for now_datetime, price, volume in context.data[MarketDataLoader.TICKER][symbol]:
        now_datetime = now_datetime.replace(tzinfo=pytz.timezone(Constants.TIME_ZONE))
        if until_before > now_datetime > from_after:
            data.append(price)
    return data


def _time_minutes_ago(context, minutes):
    return context.now - datetime.timedelta(minutes=minutes)

# ---------------------------------------------------------------------------------------------------------------------
# Strategy Functions


def test(context, parameters):
    context.add_signal(parameters['symbol'], order_type='hold')
    return context.signals


def pairs(context, parameters):
    # Get all ticker data for both symbols from the last hour.
    one_hour_ago = _time_minutes_ago(context, int(parameters['minutes_to_look_back']))
    a_values = _get_values_in_datetime_range(context, parameters['symbol_a'], one_hour_ago, context.now)
    b_values = _get_values_in_datetime_range(context, parameters['symbol_b'], one_hour_ago, context.now)

    # Get strategy variables.
    mean_relative_difference = context.get_variable('mean_relative_difference')
    a_mean_value = float(context.get_variable('a_mean_value', default=0.0))
    b_mean_value = float(context.get_variable('b_mean_value', default=0.0))

    # Calculate relative difference for last hour.
    relative_differences = [abs(a - b) for a, b in zip(a_values, b_values)]
    current_mean_difference = float(mean(relative_differences))

    # Generate signal.
    condition = mean_relative_difference and (float(current_mean_difference) * float(parameters['threshold']) > float(mean_relative_difference))
    condition = mean_relative_difference > parameters['value_threshold'] if 'value_threshold' in parameters else condition
    if condition:
        # Decide which ticker is differing from the trend.
        # +ve = up, -ve = down
        a_change_direction = mean(a_values) - a_mean_value
        b_change_direction = mean(b_values) - b_mean_value

        changing_ticker = parameters['symbol_a'] if abs(a_change_direction) > abs(b_change_direction) else parameters['symbol_b']
        if changing_ticker == parameters['symbol_a']:
            # If a's value is rising buy b, if a's value is dropping sell b.
            order_type = 'buy' if a_change_direction > 0 else 'sell'
            context.add_signal(parameters['symbol_b'], order_type=order_type, target_value=_get_latest_value(context, parameters['symbol_b']))
        else:
            # If b's value is rising buy a, if b's value is dropping sell a.
            order_type = 'buy' if b_change_direction > 0 else 'sell'
            context.add_signal(parameters['symbol_a'], order_type=order_type, target_value=_get_latest_value(context, parameters['symbol_a']))
    else:
        # Hold both.
        context.add_signal(parameters['symbol_a'])
        context.add_signal(parameters['symbol_b'])

    # Could do something sexy like balance out exposure by selling assets.

    # Update context variable.
    context.set_variable('mean_relative_difference', current_mean_difference)
    context.set_variable('a_mean_value', mean(a_values))
    context.set_variable('b_mean_value', mean(b_values))
    return context.signals


def test(context, parameters):

    context.add_signal(parameters['symbol_a'])

    return context.signals
