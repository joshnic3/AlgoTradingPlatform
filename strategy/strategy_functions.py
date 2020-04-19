import strategy.strategy_utils as utils


def basic(context, symbol):
    from statistics import mean

    # Fetch static all data together.
    eight_hours_ago = utils.time_minutes_ago(context, 60*8)
    previous_values = utils.get_values_in_datetime_range(context, symbol, eight_hours_ago, context.now)
    latest_value = utils.get_live_value(context, symbol)

    # Calculate values.
    mean_value = mean(previous_values)
    threshold = mean_value * 0.02

    # Generate signal.
    if latest_value > mean_value + threshold:
        context.signal.sell(symbol, latest_value)
    elif latest_value < mean_value - threshold:
        context.signal.buy(symbol, latest_value)
    else:
        context.signal.hold(symbol)

    # return context.signal.
    current_value = utils.get_live_value(context, symbol)
    context.signal.buy(symbol, current_value)
    return context.signal


def always_buy(context, symbol):
    context.signal.buy(symbol)
    return context.signal
