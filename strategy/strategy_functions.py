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
        context.add_signal(symbol, order_type='sell', target_value=latest_value)
    elif latest_value < mean_value - threshold:
        context.add_signal(symbol, order_type='buy', target_value=latest_value)
    else:
        context.add_signal(symbol, order_type='hold')

    # return context.signal.
    return context.signals


def pairs_frequent(context, symbol_a, symbol_b):
    # Short term pairs.
    from statistics import mean

    # Get data.
    one_hour_ago = utils.time_minutes_ago(context, 60)
    mean_relative_difference = context.get_variable('mean_relative_difference')
    a_values = utils.get_values_in_datetime_range(context, symbol_a, one_hour_ago, context.now)
    b_values = utils.get_values_in_datetime_range(context, symbol_b, one_hour_ago, context.now)

    # Record symbol means.
    symbol_a_mean = context.set_variable('symbol_a_mean', mean(a_values))

    # Calculate relative difference.
    relative_differences = [abs(a - b) for a, b in zip(a_values, b_values)]
    current_mean_difference = float(mean(relative_differences))

    # Generate signal.
    threshold = 1.1
    if mean_relative_difference and current_mean_difference > float(mean_relative_difference) * threshold:
        # Decide which ticker is changing the most.
        symbol_a_historical_mean = float(context.get_variable('symbol_a_mean'))
        symbol_b_historical_mean = float(context.get_variable('symbol_b_mean'))
        changing_ticker = symbol_a if symbol_a_mean > symbol_a_historical_mean else symbol_b
        if symbol_a:
            utils.get_latest_value(symbol_a)
            if mean(a_values) > symbol_a_historical_mean:
                context.add_signal(symbol_b, order_type='buy', target_value=utils.get_latest_value(symbol_b))
            else:
                context.add_signal(symbol_b, order_type='sell', target_value=utils.get_latest_value(symbol_b))
        else:
            utils.get_latest_value(symbol_b)
            if mean(b_values) > symbol_b_historical_mean:
                context.add_signal(symbol_a, order_type='buy', target_value=utils.get_latest_value(symbol_a))
            else:
                context.add_signal(symbol_a, order_type='sell', target_value=utils.get_latest_value(symbol_a))
    else:
        # Hold both.
        context.add_signal(symbol_a)
        context.add_signal(symbol_b)

    context.set_variable('mean_relative_difference', current_mean_difference)

    # context.signal.buy(symbol)
    return context.signals


# TODO Implement pairs trading strat function so we can do hourly pairs on target and walmart
    # Will require returning multiple signals
