import datetime


def get_latest_value(context, symbol):
    return float(context.data['ticker'][symbol][-1][1])


def get_values_in_datetime_range(context, symbol, from_after, until_before):
    return [price for date_time, price in context.data['ticker'][symbol] if until_before > date_time > from_after]


def get_todays_values(context, symbol):
    now = context.now.strftime('%Y%m%d%H%M%S')
    this_morning = '{0}0000'.format(now[8:])
    return get_values_in_datetime_range(context, symbol, this_morning, now)


def get_live_value(context, symbol):
    result = context.ds.request_tickers([symbol, 'JPM'])
    return float(result[symbol])


def time_minutes_ago(context, minutes):
    return context.now - datetime.timedelta(minutes=minutes)

