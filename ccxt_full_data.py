import ccxt
import pprint
import pandas as pd
from datetime import datetime as dt


def get_available_exchanges():
    ex = ccxt.exchanges
    pprint.pprint(ex)


def get_available_pairs_from_exchange(exchange):
    exchange = exchange.lower()

    try:
        get_exchange = getattr(ccxt, exchange)()
    except AttributeError:
        raise f'Exchange "{exchange}" not found.'
    else:
        pairs = get_exchange.load_markets()
        pairs = list(pairs.keys())
        pprint.pprint(pairs)


def get_data(symbol, exchange, timeframe, fromdate, todate):
    exchange = getattr(ccxt, exchange)()
    since = exchange.parse8601(fromdate)
    data = exchange.fetch_ohlcv(symbol, timeframe, since=since, limit=1000)

    header = ['Timestamp', 'Open', 'High', 'Low', 'Close', 'Volume']
    df = pd.DataFrame(data, columns=header).set_index('Timestamp')

    filename = symbol.replace("/", "") + '-' + timeframe
    df.to_csv(filename)
    df2 = pd.read_csv(filename, parse_dates=['Timestamp'])

    last_candle = 999
    last_timestamp = (int(df2.at[0, 'Timestamp']))
    last_datetime = dt.utcfromtimestamp(last_timestamp / 1000)
    ending_date = dt.strptime(todate, '%Y-%m-%d %H:%M:%S')

    while not ending_date <= last_datetime:
        last_candle += 999

        if timeframe == '15s':
            last_timestamp = last_timestamp + 15000000
        elif timeframe == '1m':
            last_timestamp = last_timestamp + 60000000
        elif timeframe == '5m':
            last_timestamp = last_timestamp + 300000000
        elif timeframe == '15m':
            last_timestamp = last_timestamp + 900000000
        elif timeframe == '30m':
            last_timestamp = last_timestamp + 1800000000
        elif timeframe == '1h':
            last_timestamp = last_timestamp + 3600000000
        elif timeframe == '4h':
            last_timestamp = last_timestamp + 14400000000
        elif timeframe == '6h':
            last_timestamp = last_timestamp + 21600000000
        elif timeframe == '12h':
            last_timestamp = last_timestamp + 43200000000
        elif timeframe == '1d':
            last_timestamp = last_timestamp + 86400000000
        elif timeframe == '1w':
            last_timestamp = last_timestamp + 86400000000 * 7

        data = exchange.fetch_ohlcv(symbol, timeframe, since=last_timestamp, limit=1000)
        df2 = pd.DataFrame(data, columns=header).set_index('Timestamp')
        df2.to_csv(filename, mode='a', header=False)
        last_datetime = dt.utcfromtimestamp(last_timestamp / 1000)

