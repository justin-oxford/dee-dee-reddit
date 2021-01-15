import finnhub
import time
import json
import datetime

# Configure API key
configuration_1 = finnhub.Configuration(
    api_key={
        'token': 'a'
    }
)

configuration_2 = finnhub.Configuration(
    api_key={
        'token': 'a'
    }
)

# Connect to the two FinnHub Clients
finnhub_client_1 = finnhub.DefaultApi(finnhub.ApiClient(configuration_1))
finnhub_client_2 = finnhub.DefaultApi(finnhub.ApiClient(configuration_2))


# get the finnhub data for the requested stock symbol
def fetch_finnhub_data(reddit_all_mentions_raw_data):
    finnhub_return_data = {}
    for symbol_mention in reddit_all_mentions_raw_data:
        try:
            print('finnhub fetch: ' + str(symbol_mention))
            finnhub_return_data[symbol_mention] = {}
            finnhub_return_data[symbol_mention]['symbol'] = symbol_mention

            # Get price data
            this_price = json.loads(str(finnhub_client_1.quote(symbol_mention)).replace("'", '"'))
            finnhub_return_data[symbol_mention]['price'] = this_price["c"]
            finnhub_return_data[symbol_mention]['price_high'] = this_price["h"]
            finnhub_return_data[symbol_mention]['price_low'] = this_price["l"]
            finnhub_return_data[symbol_mention]['price_open'] = this_price["o"]
            finnhub_return_data[symbol_mention]['price_pvcl'] = this_price["pc"]

            # Get Candle Data for last 5 minutes
            current_time = round(time.time())
            past_time = round(current_time - (60 * 60 * 72))
            this_candles = json.loads(str(finnhub_client_2.stock_candles(symbol_mention, "1", past_time, current_time)).replace("'", '"'))
            finnhub_return_data[symbol_mention]['volume_1'] = 0
            finnhub_return_data[symbol_mention]['volume_2'] = 0
            finnhub_return_data[symbol_mention]['volume_3'] = 0
            finnhub_return_data[symbol_mention]['volume_4'] = 0
            finnhub_return_data[symbol_mention]['high_1'] = 0
            finnhub_return_data[symbol_mention]['low_1'] = 0
            finnhub_return_data[symbol_mention]['high_2'] = 0
            finnhub_return_data[symbol_mention]['low_2'] = 0
            if this_candles["s"] == "ok":
                finnhub_return_data[symbol_mention]['volume_1'] = this_candles["v"][-1]
                finnhub_return_data[symbol_mention]['volume_2'] = this_candles["v"][-2]
                finnhub_return_data[symbol_mention]['volume_3'] = this_candles["v"][-3]
                finnhub_return_data[symbol_mention]['volume_4'] = this_candles["v"][-4]
                finnhub_return_data[symbol_mention]['high_1'] = this_candles["h"][-1]
                finnhub_return_data[symbol_mention]['low_1'] = this_candles["l"][-1]
                finnhub_return_data[symbol_mention]['high_2'] = this_candles["h"][-2]
                finnhub_return_data[symbol_mention]['low_2'] = this_candles["l"][-2]
        except Exception as err:
            print("**ERROR** in Finnhub Get. " + str(symbol_mention) + " not polled.")
            print(str(err))
            pass
        # Sleep to prevent the API from rejecting. Might look into setting up multiple API keys in this case.
        time.sleep(1)
    return finnhub_return_data
