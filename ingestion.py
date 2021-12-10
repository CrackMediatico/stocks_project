import json
from time import sleep
from requests.structures import CaseInsensitiveDict
import requests as requests
from datetime import datetime
import pandas as pd

config_file_path = 'config.json'
with open(config_file_path, "r") as config_file:
    config_data = json.load(config_file)
    config_file.close()
api_endpoint = config_data['api_endpoint']
api_key = config_data['api_key']
stock_list = config_data['stock_list']
crypto_list = config_data['crypto_list']
currency_list = config_data['currency_list']

# Creación del df para almacenar valores de stocks
stock_fields = ['stock_name', 'stock_symbol', 'date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_value']
stock_df = pd.DataFrame(columns=stock_fields)

stock_daily_params = CaseInsensitiveDict()
stock_daily_params['function'] = 'TIME_SERIES_DAILY'
# stock_daily_params['symbol'] = 'IBM' # set any stock symbol here
stock_daily_params['outputsize'] = 'full' # also 'compact' available
stock_daily_params['datatype'] = 'json' # also 'csv' available
stock_daily_params['apikey'] = api_key

for stock in stock_list:
    stock_daily_params['symbol'] = stock['symbol']
    stock_daily_response = requests.get(url=api_endpoint, params=stock_daily_params)
    stock_daily_json = stock_daily_response.json()
    # Next rows for test & development purposes only. Comment them and uncomment previous ones for production
    # with open('sample_responses/daily.json', "r") as sample_response_daily_file:
    #     stock_daily_json = json.load(sample_response_daily_file)
    #     sample_response_daily_file.close()
    stock_row = dict.fromkeys(stock_fields, None)
    stock_row['stock_name'] = stock['label']
    stock_row['stock_symbol'] = stock_daily_json['Meta Data']['2. Symbol']
    time_series = stock_daily_json['Time Series (Daily)']
    for row_date, row_value in time_series.items():
        stock_row['date'] = datetime.strptime(row_date, '%Y-%m-%d').date()
        stock_row['open_value'] = float(row_value['1. open'])
        stock_row['high_value'] = float(row_value['2. high'])
        stock_row['low_value'] = float(row_value['3. low'])
        stock_row['close_value'] = float(row_value['4. close'])
        stock_row['volume_value'] = int(row_value['5. volume'])
        stock_df = stock_df.append(stock_row, ignore_index=True)
    sleep(5)
    stock_df.to_csv('data/stocks.csv', index=False)
print('Stock Data downloaded')

# CRYPTOS
# Creación del df para almacenar valores de cryptos
crypto_fields = ['crypto_name', 'crypto_symbol', 'date', 'open_value', 'high_value', 'low_value', 'close_value', 'volume_value']
crypto_df = pd.DataFrame(columns=crypto_fields)

crypto_daily_params = CaseInsensitiveDict()
crypto_daily_params['function'] = 'DIGITAL_CURRENCY_DAILY'
# crypto_daily_params['symbol'] = 'BTC' # set any crypto symbol here
crypto_daily_params['market'] = 'USD' # set any market symbol here
crypto_daily_params['datatype'] = 'json' # also 'csv' available
crypto_daily_params['apikey'] = api_key

for crypto in crypto_list:
    crypto_daily_params['symbol'] = crypto['symbol']
    crypto_daily_response = requests.get(url=api_endpoint, params=crypto_daily_params)
    crypto_daily_json = crypto_daily_response.json()
    crypto_row = dict.fromkeys(crypto_fields, None)
    crypto_row['crypto_name'] = crypto['label']
    crypto_row['crypto_symbol'] = crypto_daily_json['Meta Data']['2. Digital Currency Code']
    time_series = crypto_daily_json['Time Series (Digital Currency Daily)']
    for row_date, row_value in time_series.items():
        crypto_row['date'] = datetime.strptime(row_date, '%Y-%m-%d').date()
        crypto_row['open_value'] = float(row_value['1a. open (USD)'])
        crypto_row['high_value'] = float(row_value['2a. high (USD)'])
        crypto_row['low_value'] = float(row_value['3a. low (USD)'])
        crypto_row['close_value'] = float(row_value['4a. close (USD)'])
        crypto_row['volume_value'] = float(row_value['5. volume'])
        crypto_df = crypto_df.append(crypto_row, ignore_index=True)
    sleep(5)
    crypto_df.to_csv('data/cryptos.csv', index=False)
print('Crypto Data downloaded')

# CURRENCIES
# Creación del df para almacenar valores de currencies
currency_fields = ['currency_name', 'currency_symbol', 'date', 'open_value', 'high_value', 'low_value', 'close_value']
currency_df = pd.DataFrame(columns=currency_fields)

currency_daily_params = CaseInsensitiveDict()
currency_daily_params['function'] = 'FX_DAILY'
# currency_daily_params['from_symbol'] = 'EUR'  # set any currency symbol here
currency_daily_params['to_symbol'] = 'USD'  # set any currency symbol here
currency_daily_params['outputsize'] = 'full'  # also 'compact' available
currency_daily_params['datatype'] = 'json'  # also 'csv' available
currency_daily_params['apikey'] = api_key

for currency in currency_list:
    currency_daily_params['from_symbol'] = currency['symbol']
    currency_daily_response = requests.get(url=api_endpoint, params=currency_daily_params)
    currency_daily_json = currency_daily_response.json()
    currency_row = dict.fromkeys(currency_fields, None)
    currency_row['currency_name'] = currency['label']
    currency_row['currency_symbol'] = currency_daily_json['Meta Data']['2. From Symbol']
    time_series = currency_daily_json['Time Series FX (Daily)']
    for row_date, row_value in time_series.items():
        currency_row['date'] = datetime.strptime(row_date, '%Y-%m-%d').date()
        currency_row['open_value'] = float(row_value['1. open'])
        currency_row['high_value'] = float(row_value['2. high'])
        currency_row['low_value'] = float(row_value['3. low'])
        currency_row['close_value'] = float(row_value['4. close'])
        currency_df = currency_df.append(currency_row, ignore_index=True)
    sleep(5)
    currency_df.to_csv('data/currencies.csv', index=False)
print('Currency Data downloaded')

# FEDERAL FUNDS RATE
# Creación del df para almacenar valores de Federal Funds Rate
ffr_fields = ['date', 'ffr_value']
ffr_df = pd.DataFrame(columns=ffr_fields)

ffr_params = CaseInsensitiveDict()
ffr_params['function'] = 'FEDERAL_FUNDS_RATE'
ffr_params['interval'] = 'daily'  # set any currency symbol here
ffr_params['datatype'] = 'json'  # also 'csv' available
ffr_params['apikey'] = api_key

ffr_response = requests.get(url=api_endpoint, params=ffr_params)
ffr_json = ffr_response.json()
ffr_row = dict.fromkeys(ffr_fields, None)
time_series = ffr_json['data']
for element in time_series:
    ffr_row['date'] = datetime.strptime(element['date'], '%Y-%m-%d').date()
    ffr_row['ffr_value'] = float(element['value'])
    ffr_df = ffr_df.append(ffr_row, ignore_index=True)
sleep(5)
ffr_df.to_csv('data/federal_funds_rate.csv', index=False)
print('Federal Funds Rate Data downloaded')
