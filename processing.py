import pandas as pd

processed_df = pd.DataFrame(columns=['date'])

# Stocks processing
stock_df = pd.read_csv('data/stocks.csv', header='infer', parse_dates=True, infer_datetime_format=True)
for index, row in stock_df.iterrows():
    row_data = pd.Series()
    stock_name = row['stock_name'].replace(' ', '_')
    date = row['date']
    index_series = 3
    for column in row.iloc[3:]:
        column_name = stock_name + '_' + row.index[index_series]
        index_series += 1
        if date in processed_df['date'].values:
            processed_df.at[processed_df.index[processed_df['date'] == date], column_name] = column
        else:
            row_data['date'] = date
            row_data[column_name] = column
            processed_df = processed_df.append(row_data, ignore_index=True)
    print(f'Row read: {date} - {stock_name}')
print('Stocks loaded')

# Cryptos processing
cryptos_df = pd.read_csv('data/cryptos.csv', header='infer', parse_dates=True, infer_datetime_format=True)
for index, row in cryptos_df.iterrows():
    row_data = pd.Series()
    crypto_name = row['crypto_name'].replace(' ', '_')
    date = row['date']
    index_series = 3
    for column in row.iloc[3:]:
        column_name = crypto_name + '_' + row.index[index_series]
        index_series += 1
        if date in processed_df['date'].values:
            processed_df.at[processed_df.index[processed_df['date'] == date], column_name] = column
        else:
            row_data['date'] = date
            row_data[column_name] = column
            processed_df = processed_df.append(row_data, ignore_index=True)
    print(f'Row read: {date} - {crypto_name}')
print('Cryptos loaded')

# Currencies processing
currencies_df = pd.read_csv('data/currencies.csv', header='infer', parse_dates=True, infer_datetime_format=True)
for index, row in currencies_df.iterrows():
    row_data = pd.Series()
    currency_name = row['currency_name'].replace(' ', '_')
    date = row['date']
    index_series = 3
    for column in row.iloc[3:]:
        column_name = currency_name + '_' + row.index[index_series]
        index_series += 1
        if date in processed_df['date'].values:
            processed_df.at[processed_df.index[processed_df['date'] == date], column_name] = column
        else:
            row_data['date'] = date
            row_data[column_name] = column
            processed_df = processed_df.append(row_data, ignore_index=True)
    print(f'Row read: {date} - {currency_name}')
print('Currencies loaded')

# Federal Funds Rate processing
ffr_df = pd.read_csv('data/federal_funds_rate.csv', header='infer', parse_dates=True, infer_datetime_format=True)
for index, row in ffr_df.iterrows():
    row_data = pd.Series()
    date = row['date']
    column_name = 'federal_funds_rate'
    index_series = 3
    for column in row.iloc[1:]:
        index_series += 1
        if date in processed_df['date'].values:
            processed_df.at[processed_df.index[processed_df['date'] == date], column_name] = column
        else:
            row_data['date'] = date
            row_data[column_name] = column
            processed_df = processed_df.append(row_data, ignore_index=True)
    print(f'Row read: {date} - Federal Funds Rate')
print('Federal Funds Rate loaded')

processed_df.to_csv('processed_data/data_input.csv', index=False)






