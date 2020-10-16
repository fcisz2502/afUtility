import pandas as pd
import os
from pandas.tseries.offsets import Minute
from datetime import datetime, time, timedelta
import jqdatasdk as jq
from afUtility.keyInfo import jqAccount, jqPassword, cwhEmail
from afUtility.mailing import Email
jq.auth(jqAccount, jqPassword)


# -----------------------------------------------------------------------------
def getJointquantData(stocks, dataSavingPath, fre='60m'):
    # stocks = ['002475', '000333', '000661', '000858', '600036', '600276', '600309', '600585', '603288', '601318', '600009']
    start_date = '2020-01-01'
    end_date = datetime.today().strftime("%Y-%m-%d")
    
    if not os.path.exists(dataSavingPath):
        os.makedirs(dataSavingPath)
        
    for stock in stocks:
        security = jq.normalize_code(stock)
        # df = get_bars(security, 100, unit='1m', fields=['date', 'open', 'high', 'low', 'close'],
        #               include_now=False, end_dt=None, fq_ref_date=None)
        df2 = jq.get_price(security, start_date, end_date, frequency=fre, skip_paused=True, fq='pre')
        df2 = df2.loc[:, ['open', 'high', 'low', 'close']]
        df2.loc[:, 'datetime'] = df2.index
        df2.reset_index(drop=True, inplace=True)
        data = df2.loc[:, ['datetime', 'open', 'high', 'low', 'close']]
        data.loc[:, 'ft'] = 0
        data.to_csv(os.path.join(dataSavingPath, stock +".csv"), index=0)
    
# for stock in stocks:
#     data['open'] = data['open'] * 1000
#     data['high'] = data['high'] * 1000
#     data['low'] = data['low'] * 1000
#     data['close'] = data['close'] * 1000
#     data['open'] = data['open'].apply(lambda x: round(x))
#     data['high'] = data['high'].apply(lambda x: round(x))
#     data['low'] = data['low'].apply(lambda x: round(x))
#     data['close'] = data['close'].apply(lambda x: round(x))
#     # data['date'] = data['date'].apply(lambda x: x.split("'")[1])
#     # data['time'] = data['time'].apply(lambda x: x.split("'")[1])
#     data['open'] = data['open'].apply(lambda x: int(x))
#     data['high'] = data['high'].apply(lambda x: int(x))
#     data['low'] = data['low'].apply(lambda x: int(x))
#     data['close'] = data['close'].apply(lambda x: int(x))
#     data['time'] = data['time'].apply(lambda x: str(x))
#     data['time'] = pd.to_timedelta(data['time']) - Minute(1)
#     data.loc[data['time'] < "12:00:00", 'time'] = data.loc[data['time'] < "12:00:00", 'time'] - Minute(30)
#     data['time'] = data['time'].apply(lambda x: str(x).split(" ")[2])
#     data['time'] = data['time'].apply(lambda x: x.split(":")[0] + ":" + x.split(":")[1])
#     # data['date'] = data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
#     data.to_csv(os.path.join(stocks_path, "mt4_" + stock + ".csv", index=0))


# ----------------------------------------------------------------------------------------------------------------------
future_exchange_house_dict = {
    'XSGE':['rb', 'bu'],
    'XDCE': ['p', 'm'],
    'XZCE': [],
    'XINE': []
}


# ----------------------------------------------------------------------------------------------------------------------
def get_symbol(instrument):
    symbol = instrument[0]
    try:
        isinstance(int(instrument[1]), int)
        pass
    except ValueError:
        symbol = instrument[:2]
    return symbol


# ----------------------------------------------------------------------------------------------------------------------
def getFutueDataFromJointquant(futures_):
    today = date.today().strftime('%Y-%m-%d')
    tomorrow = (date.today() + timedelta(days=1)).strftime('%Y-%m-%d')
    email = Email()
    email.receivers = [cwhEamil]
    for future in futures_:
        symbol = get_symbol(future)
        print('future is %s, symbol is %s.' %(future, symbol))
        future_with_suffix = None
        for key, value in future_exchange_house_dict.items():
            if symbol in value:
                future_with_suffix = future+'.'+key
        if future_with_suffix is None: 
            email.send("%s, future with suffix is None" %future, 'getting future data from jointquant has stop.')
            return

        # get future data from jointquant
        df = jq.get_price(future_with_suffix, start_date=today, end_date=tomorrow, frequency='1m', skip_paused=False, fq='pre')
        if len(df) == 0:
            email.send("%s, get empty dataframe from jointquant"%future_with_suffix, 
                "one of the reason might be that %s has stop trading, "
                "there is no trading data from today on."%future)
            return
        
        df['datetime'] = df.index
        df['datetime'] = df['datetime'] - Minute(1)
        df.set_index('datetime', inplace=True, drop=True)

        # resample to 60 minutes
        hour_data = pd.DataFrame()
        hour_data['open'] = df['open'].resample('60T', closed='left', label='left').first()
        hour_data['high'] = df['high'].resample('60T', closed='left', label='left').max()
        hour_data['low'] = df['low'].resample('60T', closed='left', label='left').min()
        hour_data['close'] = df['close'].resample('60T', closed='left', label='left').last()
        hour_data['volume'] = df['volume'].resample('60T', closed='left', label='left').sum()
        hour_data = hour_data[hour_data['volume'] != 0]
        hour_data = hour_data.dropna()
        hour_data = hour_data[['open', 'high', 'low', 'close']]

        # history data
        his_data = pd.read_csv("\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\future_daily_data\\" + future + '.csv')
        his_data['date'] = pd.to_datetime(his_data['datetime']).apply(lambda x: x.strftime("%Y-%m-%d"))
        his_data = his_data[his_data['date'] != today]
        his_data = his_data[['datetime', 'open', 'high', 'low', 'close']]
        his_data.set_index('datetime', inplace=True, drop=True)

        # concat
        new_data = pd.concat([his_data, hour_data])
        new_data.drop_duplicates(inplace=True)
        # new_data = new_data[~(new_data['high'] == new_data['low'])]

        # new_data.to_csv("\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\future_daily_data\\" + future + '.csv')
        print(new_data.tail(5))


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    stocks = ['002475', '000333', '000661', '000858', '600036', 
              '600276', '600309', '600585', '603288', '601318', '600009']
    dataSavingPath = os.path.join('c:', os.sep, 'cwh',  'spike stocks')
    getJointquantData(stocks, dataSaveingPath)
