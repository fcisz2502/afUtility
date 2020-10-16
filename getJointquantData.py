import pandas as pd
import os
from datetime import datetime, time, timedelta
import jqdatasdk as jq
from afUtility.keyInfo import jqAccount, jqPassword, cwhEmail
from afUtility.mailing import Email, cwhEmail
jq.auth(jqAccount, jqPassword)


# -----------------------------------------------------------------------------
def getJointquantData(stocks, dataSavingPath_, fre='60m'):
    # stocks = ['002475', '000333', '000661', '000858', '600036', '600276', '600309', '600585', '603288', '601318', '600009']
    start_date = '2020-01-01'
    tomorrow = (datetime.combine(datetime.today().date(), time(0))+timedelta(hours=24)).strftime("%Y-%m-%d")
    todayTime0 = datetime.combine(datetime.today().date(), time(0)).strftime("%Y-%m-%d %H:%M:%S")
    
    if not os.path.exists(dataSavingPath_):
        os.makedirs(dataSavingPath_)
        
    for stock in stocks:
        print('getting to %s.' % stock)
        security = jq.normalize_code(stock)
        # df = get_bars(security, 100, unit='1m', fields=['date', 'open', 'high', 'low', 'close'],
        #               include_now=False, end_dt=None, fq_ref_date=None)
        jqdata = jq.get_price(security, start_date, tomorrow, frequency=fre, skip_paused=True, fq='pre')
        jqdata = jqdata.loc[:, ['open', 'high', 'low', 'close']]
        jqdata.loc[:, 'ft'] = 0
        jqdata = jqdata.loc[:, ['open', 'high', 'low', 'close', 'ft']]
        jqdata.index.rename("datetime", inplace=True)

        previous_data = pd.read_csv(os.path.join(dataSavingPath_, stock + ".csv"),
                                  parse_dates=['datetime'],
                                  index_col='datetime')
        full_bars = pd.concat([previous_data.loc[:todayTime0], jqdata.loc[todayTime0:, :]])
        full_bars.drop_duplicates(keep='first', inplace=True)

        full_bars.to_csv(os.path.join(dataSavingPath_, stock + ".csv"))
    print("Getting jq data done. Data has been saved to: \n%s." % dataSavingPath_)
    
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
def getFutueDataFromJointquant(instruments_, dataSavingPath_):
    tenDaysBefore = (datetime.now()-timedelta(days=10)).strftime('%Y-%m-%d')
    tomorrow = (datetime.now() + timedelta(days=1)).strftime('%Y-%m-%d')
    email = Email()
    email.receivers = [cwhEmail]
    for instrument in instruments_:
        symbol = get_symbol(instrument)
        print('instrument is %s, symbol is %s.' %(instrument, symbol))
        instrument_with_suffix = None
        for key, value in future_exchange_house_dict.items():
            if symbol in value:
                instrument_with_suffix = instrument.upper()+'.'+key
        if instrument_with_suffix is None:
            email.send("%s, instrument with suffix is None" %instrument,
                       'getting instrument data from jointquant has stopped.')
            return

        # get data from jointquant
        jqdata = jq.get_price(instrument_with_suffix, start_date=tenDaysBefore, end_date=tomorrow, frequency='1m', skip_paused=False, fq='pre')
        
        jqdata['datetime'] = jqdata.index
        jqdata['datetime'] = jqdata['datetime'] - timedelta(minutes=1)
        jqdata.set_index('datetime', inplace=True, drop=True)

        # resample to 60 minutes
        jq_hour_data = pd.DataFrame()
        jq_hour_data['open'] = jqdata['open'].resample('60T', closed='left', label='left').first()
        jq_hour_data['high'] = jqdata['high'].resample('60T', closed='left', label='left').max()
        jq_hour_data['low'] = jqdata['low'].resample('60T', closed='left', label='left').min()
        jq_hour_data['close'] = jqdata['close'].resample('60T', closed='left', label='left').last()
        jq_hour_data['volume'] = jqdata['volume'].resample('60T', closed='left', label='left').sum()
        jq_hour_data = jq_hour_data[jq_hour_data['volume'] != 0]
        jq_hour_data = jq_hour_data.dropna()
        jq_hour_data = jq_hour_data[['open', 'high', 'low', 'close']]

        # history data
        # his_data = pd.read_csv("\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\future_daily_data\\" + future + '.csv')
        his_data = pd.read_csv(os.path.join(dataSavingPath_, instrument+'.csv'),
                               parse_dates=['datetime'], index_col='datetime')
        time_now = datetime.now().time()
        # if fetch data after 2300 and before 800,
        # last trading section is a night section or night section is cancelled then there is no data
        if time_now > time(23) or time_now < time(8):
            join_datetime = datetime.combine(his_data.index.to_list()[-1].date(), time(16)
                                              ).strftime("%Y-%m-%d %H:%M:%S")
        # if fetch at noon or after 1500,
        # go back to last night at 2300,
        # then fetch history data up to last night and get the last bar's date, go back to that date's 1600
        else:
            join_datetime = datetime.combine((datetime.now()-timedelta(days=1)).date(), time(23))
            join_datetime = datetime.combine(his_data.loc[:join_datetime, :].index.date(), time(16)
                                              ).strftime("%Y-%m-%d %H:%M:%S")

        if 0 == len(jq_hour_data.loc[join_datetime:, :]):
            email.send("%s, get empty dataframe from jointquant"%instrument_with_suffix,
                "one of the reasons might be that %s has stopped trading, "
                "there is no trading data from today on."%instrument)
            return

        # keep update today's data to avoid data error during night or morning section
        # if there's any, I think jointquant will fix it asap
        # what about afternoon section data error? Well, I have no idea.
        # order review will detect data differences if there is any
        full_bar = pd.concat([his_data, jq_hour_data.loc[join_datetime:, :]])
        full_bar.drop_duplicates(keep='first', inplace=True)

        full_bar.to_csv(os.path.join(dataSavingPath_, instrument+'.csv'))
        # print(full_bar.tail(5))


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    stocks = ['002475', '000333', '000661', '000858', '600036', 
              '600276', '600309', '600585', '603288', '601318', '600009']
    stock_data_path = os.path.join('c:', os.sep, 'jqData',  'jqData')
    # getJointquantData(stocks, stock_data_path)

    # -------------------------------------------------------------------------
    # get future data
    future_data_path = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud",
                               "dataProcess", "future_daily_data")
    futures= ['rb2101', 'm2101']
    getFutueDataFromJointquant(futures, future_data_path)
