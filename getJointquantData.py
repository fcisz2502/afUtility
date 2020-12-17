import pandas as pd
import os
from datetime import datetime, time, timedelta
from afUtility.keyInfo import jqAccount, jqPassword
from afUtility.mailing import Email, cwhEmail


# -----------------------------------------------------------------------------
def getStockDataFromJQ(stock, start_date, fre):
    import jqdatasdk as jq
    jq.auth(jqAccount, jqPassword)
    tomorrow = (datetime.combine(datetime.today().date(), time(0)) + timedelta(days=1)).strftime("%Y-%m-%d")
    security = jq.normalize_code(stock)
    jqdata = jq.get_price(security, start_date, tomorrow, frequency=fre, skip_paused=True, fq='pre')
    jqdata = jqdata.loc[:, ['open', 'high', 'low', 'close', 'money', 'volume']]
    jqdata.index.rename("datetime", inplace=True)

    return jqdata

# -----------------------------------------------------------------------------
def getJQStockDataForBacktesting(stocks, dataSavingPath_, fre='60m'):
    if not os.path.exists(dataSavingPath_):
        os.makedirs(dataSavingPath_)

    for stock in stocks:
        jqdata = getStockDataFromJQ(stock, '2014-01-01', fre)
        jqdata.reset_index(inplace=True)
        jqdata.loc[:, "datetime"] -= timedelta(minutes=60)
        jqdata.loc[:, 'time'] = jqdata.loc[:, 'datetime'].map(lambda x: x.time())
        jqdata.loc[jqdata['time'] < time(12), 'datetime'] -= timedelta(minutes=30)
        jqdata.drop(columns='time', inplace=True)
        volume_scalar = 1000000
        jqdata.loc[:, 'money'] = jqdata.loc[:, 'money'].map(lambda x: int(x/volume_scalar))
        print("%s's money has been divided by %s and changed to integer for both applepy and MT4." % (stock, volume_scalar))
        jqdata.to_csv(os.path.join(dataSavingPath_, stock+'_60m_for_applepy.csv'), index=0)
        ohlc_scalar = 100
        jqdata.loc[:, ["open", "high", "low", "close"]] *= ohlc_scalar
        print("%s's ohlc has been amplified by %s for just MT4." %(stock, ohlc_scalar))
        # jqdata.loc[:, 'money'] /= 1000000
        jqdata.loc[:, 'money'] = jqdata.loc[:, 'money'].map(lambda x: int(x))
        jqdata.to_csv(os.path.join(dataSavingPath_, stock+'_60m_for_MT4.csv'), index=0)
        print("Getting %s's data from JQ has done." % stock)

# -----------------------------------------------------------------------------
def getJQStockDataForTrading(stocks, folderPath, fre='60m'):
    if not os.path.exists(folderPath):
        os.makedirs(folderPath)

    todayTime0Str = datetime.combine(datetime.today().date(), time(0)).strftime("%Y-%m-%d %H:%M:%S")  # .strftime("%Y-%m-%d %H:%M:%S")
    nowStr = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    for stock in stocks:
        filePath = os.path.join(folderPath, stock+"_trading_bars.csv")
        print('getting to %s.' % stock)
        if os.path.exists(filePath):
            start_date = datetime.today() - timedelta(days=10)
            previous_data = pd.read_csv(filePath, parse_dates=['datetime'], index_col='datetime')
        else:
            start_date = datetime.today() - timedelta(days=365)
            previous_data = None

        jqdata = getStockDataFromJQ(stock, start_date.strftime("%Y-%m-%d"), fre)
        jqdata.loc[:, 'ft'] = 0
        jqdata = jqdata.loc[:, ['open', 'high', 'low', 'close', 'ft']]

        if previous_data is not None:
            full_bars = pd.concat([previous_data.loc[:todayTime0Str, :], jqdata.loc[todayTime0Str:nowStr, :]])
            full_bars.drop_duplicates(keep='first', inplace=True)
        else:
            full_bars = jqdata
        full_bars.to_csv(filePath)

    print("Getting jq data done. Data has been saved to: \n%s." %folderPath)


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
def getFutureDataFromJQ(instrument_with_suffix_, start_date):
    import jqdatasdk as jq
    jq.auth(jqAccount, jqPassword)
    
    jqdata = jq.get_price(instrument_with_suffix_,
                          start_date=start_date,
                          end_date=(datetime.today() + timedelta(days=1)).strftime('%Y-%m-%d'),
                          frequency='1m',
                          skip_paused=False,
                          fq='pre')

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

    return jq_hour_data

# ----------------------------------------------------------------------------------------------------------------------
def updateFutureDataWithJointquant(futures_, dataSavingPath_):
    email = Email()
    email.receivers = [cwhEmail]
    for instrument in futures_:
        symbol = get_symbol(instrument)
        print('instrument is %s, symbol is %s.' %(instrument, symbol))
        instrument_with_suffix = None
        for key, value in future_exchange_house_dict.items():
            if symbol in value:
                instrument_with_suffix = instrument.upper()+'.'+key
                print("instrument_with_suffix is: ", instrument_with_suffix)
        if instrument_with_suffix is None:
            email.send("%s, instrument with suffix is None" %instrument,
                       'getting instrument data from jointquant has failed. '
                       'Instrument_with_suffix is like: BU2106.XSGE')
            return

        # data not exist, get data
        # his_data = pd.read_csv("\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\future_daily_data\\" + future + '.csv')
        if not os.path.exists(os.path.join(dataSavingPath_, instrument + '.csv')):
            jq_hour_data = getFutureDataFromJQ(instrument_with_suffix,
                                               (datetime.today()-timedelta(days=100)).strftime("%Y-%m-%d"))
            print(jq_hour_data.head(5))
            jq_hour_data.to_csv(os.path.join(dataSavingPath_, instrument + '.csv'))
            print("first fetching %s's data has done." %instrument)

        # update existing data
        else:
            jq_hour_data = getFutureDataFromJQ(instrument_with_suffix,
                                               (datetime.today()-timedelta(days=10)).strftime("%Y-%m-%d"))

            his_data = pd.read_csv(os.path.join(dataSavingPath_, instrument+'.csv'),
                                   parse_dates=['datetime'], index_col='datetime')
            time_now = datetime.now().time()
            # if fetch data after 2300 and before 800,
            # last trading section is a night section or night section is cancelled then there is no data
            if time_now > time(23) or time_now < time(8):
                # Warning
                # Warning
                # Warning， 如果是铜、金等交易到下半夜的品种，下面写法就会考虑不周到
                join_datetime = datetime.combine(his_data.index.to_list()[-1].date(), time(16)
                                                  ).strftime("%Y-%m-%d %H:%M:%S")
            # if fetch at noon or after 1500,
            # go back to last trading day, before night section
            else:
                if 6 == datetime.today().weekday():   # Sunday, go back two days to Friday
                    days_ = 2
                elif 0 == datetime.today().weekday():  # Monday, go back three days to Friday
                    days_ = 3
                else:
                    days_ = 1 # go back to yesterday
                join_datetime = (datetime.combine(datetime.now().date(), time(16))-timedelta(days=days_)).strftime("%Y-%m-%d %H:%M:%S")
            # print("join_datetime is: ", join_datetime)
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


# ----------------------------------------------------------------------------------------------------------------------
class LocalDataReplacement(object):
    def __init__(self, stock, local_folder_path):
        print('get to %s.' %stock)
        self._stock = stock
        self._threshold = 0.0025

        self._local_folder_path = local_folder_path
        self._local_bar_path = os.path.join(self._local_folder_path, stock+"_trading_bars.csv")
        self._local_bar_path_backup = os.path.join(self._local_folder_path, stock+"_trading_bars_backup.csv")

        self._jointquant_bars = None
        self._local_bars = None
        self._get_jointquant_bars()
        self._get_local_bars()
        
        self.todayStr = datetime.combine(datetime.today(), time(0)).strftime("%Y-%m-%d %H:%M:%S")

    # --------------------------------------------------------------------------------------------------------------
    def set_threshold(self, x):
        # better have code to evalue x
        self._threshold = x

    # --------------------------------------------------------------------------------------------------------------
    def _get_local_bars(self):
        self._local_bars = pd.read_csv(self._local_bar_path, parse_dates=['datetime'], index_col='datetime')

    # --------------------------------------------------------------------------------------------------------------
    def _get_jointquant_bars(self):
        path = os.path.join(os.sep * 2, "FCIDEBIAN", "FCI_Cloud", "dataProcess",
    		"spike stocks", "stock data for order review", self._stock+"_trading_bars.csv")
        self._jointquant_bars = pd.read_csv(path, parse_dates=['datetime'], index_col='datetime')

    # --------------------------------------------------------------------------------------------------------------
    def check_jq_data(self):
        # check number of bars
        # check bars end times
        check_pass = True

        todaysBars = self._jointquant_bars.loc[datetime.today().strftime("%Y-%m-%d"):, :]
        numberOfTodaysBars = len(todaysBars)

        listOftodaysBarsDatetime = todaysBars.index.to_list()

        if time(11, 30) < datetime.now().time() < time(13):
            if 2 != numberOfTodaysBars:
                print("%s number of bars in the monring section is not 2." %self._stock)
                check_pass = False
                return check_pass
            else:
                if listOftodaysBarsDatetime[0].time() != time(10, 30) or \
                    listOftodaysBarsDatetime[1].time() != time(11, 30):
                    print('%s bars not end at 10:30 or 11:30.' %self._stock)
                    check_pass = False
                    return check_pass

        elif time(15) < datetime.now().time():
            if 4 != numberOfTodaysBars:
                print("%s number of today's bars is not 4." % self._stock)
                check_pass = False
                return check_pass
            else:
                if listOftodaysBarsDatetime[0].time() != time(10, 30) or \
                        listOftodaysBarsDatetime[1].time() != time(11, 30) or \
                        listOftodaysBarsDatetime[2].time() != time(14) or \
                        listOftodaysBarsDatetime[3].time() != time(15):
                    print('%s bars not end at 10:30 or 11:30 or 14:00 or 15:00.' % self._stock)
                    check_pass = False
                    return check_pass
        elif datetime.now() < time(9, 25):  # trading not yet started, no data
            pass
        else:  # during trading, 9:25-11:30 and 13:00-15:00, close price not know yet
            pass

        return check_pass

    # --------------------------------------------------------------------------------------------------------------
    def _compare_two_sources(self):
        # compare ohlc difference
        compare_pass = True

        df_local = self._local_bars.loc[self.todayStr:, :].copy()
        df_jq = self._jointquant_bars.loc[self.todayStr:, :].copy()

        df_jq.loc[:, 'on'] = range(0, len(df_jq))
        df_local.loc[:, 'on'] = range(0, len(df_local))

        df_both = pd.merge(df_jq, df_local, on='on')

        df_both.loc[:, 'o_diff'] = df_both.loc[:, 'open_x'] - df_both.loc[:, 'open_y']
        df_both.loc[:, 'h_diff'] = df_both.loc[:, 'high_x'] - df_both.loc[:, 'high_y']
        df_both.loc[:, 'l_diff'] = df_both.loc[:, 'low_x'] - df_both.loc[:, 'low_y']
        df_both.loc[:, 'c_diff'] = df_both.loc[:, 'close_x'] - df_both.loc[:, 'close_y']
        # df_both.loc[:, 'o_min'] = min(df_jq.loc[:, 'open'], df_spike2.loc[:, 'open'])
        for i in range(len(df_both)):
            df_both.loc[i, 'o_min'] = min(df_both.loc[i, 'open_x'], df_both.loc[i, 'open_y'])
            df_both.loc[i, 'h_min'] = min(df_both.loc[i, 'high_x'], df_both.loc[i, 'high_y'])
            df_both.loc[i, 'l_min'] = min(df_both.loc[i, 'low_x'], df_both.loc[i, 'low_y'])
            df_both.loc[i, 'c_min'] = min(df_both.loc[i, 'close_x'], df_both.loc[i, 'close_y'])

        df_both.loc[:, 'o_diff%'] = abs(df_both.loc[:, 'o_diff'] / df_both.loc[:, 'o_min'])
        df_both.loc[:, 'h_diff%'] = abs(df_both.loc[:, 'h_diff'] / df_both.loc[:, 'h_min'])
        df_both.loc[:, 'l_diff%'] = abs(df_both.loc[:, 'l_diff'] / df_both.loc[:, 'l_min'])
        df_both.loc[:, 'c_diff%'] = abs(df_both.loc[:, 'c_diff'] / df_both.loc[:, 'c_min'])

        # threshold = 0.0025
        df_both.loc[:, 'o_diff%>threshold'] = df_both.loc[:, 'o_diff%'] > self._threshold
        df_both.loc[:, 'h_diff%>threshold'] = df_both.loc[:, 'h_diff%'] > self._threshold
        df_both.loc[:, 'l_diff%>threshold'] = df_both.loc[:, 'l_diff%'] > self._threshold
        df_both.loc[:, 'c_diff%>threshold'] = df_both.loc[:, 'c_diff%'] > self._threshold

        if df_both.loc[:, 'o_diff%>threshold':'c_diff%>threshold'].sum().sum():
            print('%s has significant differences between trading bars and jq bars.')
            print(df_both.loc[:, 'open_x':'close_x'])
            print(df_both.loc[:, 'open_y':'close_y'])
            print(df_both.loc[:, 'o_diff':'c_diff'])
            print(df_both.loc[:, 'o_min':'c_min'])
            print(df_both.loc[:, 'o_diff%':'c_diff%'])
            print(df_both.loc[:, 'o_diff%>threshold':'c_diff%>threshold'])
            compare_pass = False

        return compare_pass

    # --------------------------------------------------------------------------------------------------------------
    def chek_local_bars_end_time(self):
        pass

    # --------------------------------------------------------------------------------------------------------------
    def _backup_local_bars(self):
        if os.path.exists(self._local_bar_path_backup):
            bars_backup = pd.read_csv(self._local_bar_path_backup, parse_dates=['datetime'], index_col='datetime')
            last_backup_datetime = bars_backup.tail(1).index.to_list()[0]+timedelta(hours=1)
            df = pd.concat([bars_backup,
                            self._local_bars.loc[last_backup_datetime.strftime("%Y-%m-%d %H:%M:%S"):, :]
                            ])
            df.to_csv(self._local_bar_path_backup)
        else:
            self._local_bars.to_csv(self._local_bar_path_backup)
    
    # --------------------------------------------------------------------------------------------------------------
    def _replaceLocalBarsWithJointquantData(self):
        new_bars = pd.concat([
                self._local_bars.loc[:self.todayStr, :], 
                self._jointquant_bars.loc[self.todayStr:, :]])
        
        new_bars = new_bars.drop_duplicates()
        new_bars.to_csv(self._local_bar_path)

    # --------------------------------------------------------------------------------------------------------------
    def run_replacement(self):
        replacement_successed = True
        
        if self.check_jq_data():
            if self._compare_two_sources():
                self._backup_local_bars()
                self._replaceLocalBarsWithJointquantData()
            else:
                replacement_successed = False
        else:
            replacement_successed = False
        return replacement_successed


# -----------------------------------------------------------------------------
def replace_spike5_trading_bars(stocks):
    # __for spike 5
    folder_path = os.path.join('C', os.sep, 'quant',
                               'spike5.0', 'realtrading',
                               'realTradingData')
    # stocks = ['002475', '600585']
    stocks_ = stocks[:]
    for stock in stocks:
        local_folder_path = os.path.join(folder_path, stock)
        ldr = LocalDataReplacement(stock, local_folder_path)
        #    ldr.check_jq_data()
        res = ldr.run_replacement()
        if res:
            stocks_.remove(stock)
    if stocks_:
        email = Email()
        email.send('Spike5 replace local bars with jq data has failed',
                   str(stocks_))
    else:
        print('\nspike5 trading bars replacement succeed')

        
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # stocks = ['002475', '000333', '000661', '000858', '600036',
    #           '600276', '600309', '603288', '601318']
#    stocks = ['600585']
    # folder_path = os.path.join('c:', os.sep, 'jqData',  'jqData')
    # getJQStockDataForBacktesting(stocks, folder_path)

    # folder_path = os.path.join('c:', os.sep, 'jqData', 'jqData')
#    folder_path = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "dataProcess",
#                                    "spike stocks", "stock data for order review")
#    getJQStockDataForTrading(stocks, folder_path)

    # -------------------------------------------------------------------------
    # get future data
    # future_data_path = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud",
    #                            "dataProcess", "future_daily_data")
    # futures= ['rb2105']
    # updateFutureDataWithJointquant(futures, future_data_path)
    
    folder_path = os.path.join('C', os.sep, 'quant',
                               'spike5.0', 'realtrading', 
                               'realTradingData')
    stocks = ['600031', '002475',
              '000858', '600009', 
              '600276', '000661',
              '600309', '603288', 
              '000333', '600036',
              '601318']
    stocks = ['603288']
    stocks_ = stocks[:]
    for stock in stocks:
        local_folder_path = os.path.join(folder_path, stock)
        ldr = LocalDataReplacement(stock, local_folder_path)
    #    ldr.check_jq_data()
        res = ldr.run_replacement()
        if res:
            stocks_.remove(stock)
    if stocks_:
        email = Email()
        email.send('Spike replace local bars with jq data has failed', 
                   str(stocks_))
    else:
        print('\nspike5 trading bars replacement succeed')

