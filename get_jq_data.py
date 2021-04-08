# -*- coding: utf-8 -*-
import pandas as pd
import os
from datetime import datetime, time, timedelta
from afUtility.keyInfo import jqAccount, jqPassword
from afUtility.mailing import Email, cwhEmail
from dateutil import parser
import jqdatasdk as jq


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
                # Warning! 如果是铜、金等交易到下半夜的品种，下面写法就会考虑不周到
                # Warning! 如果是铜、金等交易到下半夜的品种，下面写法就会考虑不周到
                # Warning! 如果是铜、金等交易到下半夜的品种，下面写法就会考虑不周到
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
def get_jq_stock_data_for_backtesting(
    stocks, 
    file_dir=os.path.join(
        os.sep*2, 'FCIDEBIAN', 'FCI_Cloud', 'dataProcess', 'stock data from jq', 'backtesting'
        ), 
    fre='60m'):
    if not os.path.exists(file_dir):
        os.makedirs(file_dir)
    
    for stock in stocks:
        print("Getting %s's data from JQ." % stock)
        file_path = os.path.join(file_dir, stock+'_'+fre+'_for_applepy.csv')
        if not os.path.exists(file_path):
            prev_data = None
            jq_start_date = '2014-01-01'
        else:
            prev_data = pd.read_csv(file_path, parse_dates=['datetime'])
            jq_start_date = prev_data.loc[len(prev_data)-1, 'datetime'].strftime('%Y-%m-%d')

        jqdata = JointquantDataReplacement.get_stock_data_from_jointquant(
            stock,
            jq_start_date,
            fre
        )
        jqdata.reset_index(inplace=True)

        if fre[-1] == 'm':
            # move time back
            jqdata.loc[:, "datetime"] -= timedelta(minutes=int(fre[:-1]))
            jqdata.loc[:, 'time'] = jqdata.loc[:, 'datetime'].map(lambda x: x.time())
            jqdata.loc[jqdata['time'] < time(12), 'datetime'] -= timedelta(minutes=30)
            jqdata.drop(columns='time', inplace=True)
        elif fre == '1d':
            pass
        # change amount for both applepy and MT4
        # amount_scalar = 1000000
        # jqdata.loc[:, 'amount'] = jqdata.loc[:, 'amount'].map(lambda x: int(x/amount_scalar))
        # print("%s's amount has been divided by %s and changed to integer for both applepy and MT4." % (stock, amount_scalar))

        if prev_data is not None:
            full = pd.concat([prev_data, jqdata])
            full.drop_duplicates(keep='last', inplace=True)
        else:
            full = jqdata

        # save for applepy
        full.to_csv(os.path.join(file_path), index=0)

        # change ohlc and save for MT4
        ohlc_scalar = 100
        full.loc[:, ["open", "high", "low", "close"]] *= ohlc_scalar
        print("%s's ohlc has been amplified by %s for just MT4." %(stock, ohlc_scalar))

        MT4_dir = os.path.join(file_dir, 'for MT4')
        if not os.path.exists(MT4_dir):
            os.makedirs(MT4_dir)

        full.to_csv(os.path.join(MT4_dir, stock+'_'+fre+'_for_MT4.csv'), index=0)



    print("Getting %s's data from JQ is done." % stocks)


# ----------------------------------------------------------------------------------------------------------------------
class JointquantDataReplacement(object):
    def __init__(self, stock):
        self._stock = stock
        self.email = Email()

        self._jointquant_data_dir = None
        self._jointquatn_data_path = None
        self._set_jointquant_data_dir()
        self._trading_bars_dir = None

        self._threshold = 0.0025
        self._todayStr = datetime.combine(datetime.now().date(), time(0)).strftime('%Y-%m-%d %H:%M:%S')

        self._trading_bars = None
        self._jointquant_data_origin = None
        self._jq_bars_for_all_spike = None
        self._jq_bars_for_spike6 = None
        self._jq_bars_for_spike5 = None
        self._jq_bars_for_spike3 = None

    # ------------------------------------------------------------------------------------------------------------------
    def _email_and_print(self, cont):
        self.email.send(cont, '')
        print(cont)

    # ------------------------------------------------------------------------------------------------------------------
    def _set_jointquant_data_dir(self):
        self._jointquant_data_dir = os.path.join(
            os.sep * 2, 'FCIDEBIAN', 'FCI_Cloud',
            'dataProcess', 'stock data from jq')
        self._jointquatn_data_path = os.path.join(self._jointquant_data_dir, self._stock + '_origin.csv')

    # ------------------------------------------------------------------------------------------------------------------
    @staticmethod
    def get_stock_data_from_jointquant(stock, start_date, fre='60m'):
        jq.auth(jqAccount, jqPassword)
        security = jq.normalize_code(stock)

        if security[:6]=='000300':
            security = '000300.XSHG'

        jqdata = jq.get_price(security,
                              start_date,
                              (datetime.now() + timedelta(days=1)).strftime("%Y-%m-%d"),
                              frequency=fre,
                              skip_paused=True,
                              fq='pre'
                              )
        jqdata = jqdata.loc[:, ['open', 'high', 'low', 'close', 'money', 'volume']]
        jqdata.rename(columns={'money':'amount'},inplace=True)
        jqdata.index.rename("datetime", inplace=True)

        return jqdata

    # ------------------------------------------------------------------------------------------------------------------
    def _update_jointquant_data(self):
        has_jq_origin_data_updated = True
        # update and save to local machine
        if os.path.exists(self._jointquatn_data_path):
            previous_jointquant_data = pd.read_csv(
                self._jointquatn_data_path,
                parse_dates=['datetime'],
                index_col='datetime'
            )

            # whether has updated today after 1500 
            # has not
            if previous_jointquant_data.index.to_list()[-1] < datetime.combine(datetime.now().date(), time(12)):
                try:
                    last_update_datetime = previous_jointquant_data.iloc[-1].name
                except:
                    last_update_datetime = previous_jointquant_data.index.to_list()[-1]

                latest_jointquant_data = self.get_stock_data_from_jointquant(
                    self._stock,
                    (last_update_datetime - timedelta(days=10)).strftime("%Y-%m-%d")
                )

                try:
                    joint_datetime = latest_jointquant_data.iloc[1].name
                except:
                    joint_datetime = latest_jointquant_data.index.to_list()[0]
                joint_datetime -= timedelta(minutes=15)

                self._jointquant_data_origin = pd.concat([
                    previous_jointquant_data.loc[:joint_datetime.strftime('%Y-%m-%d %H:%M:%S'), :],
                    latest_jointquant_data.loc[:, :]
                ])
            # has
            # previous_jointquant_data.index.to_list()[-1] > 1400 or 1500, no need to update jq data nor save it to local
            else:
                has_jq_origin_data_updated = False
                self._jointquant_data_origin = previous_jointquant_data
        else:
            self._jointquant_data_origin = self.get_stock_data_from_jointquant(
                self._stock,
                (datetime.now() - timedelta(days=180)).strftime("%Y-%m-%d")
            )

        self._jointquant_data_origin.drop_duplicates(keep='last', inplace=True)

        if datetime.now().time() > time(15):
            end_time = datetime.combine(datetime.now().date(), time(16)).strftime('%Y-%m-%d %H:%M:%S')
        elif datetime.now().time() > time(11, 30):
            end_time = datetime.combine(datetime.now().date(), time(12)).strftime('%Y-%m-%d %H:%M:%S')
        else:
            end_time = datetime.combine(datetime.now().date(), time(9)).strftime('%Y-%m-%d %H:%M:%S')

        self._jointquant_data_origin = self._jointquant_data_origin.loc[:end_time, :]

    # ------------------------------------------------------------------------------------------------------------------
    def _update_and_check_jointquant_data(self):
        '''
        check number of bars
        check bars end times
        if True, save jq data to local
        return True if pass else False
        '''
        self._update_jointquant_data()

        check_pass = True

        todaysBars = self._jointquant_data_origin.loc[datetime.today().strftime("%Y-%m-%d"):, :]
        numberOfTodaysBars = len(todaysBars)

        listOftodaysBarsDatetime = todaysBars.index.to_list()

        if time(11, 30) < datetime.now().time() < time(13):
            if 2 != numberOfTodaysBars:
                self._email_and_print("%s number of bars in the monring section is not 2." % self._stock)
                check_pass = False
                return check_pass
            else:
                if listOftodaysBarsDatetime[0].time() != time(10, 30) or \
                    listOftodaysBarsDatetime[1].time() != time(11, 30):
                    self._email_and_print('%s bars not end at 10:30 or 11:30.' %self._stock)
                    check_pass = False
                    return check_pass

        elif time(15) < datetime.now().time():
            if 4 != numberOfTodaysBars:
                self._email_and_print("%s number of today's bars is not 4." % self._stock)
                check_pass = False
                return check_pass
            else:
                if listOftodaysBarsDatetime[0].time() != time(10, 30) or \
                        listOftodaysBarsDatetime[1].time() != time(11, 30) or \
                        listOftodaysBarsDatetime[2].time() != time(14) or \
                        listOftodaysBarsDatetime[3].time() != time(15):
                    self._email_and_print('%s bars not end at 10:30 or 11:30 or 14:00 or 15:00.' % self._stock)
                    check_pass = False
                    return check_pass
        elif datetime.now().time() < time(9, 25):  # trading not yet started, no data
            pass
        else:  # during trading, 9:25-11:30 and 13:00-15:00, close price not know yet
            pass

        if check_pass:
            self._jointquant_data_origin.to_csv(self._jointquatn_data_path)

        return check_pass

    # ------------------------------------------------------------------------------------------------------------------
    def _produce_jq_bars_for_all_spike(self):
        self._jq_bars_for_all_spike = self._jointquant_data_origin.copy()
        self._jq_bars_for_all_spike.loc[:, 'datetime'] = self._jq_bars_for_all_spike.index
        self._jq_bars_for_all_spike.reset_index(inplace=True, drop=True)

        _len = len(self._jq_bars_for_all_spike)
        if self._jq_bars_for_all_spike.loc[_len-1, 'datetime'].time() < time(12):
            self._jq_bars_for_all_spike.loc[_len-1, 'amount'] += self._jq_bars_for_all_spike.loc[_len-2, 'amount']
        else:
            self._jq_bars_for_all_spike.loc[_len-1, 'amount'] += self._jq_bars_for_all_spike.loc[_len-4:_len-2, 'amount'].sum()

        self._jq_bars_for_all_spike = self._jq_bars_for_all_spike.loc[:,
                                   ['datetime', 'open', 'high', 'low', 'close', 'amount']]

        self._jq_bars_for_all_spike.loc[:, 'ft'] = 0
        self._jq_bars_for_all_spike.set_index('datetime', drop=True, inplace=True)

    # ------------------------------------------------------------------------------------------------------------------
    def save_jq_bars_for_spike5(self):
        save = True
        if self._update_and_check_jointquant_data():
            self._produce_jq_bars_for_all_spike()
            self._jq_bars_for_spike5 = self._jq_bars_for_all_spike.copy()
            # spike5 does not need amount
            self._jq_bars_for_spike5 = self._jq_bars_for_spike5.loc[
                :, ['open', 'high', 'low', 'close', 'ft']]

            self._jq_bars_for_spike5.to_csv(
                os.path.join(self._jointquant_data_dir, 'for spike 5', self._stock+'_trading_bars.csv'))
        else:
            save = False
        return save

    # ------------------------------------------------------------------------------------------------------------------
    def save_jq_bars_for_spike6(self):
        save = True
        if self._update_and_check_jointquant_data():
            self._produce_jq_bars_for_all_spike()
            self._jq_bars_for_spike6 = self._jq_bars_for_all_spike.copy()

            self._jq_bars_for_spike6.to_csv(
                os.path.join(self._jointquant_data_dir, 'for spike 6', self._stock+'_trading_bars.csv'))
        else:
            save = False
        return save

    # ------------------------------------------------------------------------------------------------------------------
    def _compare_ohlc(self):
        # compare ohlc difference
        compare_pass = True

        df_local = self._trading_bars.loc[self._todayStr:, :].copy()
        # df_jq = self._jq_bars_for_spike6.loc[self._todayStr:, :].copy()
        df_jq = self._jq_bars_for_all_spike.loc[self._todayStr:, :].copy()

        df_jq.rename(columns={'open': 'open_jq', 'high': 'high_jq', 'low': 'low_jq', 'close': 'close_jq'}, inplace=True)
        df_local.rename(columns={'open': 'open_lo', 'high': 'high_lo', 'low': 'low_lo', 'close': 'close_lo'},
                        inplace=True)
        df_jq.loc[:, 'on'] = range(0, len(df_jq))
        df_local.loc[:, 'on'] = range(0, len(df_local))

        df_both = pd.merge(df_local, df_jq, on='on')

        df_both.loc[:, 'o_diff'] = df_both.loc[:, 'open_lo'] - df_both.loc[:, 'open_jq']
        df_both.loc[:, 'h_diff'] = df_both.loc[:, 'high_lo'] - df_both.loc[:, 'high_jq']
        df_both.loc[:, 'l_diff'] = df_both.loc[:, 'low_lo'] - df_both.loc[:, 'low_jq']
        df_both.loc[:, 'c_diff'] = df_both.loc[:, 'close_lo'] - df_both.loc[:, 'close_jq']
        # df_both.loc[:, 'o_min'] = min(df_jq.loc[:, 'open'], df_spike2.loc[:, 'open'])
        for i in range(len(df_both)):
            df_both.loc[i, 'o_min'] = min(df_both.loc[i, 'open_lo'], df_both.loc[i, 'open_jq'])
            df_both.loc[i, 'h_min'] = min(df_both.loc[i, 'high_lo'], df_both.loc[i, 'high_jq'])
            df_both.loc[i, 'l_min'] = min(df_both.loc[i, 'low_lo'], df_both.loc[i, 'low_jq'])
            df_both.loc[i, 'c_min'] = min(df_both.loc[i, 'close_lo'], df_both.loc[i, 'close_jq'])

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
            self._email_and_print('%s has significant differences in ohlc between trading and jq bars.' % self._stock)
            print(df_both.loc[:, 'open_lo':'close_lo'])
            print(df_both.loc[:, 'open_jq':'close_jq'])
            print(df_both.loc[:, 'o_diff':'c_diff'])
            print(df_both.loc[:, 'o_min':'c_min'])
            print(df_both.loc[:, 'o_diff%':'c_diff%'])
            print(df_both.loc[:, 'o_diff%>threshold':'c_diff%>threshold'])
            compare_pass = False

        return compare_pass

    # ------------------------------------------------------------------------------------------------------------------
    def _compare_amount(self):
        # compare ohlc difference
        compare_pass = True

        df_local = self._trading_bars.loc[self._todayStr:, :].copy()
        df_jq = self._jq_bars_for_spike6.loc[self._todayStr:, :].copy()

        df_local.rename(columns={'amount': 'amount_lo'}, inplace=True)
        df_jq.rename(columns={'amount': 'amount_jq'}, inplace=True)

        df_jq.loc[:, 'on'] = range(0, len(df_jq))
        df_local.loc[:, 'on'] = range(0, len(df_local))

        df_both = pd.merge(df_jq, df_local, on='on')

        df_both.loc[:, 'a_diff'] = df_both.loc[:, 'amount_lo'] - df_both.loc[:, 'amount_jq']

        for i in range(len(df_both)):
            df_both.loc[i, 'a_min'] = min(df_both.loc[i, 'amount_lo'], df_both.loc[i, 'amount_jq'])

        df_both.loc[:, 'a_diff%'] = abs(df_both.loc[:, 'a_diff'] / df_both.loc[:, 'a_min'])

        # self._threshold = 0.0025
        df_both.loc[:, 'a_diff%>threshold'] = df_both.loc[:, 'a_diff%'] > self._threshold

        if df_both.loc[:, 'a_diff%>threshold'].sum():
            self._email_and_print('%s has significant differences in amount between trading bars jq bars.' % self._stock)
            print(df_both.loc[:, ['amount_lo', 'amount_jq', 'a_diff%', 'a_diff%>threshold']])
            compare_pass = False

        return compare_pass

    # ------------------------------------------------------------------------------------------------------------------
    def _set_trading_bars_dir(self, x):
        self._trading_bars_dir = x

    # ------------------------------------------------------------------------------------------------------------------
    def _get_trading_bars(self):
        file_path = os.path.join(self._trading_bars_dir, self._stock, self._stock+'_trading_bars.csv')
        if os.path.exists(file_path):
            self._trading_bars = pd.read_csv(file_path, parse_dates=['datetime'], index_col='datetime')
        else:
            raise Exception('%s trading bars does not exist!' % self._stock)

    # ------------------------------------------------------------------------------------------------------------------
    def _backup_trading_bars(self):
        previous_bars_path = os.path.join(
            self._trading_bars_dir, self._stock, self._stock+'_trading_bars_backup.csv')
        if os.path.exists(previous_bars_path):
            try:
                previous_bars = pd.read_csv(previous_bars_path, parse_dates=['datetime'], index_col='datetime')
                last_backup_dt = previous_bars.index.to_list()[-1]

                dt = (last_backup_dt + timedelta(minutes=30)).strftime('%Y-%m-%d %H:%M:%S')
                full = pd.concat([previous_bars, self._trading_bars.loc[dt:, :]])
                full.drop_duplicates(keep='last', inplace=True)
            except pd.errors.EmptyDataError:
                full = self._trading_bars
        else:
            full = self._trading_bars

        full.to_csv(previous_bars_path)

    # ------------------------------------------------------------------------------------------------------------------
    def _compare_local_to_jq(self):
        pass

        # if 'spike6' in self._trading_bars_dir:
        #     self.save_jq_bars_for_spike6()
        # elif 'spike5' in self._trading_bars_dir:
        #     self.save_jq_bars_for_spike5()
        # else:
        #     raise Exception('dir does not include spike6 nor spike5.')
        # # spike6_trading_bars_dir = os.path.join(
        # #     'C', os.sep, 'quant', 'spike6.0', 'realTrading', 'realTradingData'
        # #     )
        # # self._get_spike6_trading_bars()
        # self._get_trading_bars()
        # res = False
        # if 'spike6' in self._trading_bars_dir:
        #     if self._compare_amount() and self._compare_ohlc():
        #         res = True
        # else:
        #     if self._compare_ohlc():
        #         res = True

    # ------------------------------------------------------------------------------------------------------------------
    def replace_spike6_trading_bars(self):
        replaced = True
        if self.save_jq_bars_for_spike6():

            trading_bars_dir = os.path.join(
                'C', os.sep, 'quant', 'spike6.0', 'realTrading', 'realTradingData'
            )
            self._set_trading_bars_dir(trading_bars_dir)
            self._get_trading_bars()

            if self._compare_amount() and self._compare_ohlc():
                self._backup_trading_bars()
                self._jq_bars_for_spike6.to_csv(
                        os.path.join(
                            trading_bars_dir,
                            self._stock,
                            self._stock+'_trading_bars.csv'
                            )
                        )
            else:
                replaced = False
        replaced = False

        return replaced

    # ------------------------------------------------------------------------------------------------------------------
    def replace_spike5_trading_bars(self):
        replaced = True
        if self.save_jq_bars_for_spike5():

            trading_bars_dir = os.path.join(
                'C', os.sep, 'quant', 'spike5.0', 'realTrading', 'realTradingData'
            )
            self._set_trading_bars_dir(trading_bars_dir)
            self._get_trading_bars()

            if self._compare_ohlc():
                self._backup_trading_bars()
                self._jq_bars_for_spike5.to_csv(
                    os.path.join(
                        trading_bars_dir,
                        self._stock,
                        self._stock + '_trading_bars.csv'
                    )
                )
            else:
                replaced = False
        replaced = False

        return replaced


# -----------------------------------------------------------------------------
def replace_spike5_trading_bars(stock_list):
    for stock in stock_list:
        jqr = JointquantDataReplacement(stock)
        jqr.replace_spike5_trading_bars()
    print('Replace spike5 trading bars succeeded.')


# -----------------------------------------------------------------------------
def replace_spike6_trading_bars(stock_list):
    for stock in stock_list:
        jqr = JointquantDataReplacement(stock)
        jqr.replace_spike6_trading_bars()
    print('Replace spike6 trading bars succeeded.')


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    # -------------------------------------------------------------------------
    '''repalce trading bars'''
    stocks = ['600276']
    for stock in stocks:
        for stock in stocks:
            jqr = JointquantDataReplacement(stock)
            res = jqr.replace_spike6_trading_bars()
            print('repalce %s trading bars with jq data success: %s.' % (stock, res))

    # -------------------------------------------------------------------------
    '''get data for new tradings'''
    new_stocks = ['002532']
    for stock in new_stocks:
        jqr = JointquantDataReplacement(stock)
        res = jqr.save_jq_bars_for_spike6()
        print('save %s jq data success: %s.' % (stock, res))

    # -------------------------------------------------------------------------
    '''for stocks backtesting'''
    # stocks = ['002475', '000333', '000661', '000858', '600036',
    #           '600276', '600309', '603288', '601318']
    # stocks = ['600763']
    # folder_path = os.path.join('c:', os.sep, 'jqData',  'jqData')
    # get_jq_stock_data_for_backtesting(stocks, fre='1m') # '1d', '1w''

    # -------------------------------------------------------------------------
    '''get future data'''
    # future_data_path = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud",
    #                            "dataProcess", "future_daily_data")
    # futures= ['rb2105']
    # updateFutureDataWithJointquant(futures, future_data_path)
