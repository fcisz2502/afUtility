# -*- coding: utf-8 -*-

import pymongo
import pandas as pd
import numpy as np
import os
import copy
from datetime import datetime, time
from pandas.tseries.offsets import Milli
from vnpy.trader.vtUtility import send_message
from keyInfo import cwhEmail

    
# -----------------------------------------------------
def local_vs_mongo_1500(machine_id, instrument_list, strategy, mail_receivers=[cwhEmail]):
    
    for instrument in instrument_list:
        print('start checking ' + instrument)
        
        # get symbol         
        symbol = instrument[:2]
        try:
            if int(symbol[-1]) >= 0:
                symbol = symbol[0]
        except ValueError:
            pass
        
        tick_size_dict = {'rb': 1, 'ru': 5, 'CF': 5, 'i':0.5,
                          'bu': 2, 'm': 1, 'p': 2, 'AP':1} 
      
        tick_size = tick_size_dict[symbol]
        
#        t2330 = ['CF']
#        t2300 = ['rb', 'ru', 'i', 'bu', 'm']       
#        if symbol in t2330:
#            night_end_time = time(23, 30)
#               
#        elif symbol in t2300:
#            night_end_time = time(23)
#            
#        else:
#            pass
        
#        morning_end_time = time(11,30)
#        afternoon_end_time = time(15)
        
        myclient = pymongo.MongoClient('mongodb://localhost:27017/')        
        mydb = myclient['VnTrader_Tick_Db']           
        collection = mydb[instrument]
        
        # when resample, we need open price or last trading seciton high price
        # and low price to get hidden lowPrice and HighPrice 
        dayHigh = None
        dayLow = None
        
        # -----------------------------------------------
        print('--1--, setting start time, end time, high and low price before fetching data')        
        if datetime.now().weekday() < 5:    
            print('--1.1 weekday--')
            #trading section start time and end time 
            if datetime.now().time() > time(15):
                print('--1.1.2 after 1500--')
                # get morning section highPrice and lowPrice            
                try:
                    morning_ticks = pd.DataFrame()          
                    for row2 in collection.find({'datetime': {'$gte': datetime.combine(datetime.now().date(), time(11, 29, 30)), 
                                                                  '$lt': datetime.combine(datetime.now().date(), time(11,30,2))}}, 
                                           projection=['datetime', 'lastPrice', 'highPrice', 'lowPrice']):
                        morning_ticks = morning_ticks.append(pd.DataFrame(row2, index=[0]))
                        
                    morning_ticks.reset_index(drop=True, inplace=True) 
                    
                    dayHigh = morning_ticks.loc[len(morning_ticks)-1, 'highPrice']
                    dayLow = morning_ticks.loc[len(morning_ticks)-1, 'lowPrice']
                
                except:
                    send_message(machine_id + instrument, ' ----cannot fetch dayHigh or dayLow from mongo----',
                                         ' ', mail_receivers)
                
                # afternoon seciont start time and end time
                dt = datetime.combine(datetime.now().date(), time(13, 29, 59))
                dt1 = datetime.combine(datetime.now().date(), time(15, 0, 2))
    #            dt2 = datetime.combine(datetime.now().date(), time(15))
                
            elif datetime.now().time() > time(11, 30):
                print('--1.1.3 after 1130--')         
                # morning section start time and end time
                dt = datetime.combine(datetime.now().date(), time(8, 59, 59))
                dt1 = datetime.combine(datetime.now().date(), time(11, 30, 2))
    #            dt2 = datetime.combine(datetime.now().date(), time(13, 30))
            
            elif datetime.now().time() < time(11, 30):
                pass
    
            else:
                pass
        
        elif datetime.now().weekday() == 5:
            print('--1.2 today is Saturday--')
#            try:
#                fn_open_ticks = pd.DataFrame()           
#                for row4 in collection.find({'datetime': {'$gte': datetime.combine(datetime.now().date()-pd.Timedelta('1 day'), time(20, 59)), 
#                                                              '$lt': datetime.combine(datetime.now().date()-pd.Timedelta('1 day'), time(21))}}, 
#                                       projection=['datetime', 'lastPrice', 'highPrice', 'lowPrice']):
#                    fn_open_ticks = fn_open_ticks.append(pd.DataFrame(row4, index=[0]))
#                    
#                fn_open_ticks.reset_index(drop=True, inplace=True)
#                    
#                dayHigh = fn_open_ticks.loc[0, 'lastPrice']   
#                dayLow = fn_open_ticks.loc[0, 'lastPrice']          
#
#            except:
#                send_message(machine_id + instrument, ' ----cannot fetch dayHigh or dayLow from mongo----',
#                                     ' ', mail_receivers)
#            dt = datetime.combine(datetime.now().date()-pd.Timedelta('1 days'), time(20, 59))
#            dt1 = datetime.combine(datetime.now().date()-pd.Timedelta('1 days'), time(23, 30, 2))
            
        else:
            print('--1.3 today is Sunday--')
#            try:
#                fn_open_ticks = pd.DataFrame()           
#                for row4 in collection.find({'datetime': {'$gte': datetime.combine(datetime.now().date()-pd.Timedelta('2 days'), time(20, 59)), 
#                                                              '$lt': datetime.combine(datetime.now().date()-pd.Timedelta('2 days'), time(21))}}, 
#                                       projection=['datetime', 'lastPrice', 'highPrice', 'lowPrice']):
#                    fn_open_ticks = fn_open_ticks.append(pd.DataFrame(row4, index=[0]))
#                    
#                fn_open_ticks.reset_index(drop=True, inplace=True)
#                    
#                dayHigh = fn_open_ticks.loc[0, 'lastPrice']   
#                dayLow = fn_open_ticks.loc[0, 'lastPrice']
#            
#                
#            except:
#                send_message(machine_id + instrument, ' ----cannot fetch dayHigh or dayLow from mongo----',
#                                     ' ', mail_receivers)
#            dt = datetime.combine(datetime.now().date()-pd.Timedelta('2 days'), time(20, 59))
#            dt1 = datetime.combine(datetime.now().date()-pd.Timedelta('2 days'), time(23, 30, 2))
                        
        print('\nlast trading scetion start time: ' + str(dt))
        print('last trading scetion end time: ' + str(dt1))
        print('\ngetting last trading section tick data')
        dftick = pd.DataFrame()
        
        # -----------------------------------------------
        # fetch date from database and put it in DataFrame
        for row in collection.find({'datetime': {'$gte': dt, '$lte': dt1}}, 
                                   projection=['datetime', 'lastPrice', 'highPrice', 'lowPrice']):
            dftick = dftick.append(pd.DataFrame(row, index=[0]))
    
        # get rid of _id column
        dftick.drop('_id', axis=1, inplace=True)
#        dftick = dftick[['datetime', 'lastPrice', 'highPrice', 'lowPrice']]
        
        dftick.reset_index(drop=True, inplace=True)
        
        dftick['time'] = dftick['datetime'].apply(lambda x: x.time())
        
        # ----------------------------------------------
        # move time forward or backward
#        if datetime.now().weekday() > 4 or \
#                datetime.now().time() > time(23, 30) or \
#                    datetime.now().time() < time(11, 30):
            # move night section's open time to 21:00 sharp
#        if dftick.loc[0, 'datetime'].time() > time(20, 50):
#            dftick['night_start_time'] = time(21)
#            dftick['night_start_comp'] = dftick['time'] < dftick['night_start_time'] 
#            dftick.loc[True==dftick['night_start_comp'], 'datetime'] = datetime.combine(
#                                dftick.loc[0, 'datetime'].date(), time(21))
#            
#            # move night section's close time
#            dftick['night_end_time'] = night_end_time
#            # dftick['night_end_comp'] = dftick['time'] >= dftick['night_end_time']
#
#            dftick.loc[dftick['time'] == dftick['night_end_time'], 'datetime'] = datetime.combine(
#                                dftick.loc[0, 'datetime'].date(), night_end_time) - Milli(2)
#
#            dftick.loc[dftick['time'] > dftick['night_end_time'], 'datetime'] = datetime.combine(
#                                dftick.loc[0, 'datetime'].date(), night_end_time) - Milli(1)
        
#        if datetime.now().time() > time(15):
        if dftick.loc[0, 'datetime'].time() > time(13, 20):
            dftick['afternoon_end_time'] = time(15)
            # dftick['afternoon_end_comp'] = dftick['time'] >= dftick['afternoon_end_time']

            dftick.loc[dftick['time'] == dftick['afternoon_end_time'], 'datetime'] = datetime.combine(
                                dftick.loc[0, 'datetime'].date(), time(15)) - Milli(2)

            dftick.loc[dftick['time'] > dftick['afternoon_end_time'], 'datetime'] = datetime.combine(
                                dftick.loc[0, 'datetime'].date(), time(15)) - Milli(1)
        
#        elif datetime.now().time() > time(11,30):
        elif dftick.loc[0, 'datetime'].time() > time(8, 50):
            dftick['morning_end_time'] = time(11,30)
            # dftick['morning_end_comp'] = dftick['time'] >= dftick['morning_end_time']

            dftick.loc[dftick['time'] == dftick['morning_end_time'], 'datetime'] = datetime.combine(
                                dftick.loc[0, 'datetime'].date(), time(11, 30)) - Milli(2)

            dftick.loc[dftick['time'] > dftick['morning_end_time'], 'datetime'] = datetime.combine(
                                dftick.loc[0, 'datetime'].date(), time(11,30)) - Milli(1)

        else:
            pass       
        
        dftick.sort_values(by='datetime', inplace=True)
        # print(dftick.tail(20))

        # get rid of time morning_end_time morning_end_comp column
        dftick = dftick[['datetime', 'lastPrice', 'highPrice', 'lowPrice']]
        
        # ----------------------------------------------------
        # get the hidden highPrice and lowPrice
        # if tick.highPrice higher than tick.lastPrice and higher than last tick.highPrice
        # we need to get that highPrice and use in resampling
        # why? cause that highPrice does not catched by lastPrice
        
        # hpgtlhlp = high price great than last high last price
        print('\ngetting hidden highPrice and lowPrice, dayHigh is %s, dayLow is %s' %(dayHigh, dayLow) )

        dftick['preHigh'] = dftick['highPrice'].shift(1)
        dftick.loc[0, 'preHigh'] = dayHigh
        
        dftick['preLow'] = dftick['lowPrice'].shift(1)
        dftick.loc[0, 'preLow'] = dayLow
        
        dftick['hpgtlhlp'] = (dftick['highPrice'] > dftick['preHigh']) \
            & (dftick['highPrice'] > dftick['lastPrice'])
        dftick['hpgtlhlp'] = dftick['hpgtlhlp'].apply(lambda x: int(x))
        
        # lpltlllp = low price less than last low last price
        dftick['lpltlllp'] = (dftick['lowPrice'] < dftick['preLow']) \
            & (dftick['lowPrice'] < dftick['lastPrice'])
        dftick['lpltlllp'] = dftick['lpltlllp'].apply(lambda x: int(x))     
        
        length = len(dftick)
        
        dftick.drop('preHigh', axis=1, inplace=True)
        dftick.drop('preLow', axis=1, inplace=True)
        
        # make the first two or last two ticks have different datetime
        if dftick.loc[0, 'datetime'] == dftick.loc[1, 'datetime']:
            dftick.loc[1, 'datetime'] = dftick.loc[1, 'datetime'] + Milli(10)
        if dftick.loc[len(dftick)-1, 'datetime'] == dftick.loc[len(dftick)-2, 'datetime']:
            dftick.loc[len(dftick)-2, 'datetime'] = dftick.loc[len(dftick)-2, 'datetime'] - Milli(10)

        # get the hidden highPrice and lowPrice in tick and put them in the series of lastPrice
        for i in range(length):
            if 0 == dftick.loc[i, 'hpgtlhlp'] and 0 == dftick.loc[i, 'lpltlllp']:
                pass
            
            elif dftick.loc[i, 'hpgtlhlp'] and dftick.loc[i, 'lpltlllp']:
                dtt1 = dftick.loc[i, 'datetime']  # dtt1 = dt temp 1        
                
                dftick.loc[dftick.shape[0]] = [dtt1 + Milli(1), dftick.loc[i, 'highPrice'], 0, 0, 0, 0]
                dftick.loc[dftick.shape[0]] = [dtt1 + Milli(2), dftick.loc[i, 'lowPrice'], 0, 0, 0, 0]
                dftick.loc[i, 'datetime'] = dtt1 + Milli(3)
            
            elif dftick.loc[i, 'hpgtlhlp'] and 0 == dftick.loc[i, 'lpltlllp']:
                dtt2 = dftick.loc[i, 'datetime']  # dtt1 = dt temp 2 
                
                dftick.loc[dftick.shape[0]] = [dtt2 + Milli(1), dftick.loc[i, 'highPrice'], 0, 0, 0, 0]
                
                dftick.loc[i, 'datetime'] = dtt2 + Milli(3)
        
            elif 0 == dftick.loc[i, 'hpgtlhlp'] and dftick.loc[i, 'lpltlllp']:
#            else:
                dtt3 = dftick.loc[i, 'datetime']
                dftick.loc[dftick.shape[0]] = [dtt3 + Milli(1), dftick.loc[i, 'lowPrice'], 0, 0, 0, 0]
                dftick.loc[i, 'datetime'] = dtt3 + Milli(3)

            else:
                pass
            
        dftick = dftick[['datetime', 'lastPrice']]        
        
        dftick.sort_values(by='datetime', inplace=True)
        dftick.reset_index(drop=True, inplace=True)
        print('first 10 rows of dftick')        
        print(dftick.head(10))
        
        # ---------------------------------------------------
        # cal time difference between ticks
        dftick['dtdiff'] = dftick['datetime'] - dftick['datetime'].shift(1)
        
        # gtts = great than time span
        if symbol == 'CF':  
            # cf may have two ticks in one second,
            # but these two ticks will have the same exact time
            # maybe it is not the problem of cf,
            # it is a problem with czce, I see same problem in PTA too
            dftick['time_span'] = pd.Timedelta('1.5 seconds')
        else:
            dftick['time_span'] = pd.Timedelta('1 seconds')
            
        dftick['gtts'] = (dftick['dtdiff'] > dftick['time_span']).apply(lambda x: int(x))    
        
        print(dftick[dftick['gtts']==True])         
        print('\nnumber of span gt preset is %d \n' % dftick['gtts'].sum())
        # great than time span will sum up to large number 
        # for CF and ru i as they are not active in some period
#        if dftick['gtts'].sum() >= 5:
#            if symbol == 'CF':     
#                pass
#            else:           
#                send_message(machine_id + instrument, 'number of time span great than 1 second is '+ str(dftick['gtts'].sum()),
#                             ' ', mail_receivers)    
#        else:
#            pass
        
        dftick = dftick[['datetime', 'lastPrice']]
        dftick['dt'] = dftick['datetime']
        dftick.set_index('dt', drop=True, inplace=True)
        
        # resample and get 60m data        
        freq = '60min'
        df60 = pd.DataFrame()
        
        df60 = dftick[['lastPrice']].resample(rule=freq).first()
        df60['open'] = df60['lastPrice']
        df60['high'] = dftick[['lastPrice']].resample(rule=freq).max()
        df60['low'] = dftick[['lastPrice']].resample(rule=freq).min()
        df60['close'] = dftick[['lastPrice']].resample(rule=freq).last()
        df60.dropna(inplace=True)
        df60 = df60[['open', 'high', 'low', 'close']] 
        print(df60)        
#        print(' ')
        
        df60['datetime'] = df60.index
        df60.reset_index(drop=True, inplace=True)       
        
        # ##################################################
#        filePath = 'C:\\vnpy-1.9.2\\examples\\'
        filePath = os.path.join('c:', os.sep, 'vnpy-1.9.2', 'examples')
#        folders = os.listdir(filePath)
        
        folders = list()
        
        for folder in os.listdir(filePath):
            if strategy in folder:
                folders.append(folder)
        
        print('\nfolders are: ')
        for folder in folders:
            print(folder)
        print(' ')
        
        for folder in folders:
            # i is simple and could be in any str
            #'_i' is easier to target
            if '_'+symbol in folder:
                if 'qsyh' in folder:
                    product_name = 'qsyh'
                    print('--------qsyh--------')
                    
                elif 'cfzy' in folder:
                    product_name = 'cfzy'
                    print('--------cfzy--------')
                
                else:
                    product_name = str() 
                    
#                doc = filePath + folder + '\\' + instrument + '_df_60m.csv'   
                doc = os.path.join(filePath, folder, instrument + '_df_60m.csv')
                local60m = pd.read_csv(doc, parse_dates=['datetime'])
                 
                rows = df60.shape[0]
                local60m = local60m.tail(rows)
                local60m.reset_index(drop=True, inplace=True)
                
                # compare bar hour
                i = 0
                while i < rows:
                    if (df60.loc[i, 'datetime'].time().hour == 
                            local60m.loc[i, 'datetime'].time().hour):
                        print(instrument + ' bar hour is right')
                        i += 1
                    else:
                        send_message(machine_id + instrument + ' ' + product_name,
                                     ' ----BAR TIME NOT THE SAME----', 
                                     ' ', mail_receivers)
                        break
                
        #        for i in range(rows):
        #            if (df60.loc[i, 'datetime'].time().hour == 
        #                local60m.loc[i, 'datetime'].time().hour):
        #                print(instrument+' hour is right')
        #            else:
        #                send_message(machine_id + instrument, ' ----BAR TIME NOT THE SAME----', ' ',
        #                             mail_receivers) 
        
                # compare ohlc price
                comp60 = pd.DataFrame(abs(df60['open']-local60m['open']))
                comp60['high'] = abs(df60['high'] - local60m['high'])
                comp60['low'] = abs(df60['low'] - local60m['low'])
                comp60['close'] = abs(df60['close'] - local60m['close'])
                
                comp60['tick_size'] = tick_size
                
                # check if difference great than tick size
                comp60['ocheck'] = (comp60['open']  > comp60['tick_size']).apply(lambda x: int(x))
                comp60['hcheck'] = (comp60['high']  > comp60['tick_size']).apply(lambda x: int(x))
                comp60['lcheck'] = (comp60['low']   > comp60['tick_size']).apply(lambda x: int(x))
                comp60['ccheck'] = (comp60['close'] > comp60['tick_size']).apply(lambda x: int(x))
                   
                comp60['dt'] = local60m['datetime']   
                comp60.set_index('dt', inplace=True, drop=True)
                
                # charge format to avoid displaying value like 0.000000e+00
                print(' ')        
                if symbol in ['i', 'ZC']:       
                    print(comp60.astype(np.float).T)
                
                elif symbol in ['rb', 'ru', 'CF', 'bu', 'p', 'm', 'AP']:
                    print(comp60.astype(np.int).T)
                
                else:
                    print(comp60.astype(np.float).T)

                difference = comp60[['ocheck', 'hcheck', 'lcheck', 'ccheck']].sum().sum()         
                
                # send notice if there is one or more difference great than tick size 
                if difference > 0:     
                    # adjust to make the df readable in email        
                    comp60['dt'] = comp60.index
                    comp60['dt'] = comp60['dt'].apply(lambda x: str(x)[5:19])
                    comp60.set_index('dt', drop=True, inplace=True)
                    send_message(machine_id + instrument + ' ' + product_name,
                                 ' check CTP data', 
                                 comp60.astype(np.int64).T.to_string(), 
                                 mail_receivers)    
                else:
                    pass
            
                print('\n' + instrument + ' ' + product_name + ' data check done!\n')


# ----------------------------------------------------------------------------
if __name__ == "__main__":
    machine_id = 'VM2 '
    local_vs_mongo_1500(machine_id, ['AP910'], 'Probot')

