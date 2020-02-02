# -*- coding: utf-8 -*-
"""
Created on Wed Jan 15 17:05:51 2020

@author: FCI_VM2
"""
# -*- coding: utf-8 -*-
"""
Created on Wed Oct 30 11:20:22 2019

@author: FCI_VM2
"""
import pymongo
import pandas as pd
import matplotlib.pyplot as plt
import matplotlib.finance as mpf
#from matplotlib.pylab import date2num
from datetime import datetime
import matplotlib.ticker as ticker

def checkFutureOrder():
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    mydb = myclient["VnTrader_Position_Db"]
    stocks_db = mydb['ProbotStrategy_pt']
    for future in stocks_db.find():
        orders = future
        #mycol = mydb['601318_spike_3.x']
        history_orders = orders['history_orders']
        instrument = orders['vtSymbol'][0:2]
        his_order = pd.DataFrame(history_orders).T
        if len(his_order) != 0:    
            all_order = his_order
            all_order['mark'] = all_order.index
            all_order = all_order[['last_enter_datetime',  'average_enter_price', 'last_depart_datetime',  'average_depart_price', 'direction', 'mark']]
            all_order.columns = ['open_date', 'open_price', 'close_date', 'close_price', 'direction', 'mark'] 
            all_order['open_date'] = all_order['open_date'].apply(lambda x: x.strftime('%Y/%m/%d %H')+':00:00')
            all_order['close_date'] = all_order['close_date'].apply(lambda x: x.strftime('%Y/%m/%d %H')+':00:00')
            for x in range(len(all_order['close_date'])):
                print(all_order['close_date'][x][-8:])
                if all_order['close_date'][x][-8:] == '08:00:00':
                    all_order.ix[x, 'close_date'] = all_order['close_date'][x][:-8] + '09:00:00'

            start_date = all_order['open_date'].min()
            future_data = pd.read_csv('C:\\work\\'+ instrument + '888(1h).csv', header=None)
            future_data.columns = ['datetime', 'open', 'high', 'low', 'close', 'volume', 'has']
            stock_data  = future_data[['datetime', 'open', 'high', 'low', 'close', 'volume']]
            stock_data.datetime = stock_data.datetime.apply(lambda x: x+':00')
            stock_data = stock_data[stock_data['datetime'] >= start_date]
            stock_data.reset_index(drop=True, inplace=True)
            date_index = stock_data[['datetime']]
            date_index['number'] = date_index.index 
            all_order['open_date'] = all_order['open_date'].apply(lambda x:date_index.loc[x ==date_index['datetime'], 'number'].values[0])
            all_order['close_date'] = all_order['close_date'].apply(lambda x:date_index.loc[x ==date_index['datetime'], 'number'].values[0])
    #        stock_data.set_index('datetime', inplace=True)
    #        freq = '60T'
    ##        stock_data60 = pd.DataFrame()
    #        stock_data60 = stock_data[['open']].resample(rule=freq).first()
    #        stock_data60['high'] = stock_data[['high']].resample(rule=freq).max()
    #        stock_data60['low'] = stock_data[['low']].resample(rule=freq).min()
    #        stock_data60['close'] = stock_data[['close']].resample(rule=freq).last()
    #        stock_data60['volume'] = stock_data[['close']].resample(rule=freq).sum()
    #        stock_data60.dropna(inplace=True)
    #        stock_data = stock_data60
    #        stock_data['date'] = stock_data.index
    #        stock_data['date'] = pd.to_datetime(stock_data['datetime'])
    #        stock_data.date = stock_data.date.apply(lambda x: date2num(x))
            date_tickers = stock_data['datetime'].apply(lambda x: x[:13]).values
            stock_data['date'] = stock_data.index
            # stock_data['date'] = stock_data['date'].apply(lambda x: float(x+1))
            stock_data = stock_data[['date', 'open', 'close', 'high', 'low', 'volume']]
            data_mat = stock_data.as_matrix()
            fig,ax=plt.subplots(figsize=(13,12))
            fig.subplots_adjust(bottom=0.1)
            def format_date(x, pos=None):
                # 由于前面股票数据在 date 这个位置传入的都是int
                # 因此 x=0,1,2,...
                # date_tickers 是所有日期的字符串形式列表
                if x < 0 or x > len(date_tickers) - 1:
                    return ''
                return date_tickers[int(x)]


            # 用 set_major_formatter() 方法来修改主刻度的文字格式化方式
            ax.xaxis.set_major_formatter(ticker.FuncFormatter(format_date))
            ax.xaxis.set_major_locator(ticker.MultipleLocator(6))
            ax.grid(True)
            plt.xticks(rotation=90)
            mpf.candlestick_ochl(ax,data_mat,colordown='#53c156', colorup='#ff1717',width=0.3,alpha=1)
            # plt.show()
    #        order = all_order.loc['order14']
            for i in range(len(all_order)):
                order = all_order.iloc[i]
                if order['direction'] == 'buy':
                   # ax.annotate(order['mark'], xy=(order['close_date'], order['close_price']), xytext=(order['open_date'], order['open_price']),
                   #             arrowprops=dict(width= 2,headwidth=8,facecolor='red', shrink=0.05))
                   ax.plot([order['open_date'], order['close_date']],[order['open_price'], order['close_price']], marker='o', color='red')
                if order['direction'] == 'short':
                    # ax.annotate(order['mark'], xy=(737412, order['close_price']),
                    #            xytext=(737392, order['open_price']),
                    #            arrowprops=dict(width= 2,headwidth=8,facecolor='green', shrink=0.01))
                    ax.plot([order['open_date'], order['close_date']],[order['open_price'], order['close_price']],marker='o', color='green')
            plt.show()
            fig.savefig('C:\\svnT\\his_orders\\' + instrument + '.png')


if __name__ == '__main__':
    checkFutureOrder()

