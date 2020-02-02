# -*- coding: utf-8 -*-

import pandas as pd
import matplotlib.pyplot as plt
from datautil import getDailyPrice
from datetime import datetime, time


#------------------------------------------------------------------------------
class Analysis(object):
    def __init__(self, equity, order, dfx, numOfShares=100, chartTitle='stock'):
        self.equity = equity
        self.order = order
        self.dfx = dfx
        self.numOfShares = numOfShares
        self.chartTitle = chartTitle
        self.ann_std = float()
        self.ann_return = float()
        self.interest_rate = 0.03
        self.lot_size = 100
        self.tick_size = 0.01
        self.slippage = 1
        self.commission = 0.0003
        self.stock = None
        self.stockDownSTD = float()
        self.stockReturn = float()
        self.stockSortino = float()
        self.stockDownSTDbymean = float()
        self.stockSortinobymean = float()
        
    def set_lot_size(self, x):
        self.lot_size = x
    
    def set_commission(self, x):
        self.commission = x
    
    def set_slippage(self, x):
        self.slippage = x
        
    def set_tick_size(self, x):
        self.tick_size = x

    def set_interest_rate(self, x):
        self.interest_rate = x
        
    def clean_equity(self):
        equity_c = self.equity.copy()
#        equity_c.columns = ['datetime', 'equity', 'underlying_shares', 'pocket_money']      
#        # original equity history has few values in the same day, keep the last and drop others
#        # date column will be dropped when it's used by groupby
#        # so we create a new column 'date_groupby' and use it by groupby
#        equity_c['date_groupby'] = equity_c['datetime']
#        equity_c = equity_c.groupby('date_groupby').last()
#        
#        equity_c.sort_values(by=['datetime'], inplace=True)
#        
#        equity_c.reset_index(drop=True, inplace=True)

        return equity_c

    def annual_return(self):
        equi = self.equity.copy()
        end_date = equi.loc[len(equi)-1, 'datetime']
        start_date= equi.loc[0, 'datetime']
        
        # span = n years
        span = (end_date-start_date).days/float(365)

        end_equity = equi.loc[len(equi)-1, 'equity']
        start_equity = equi.loc[0, 'equity']

        ret = pow((end_equity/start_equity), 1/(span)) - 1
        return ret     # "%.2f%%" % (ret * 100)

    def annual_std(self):
        eq = self.clean_equity()
        eq['equity_yesterday'] = eq['equity'].shift(1)
        eq['daily_ret'] = eq['equity']/eq['equity_yesterday']
        eq.dropna(inplace=True)
        return eq['daily_ret'].std() * pow(250, 0.5)
    
    def down_stdbymean(self):
        eq = self.clean_equity()
        eq['equity_yesterday'] = eq['equity'].shift(1)
        eq['daily_ret'] = eq['equity']/eq['equity_yesterday'] -1
        mean_ret = eq['daily_ret'].mean()
        eq = eq[eq['daily_ret'] <  mean_ret]
        eq['sqrt'] = (eq['daily_ret'] - mean_ret) ** 2

        return pow(eq['sqrt'].sum() / (len(eq) - 1), 0.5) * pow(250, 0.5)
    
    def down_std(self):
        eq = self.clean_equity()
        eq['equity_yesterday'] = eq['equity'].shift(1)
        eq['daily_ret'] = eq['equity']/eq['equity_yesterday'] -1
        risk_free_rate = pow(1+self.interest_rate, 1/250) - 1
        eq = eq[eq['daily_ret'] <  risk_free_rate]
        eq['sqrt'] = (eq['daily_ret'] - risk_free_rate) ** 2

        return pow(eq['sqrt'].sum() / (len(eq) - 1), 0.5) * pow(250, 0.5)
        
    def max_drawdown(self):
        df = self.clean_equity()
        # df.reset_index(drop=True, inplace=True) 
        # print(df)
        
        max_dd = -100
        for i in range(0, len(df), 1):
            for j in range(i+1, len(df), 1):
                if((df.loc[i, 'equity']-df.loc[j, 'equity'])/df.loc[i, 'equity']) > max_dd:
                    max_dd = (df.loc[i, 'equity']-df.loc[j, 'equity'])/df.loc[i, 'equity']
        # print(max_dd)
        return max_dd

    def orders_pnl(self):
        order = self.order.copy()
        if 0 != order.index[0]:
            order.reset_index(inplace=True)
        # order['open_price'].apply(float)
        # order['close_price'].apply(float)
        # order['close_price'].dtypes
        # print(order)
        order['pnl'] = ' '
        for i in range(len(order)):            
            if 'long' == order.loc[i, 'direction']:
                order.loc[i, 'pnl'] = (order.loc[i, 'close_price'] - order.loc[i, 'open_price']) \
                                      * order.loc[i, 'lot'] * self.lot_size \
                                      - order.loc[i, 'lot']*self.slippage*2*self.tick_size\
                                       - (order.loc[i, 'open_price']+order.loc[i, 'close_price'])*order.loc[i, 'lot']*self.lot_size*self.commission
            else:
                order.loc[i, 'pnl'] = (order.loc[i, 'open_price'] - order.loc[i, 'close_price']) \
                                    * order.loc[i, 'lot'] * self.lot_size \
                                    - order.loc[i, 'lot']*self.slippage*2*self.tick_size\
                                    - (order.loc[i, 'open_price']+order.loc[i, 'close_price'])*order.loc[i, 'lot']*self.lot_size*self.commission
        pnl_overlook = order[['strategy', 'pnl']]
        
        strategies = list(order['strategy'].drop_duplicates())

        stra1_wins = len(order[(order['strategy'] == strategies[0]) & (order['direction'] == 'long')
                               & (order['close_price'] > order['open_price'])]) + \
                                len(order[(order['strategy'] == strategies[0]) & (order['direction'] == 'short')
                                          & (order['close_price'] < order['open_price'])])
              
        stra1_losses = len(order[(order['strategy'] == strategies[0]) & (order['direction'] == 'long')
                                 & (order['close_price'] < order['open_price'])]) + \
                                len(order[(order['strategy'] == strategies[0]) & (order['direction'] == 'short')
                                          & (order['close_price'] > order['open_price'])])

        stra1_total_orders = stra1_wins + stra1_losses
        
        if len(strategies) == 2:
            stra2_wins = len(order[(order['strategy'] == strategies[1]) & (order['direction'] == 'long')
                                   & (order['close_price'] > order['open_price'])]) + \
                                    len(order[(order['strategy'] == strategies[1]) & (order['direction'] == 'short')
                                              & (order['close_price'] < order['open_price'])])
            
            stra2_losses = len(order[(order['strategy'] == strategies[1]) & (order['direction'] == 'long')
                                     & (order['close_price'] < order['open_price'])]) + \
                                    len(order[(order['strategy'] == strategies[1]) & (order['direction'] == 'short')
                                              & (order['close_price'] > order['open_price'])])
        
            stra2_total_orders = stra2_wins + stra2_losses
        else:
            stra2_wins = 0
            stra2_losses = 0
            stra2_total_orders = 0
        '''
        re_loss_orders = 0
        re_win_orders  = 0
        tr_loss_orders = 0
        tr_win_orders  = 0
        for i in range( len(order) ):
            if (order.loc[i, 'strategy']=='re') & (order.loc[i, 'direction'] == 'long'):
                if( float(order.loc[i,'close_price'] ) > float( order.loc[i,'open_price']) ):
                    re_win_orders+=1
                else:
                    re_loss_orders+=1
            elif (order.loc[i,'strategy']=='re') & (order.loc[i,'direction' ] == 'short' ):
                if order.loc[i,'close_price'] < order.loc[i,'open_price']:
                    re_win_orders+=1
                else:
                    re_loss_orders+=1  
                    
            elif (order.loc[i, 'strategy']=='re') & (order.loc[i, 'direction'] == 'long'):
                if order.loc[i,'close_price'] > order.loc[i,'open_price']:
                    tr_win_orders+=1
                else:
                    tr_win_orders+=1
            else:
                if order.loc[i,'close_price'] < order.loc[i,'open_price']:
                    tr_win_orders+=1
                else:
                    tr_loss_orders+=1
        '''
       
        try:
            stra1_win_rate = round(stra1_wins/stra1_total_orders, 2)
            stra1_loss_rate = round(stra1_losses/stra1_total_orders, 2)
            print(strategies[0] + ' strategy is executed!')
        except ZeroDivisionError:
            stra1_win_rate = 0
            stra1_loss_rate = 0
            
        try:
            stra2_win_rate = round(stra2_wins/stra2_total_orders, 2)
            stra2_loss_rate = round(stra2_losses/stra2_total_orders, 2)
            print(strategies[1] + ' strategy is executed!')
        except ZeroDivisionError:
            stra2_win_rate = 0
            stra2_loss_rate = 0
        
        if 1 == len(strategies):
            orders_summary = {strategies[0] + ' win orders':  [stra1_wins, stra1_win_rate],
                              strategies[0] + ' loss orders': [stra1_losses, stra1_loss_rate],
                              strategies[0] + ' total orders':[stra1_total_orders, stra1_win_rate + stra1_loss_rate]}

        else:
            orders_summary = {strategies[0] + ' win orders':  [stra1_wins, stra1_win_rate],
                              strategies[0] + ' loss orders': [stra1_losses, stra1_loss_rate],
                              strategies[0] + ' total orders': [stra1_total_orders, stra1_win_rate + stra1_loss_rate],
                              strategies[1] + ' win orders':  [stra2_wins, stra2_win_rate],
                              strategies[1] + ' loss orders': [stra2_losses, stra2_loss_rate],
                              strategies[1] + ' total orders': [stra2_total_orders, stra2_win_rate + stra2_loss_rate]}
            
        orders_summary = pd.DataFrame(orders_summary)
        orders_summary = orders_summary.T
        orders_summary.columns = ['orders', 'percentage']
        
        return pnl_overlook, orders_summary
    
        # orders = self.order
        # long_orders = len(orders[(orders['direction'] == 'long')])
        # short_orders = len(orders[(orders['direction'] == 'short')])
        # all_orders = len(orders)
        # win_orders = len(orders[(orders['direction'] == 'long') & (orders['close_price'] > orders['open_price'])]) + \
        #             len(orders[(orders['direction'] == 'short') & (orders['close_price'] < orders['open_price'])])
        # loss_orders = all_orders - win_orders
        # win_rate = win_orders / float(all_orders)
        # loss_rate = loss_orders / float(all_orders)

    def sharpe_ratio(self):
        # print('benchmark rate is:' + str(self.benchmark))
        return (self.annual_return() - self.interest_rate) / self.annual_std()
    
    def sortino_ratio(self):
        return (self.annual_return() - self.interest_rate) / self.down_std()
    
    def sortino_ratiobymean(self):
        return (self.annual_return() - self.interest_rate) / self.down_stdbymean()

    
    def stock_sortino(self):
        start_date= datetime.combine(self.equity.loc[0, 'datetime'].date(), time(8))
        end_date = self.equity.loc[len(self.equity)-1, 'datetime']
        
        self.stock = getDailyPrice(start_date, end_date, self.dfx)
        
        stock_ = self.stock.copy()
        span = (end_date-start_date).days/float(365)

        end_price = stock_.loc[len(stock_)-1, 'close']
        start_price = stock_.loc[0, 'close']

        self.stockReturn = pow((end_price/start_price), 1/(span)) - 1

        stock_['yesterday_close'] = stock_['close'].shift(1)
        stock_['daily_ret'] = stock_['close']/stock_['yesterday_close'] - 1
        risk_free_rate = pow(1+self.interest_rate, 1/250) - 1
        stock_ = stock_[stock_['daily_ret']<risk_free_rate]
        stock_['sqrt'] = (stock_['daily_ret']-risk_free_rate) ** 2
        
        self.stockDownSTD = pow(stock_['sqrt'].sum() / (len(stock_) - 1), 0.5) * pow(250, 0.5)
        
        self.stockSortino = (self.stockReturn - self.interest_rate) / self.stockDownSTD
        
    def stock_sortinobymean(self):
        start_date= datetime.combine(self.equity.loc[0, 'datetime'].date(), time(8))
        end_date = self.equity.loc[len(self.equity)-1, 'datetime']
        
        self.stock = getDailyPrice(start_date, end_date, self.dfx)
        
        stock_ = self.stock.copy()
        span = (end_date-start_date).days/float(365)

        end_price = stock_.loc[len(stock_)-1, 'close']
        start_price = stock_.loc[0, 'close']

        stockReturn = pow((end_price/start_price), 1/(span)) - 1

        stock_['yesterday_close'] = stock_['close'].shift(1)
        stock_['daily_ret'] = stock_['close']/stock_['yesterday_close'] - 1
#        risk_free_rate = pow(1+self.interest_rate, 1/250) - 1
        stock_mean_ret = stock_['daily_ret'].mean()
        stock_ = stock_[stock_['daily_ret']< stock_mean_ret]
        stock_['sqrt'] = (stock_['daily_ret']-stock_mean_ret) ** 2
        self.stockDownSTDbymean = pow(stock_['sqrt'].sum() / (len(stock_) - 1), 0.5) * pow(250, 0.5)
        self.stockSortinobymean = (stockReturn - self.interest_rate) / self.stockDownSTDbymean
        
    def equity_line(self):
        e = self.clean_equity()
        plt.plot(e['datetime'], e['equity'])
        plt.xticks(rotation=90)
        plt.xlabel('Datetime')
        plt.ylabel('equity')
        plt.title('equity history')
        plt.show()

    def summary(self):
        e1=self.clean_equity()
        self.equity_line()
        self.stock_sortino()
        self.stock_sortinobymean()

        s1 = {'start date':   (e1.loc[0,         'datetime'].strftime('%Y-%m-%d'))}
        s2 = {'end date':     (e1.loc[len(e1)-1, 'datetime'].strftime('%Y-%m-%d'))}
        s3 = {'start equity': e1.loc[0,         'equity']}
        s4 = {'final equity': e1.loc[len(e1)-1, 'equity']}
        s5 = {'equity annual return': "%.2f%%" % (self.annual_return() * 100)}
        s6 = {'equity std': "%.2f%%" % (self.annual_std() * 100)}
        s7 = {'equity sharpe': round(self.sharpe_ratio(), 4)}
        s8 = {'equity sortino': self.sortino_ratio()}
        s9 = {'equity sortino by mean': self.sortino_ratiobymean()}
        s10 = {'free interest rate': self.interest_rate}
        s11 = {'equity max drawdown': "%.2f%%" % (self.max_drawdown()*100)}
        s12 = {'equity down STD': "%.4f%%" % (self.down_std() * 100)}
        s13 = {'equity down STD by mean': "%.4f%%" % (self.down_stdbymean() * 100)}
        s14 = {'stock down STD': "%.4f%%" % (self.stockDownSTD * 100)}
        s15 = {'stock down STD by mean': "%.4f%%" % (self.stockDownSTDbymean * 100)}
        s16 = {'stock return': "%.2f%%" % (self.stockReturn * 100)}
        s17 = {'stock sortino': round(self.stockSortino, 4)}
        s18 = {'stock sortino by mean': round(self.stockSortinobymean, 4)}
        s_dict = {}
        s_dict.update(s1)
        s_dict.update(s2)
        s_dict.update(s3)
        s_dict.update(s4)
        s_dict.update(s5)
        s_dict.update(s6)
        s_dict.update(s7)
        s_dict.update(s8)
        s_dict.update(s9)
        s_dict.update(s10)
        s_dict.update(s11)
        s_dict.update(s12)
        s_dict.update(s13)
        s_dict.update(s14)
        s_dict.update(s15)
        s_dict.update(s16)
        s_dict.update(s17)
        s_dict.update(s18)
        s_dict = pd.DataFrame(s_dict, index=[0])
        s_dict = s_dict.T
        s_dict.columns = ['result']
        print(s_dict)

        pnl, order_h = self.orders_pnl()
       
        pnl = pnl.sort_values(by=['strategy'])
        labels = pnl['strategy'].values.tolist()   
        num_list = pnl['pnl'].values.tolist()        
        plt.bar(range(len(num_list)), num_list, color='green', tick_label=labels)
        plt.title('orders profit and loss')
        plt.xticks(rotation=90)
        plt.xlabel('strategies')
        plt.ylabel('pnl')
        plt.figure(num=2, figsize=(40, 20))

        plt.show()

        '''
        width = 0.4
        ind = np.linspace(1,5,len(pnl))
        # make a square figure
        fig = plt.figure(1)
        ax  = fig.add_subplot(111)
        # Bar Plot
        ax.bar(ind-width/2,quants,width,color='green')
        # Set the ticks on x-axis
        ax.set_xticks(ind)
        ax.set_xticklabels(labels)
        # labels
        ax.set_xlabel('strategy')
        ax.set_ylabel('pnl')
        # title
        ax.set_title('orders profit and loss', bbox={'facecolor':'0.8', 'pad':5})
        plt.grid(True)
        plt.show()
        plt.close()
        '''

        print(order_h)

        comp = self.equity.loc[:, ['datetime', 'equity']]
        comp['stock_value']=self.stock.loc[:, 'close'] * self.numOfShares 
        comp.set_index('datetime', inplace=True, drop=True)
        comp[['equity', 'stock_value']].plot(title=self.chartTitle, rot=45)

        return s_dict

    def print_pdf(self):
        pass
