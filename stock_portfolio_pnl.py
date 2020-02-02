# encoding: UTF-8
from afUtility.mailing import Email
import pandas as pd
import pymongo
import requests
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime

# -----------------------------------------------------------------------------
def stock_portfolio_pnl():
    email = Email()
    email.set_subjectPrefix('LN1-applepy4.0-pt')
    myclient = pymongo.MongoClient("mongodb://localhost:27017/")
    # dblist = myclient.list_database_names()
    mydb = myclient["aShare_Trading_DB"]
    stocks_db = mydb['spike_4.0']
    pnl_df = pd.DataFrame(columns=['stock', 'initial_shares', 'current_pnl', 'history_pnl', 'sig', 'win/loss'])
    for i in stocks_db.find():
        orders = i
        current_order = orders['current_orders']
        history_orders = orders['history_orders']
        initial_shares = orders['initial_shares']
        stock_id = orders['symbol']
        current_pnl = 0
        history_pnl = 0
        win_orders = 0
        loss_orders = 0
        if len(current_order) == 0:
            sig = u'正常'
        else:
            for x in current_order:
                if current_order[x]['direction'] == 'short':
                    sig = u'做空'
                if current_order[x]['direction'] == 'long':
                    sig = u'做多'
                current_pnl = current_pnl + current_order[x]['pnl']
        if len(history_orders) != 0:
            for y in history_orders:
                if history_orders[y]['pnl'] > 0:
                    win_orders += 1
                if history_orders[y]['pnl'] < 0:
                    loss_orders += 1
                history_pnl = history_pnl + history_orders[y]['pnl']
        win_loss = str(win_orders) + '/' + str(loss_orders)
        pnl_df.loc[pnl_df.shape[0]] = [stock_id, initial_shares, round(current_pnl, 2), round(history_pnl, 2), sig, win_loss]
    #position = pd.read_csv("C:\\autotrade\\DataFolder\\Position.csv")
    #position['price'] = position['MarketValue'] / position['Quantity']
    #position['stock'] = position['Stock'].apply(lambda x: x.split(".")[0])
    #position = position[['stock', 'price']] 
    #pnl = pd.merge(pnl_df, position, how='left', left_on='stock_id', right_on='stock')
    pnl = pnl_df
    for a in range(len(pnl)):
        #if pnl.loc[a, 'price'] == None:
        tick_data = pd.read_csv("C:\\applepy\\projects\\ashare\\docs\\4.0_realTradingData\\" + pnl.loc[a, 'stock'] +'\\'+pnl.loc[a, 'stock'] + '_td.csv' )
        pnl.loc[a, 'price'] = float(tick_data.loc[len(tick_data)-1, 'price'])
    pnl['present_value'] = pnl['initial_shares'] * pnl['price'] + pnl['current_pnl'] + pnl['history_pnl']
    pnl.loc[pnl['stock'] == '000333', 'start_price'] = 58.25 
    pnl.loc[pnl['stock'] == '601318', 'start_price'] = 85.46
    pnl.loc[pnl['stock'] == '002008', 'start_price'] = 40
    pnl.loc[pnl['stock'] == '600309', 'start_price'] = 56.17
    pnl.loc[pnl['stock'] == '600036', 'start_price'] = 37.58
    pnl.loc[pnl['stock'] == '600276', 'start_price'] = 87.52 
#    pnl.loc[pnl['stock'] == '600887', 'start_price'] = 31.52 
    pnl.loc[pnl['stock'] == '000858', 'start_price'] = 133.01 
    pnl.loc[pnl['stock'] == '000661', 'start_price'] = 447
    pnl.loc[pnl['stock'] == '603288', 'start_price'] = 107.51
    pnl.loc[pnl['stock'] == '600009', 'start_price'] = 78.75
    pnl.loc[pnl['stock'] == '000333', 'name'] = u'美的'
    pnl.loc[pnl['stock'] == '601318', 'name'] = u'平安'
    pnl.loc[pnl['stock'] == '002008', 'name'] = u'大族'
    pnl.loc[pnl['stock'] == '600309', 'name'] = u'万华'
    pnl.loc[pnl['stock'] == '600036', 'name'] = u'招行'
    pnl.loc[pnl['stock'] == '600276', 'name'] = u'恒瑞'
#    pnl.loc[pnl['stock'] == '600887', 'name'] = u'伊利'
    pnl.loc[pnl['stock'] == '000858', 'name'] = u'五粮液'
    pnl.loc[pnl['stock'] == '000661', 'name'] = u'长春'
    pnl.loc[pnl['stock'] == '603288', 'name'] = u'海天'
    pnl.loc[pnl['stock'] == '600009', 'name'] = u'上海机场'
    pnl['start_value'] = pnl['start_price'] * pnl['initial_shares']
    pnl = pnl[['name', 'start_value', 'present_value', 'current_pnl', 'history_pnl', 'sig', 'win/loss']]
    pnl['start_value'] = pnl['start_value'].apply(lambda x: int(x))
    pnl['present_value'] = pnl['present_value'].apply(lambda x: int(x))
    pnl['current_pnl'] = pnl['current_pnl'].apply(lambda x: int(x))
    pnl['history_pnl'] = pnl['history_pnl'].apply(lambda x: int(x))
    spike_pnl = pnl['current_pnl'].sum() + pnl['history_pnl'].sum()
    stock_pnl = pnl['present_value'].sum() - pnl['start_value'].sum() - pnl['current_pnl'].sum() - pnl['history_pnl'].sum()
    equity_history = pd.read_csv("C:\\autotrade\\spike.csv")
    add_value = 0
    last_base_money = equity_history['base_money'].iloc[-1]
    lot_value = equity_history['lot_value'].iloc[-1]
    equity = last_base_money + spike_pnl + stock_pnl + add_value
    lots = equity_history['lots'].iloc[-1]
    new_lots = lots + (add_value / lot_value)
    profit_ratio = round(equity / new_lots - 1, 2)   
#    account = pd.read_csv('C:\\autotrade\\DataFolder\\account.csv')
#    account = account[account['Account'] == 190000674392] 
#    account.reset_index(drop=True, inplace=True)
#    lever = round((float(account['AccountNetWorth'][0]) - float(account['BuyingPower'][0])) / (equity - spike_pnl), 2)
    pnl.loc[pnl.shape[0]] = [u"合计", pnl['start_value'].sum(), pnl['present_value'].sum(), pnl['current_pnl'].sum(),
                                   pnl['history_pnl'].sum(), '', '']
    pnl.loc[pnl.shape[0]] = [u"底仓盈亏", stock_pnl, u"策略盈亏", spike_pnl,u'收益率',profit_ratio,'']
    
    pnl.loc[pnl.shape[0]] = [u"开始时间", u"2020/1/1", u"初始资金",'2000000', "", '', '']
    pnl.columns = [u'股票',u'底仓资金', u'市值', u'持仓盈亏', u'历史盈亏', u'状态', u'盈/亏单']
   
    lot_value = equity / float(new_lots)
    base_money = last_base_money  + float(add_value)
    port_return = lot_value - 1
    last_stock_pnl = equity_history['stock_pnl'].iloc[-1]
    last_spike_pnl = equity_history['spike_pnl'].iloc[-1]
    fund_return = equity_history['fundamental_return'].iloc[-1]
    fund_return += (stock_pnl- last_stock_pnl) / float(lots)
    enhanced_return = equity_history['enhanced_return'].iloc[-1]
    enhanced_return += (spike_pnl -  last_spike_pnl) / float(lots)
    start_if300 = equity_history['000300'].iloc[0]
    page = requests.get('http://hq.sinajs.cn/?format=text&list=sh000300')
    page_info = page.text
    if300 = float( page_info.split(',')[3] )  # 当前价格
    equity_history.loc[equity_history.shape[0]] = [datetime.today().strftime('%Y-%m-%d'), add_value,new_lots, equity, stock_pnl, spike_pnl, lot_value, port_return, fund_return, enhanced_return,if300,if300/start_if300-1,base_money]
    equity_history.drop_duplicates('date', inplace=True)
    equity_history.to_csv("C:\\autotrade\\spike.csv", index=0)
    ytick = list(np.arange(-3, round(equity_history['portfolio_return'].max(),2) * 100 + 3, 1) / 100)
    equity_history['0'] = 0
    fig, ax = plt.subplots(figsize=(15,10))
    ax.plot(range(len(equity_history)),equity_history['portfolio_return'], label = 'portfolio_return')
    ax.plot(range(len(equity_history)),equity_history[ 'fundamental_return'], label = 'fundamental_return')
    ax.plot(range(len(equity_history)),equity_history[ 'enhanced_return'], label = 'enhanced_return')
    ax.plot(range(len(equity_history)),equity_history['000300_return'], label = '000300_return')    
    ax.set_xticks([i-1 for i in range(len(equity_history))])    
    plt.legend(loc='upper left')
    plt.title('spike performance')
    ax.plot(equity_history['0'], color='black')
    ax.set_yticks(ytick)
    ax.yaxis.set_label_position("right")
    ax.yaxis.tick_right()
    date = equity_history['date']
    #ax.xaxis.set_major_locator(mpl.ticker.MultipleLocator(base=len(equity_history)//10))
    ax_xticklabels = list(map(lambda x:str(x)[0:4]+str(x)[5:7]+str(x)[8:10], date))
    ax.set_xticklabels(ax_xticklabels, rotation=45)
    for label in ax.get_xticklabels():
        label.set_visible(False)
    for label in ax.get_xticklabels()[::5]:
        label.set_visible(True)
    fig.savefig('C:\\autotrade\\spike.png')
    email.send("spike_pnl", 
               pnl.to_html(index=0, justify='left'), 
               png=['C:\\autotrade\\spike.png'])    
    print(pnl)


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    stock_portfolio_pnl()