# -*- coding: utf-8 -*-
import pymongo
import pandas  as pd
from mailing import Email
from datetime import datetime
import matplotlib.pyplot as plt
from keyInfo import cwhEmail, zzhEmail, zmEmail


# ------------------------------------------------------------------
def future_portfolio_pnl_csr(instrumentList):
    email = Email()
    email.set_subjectPrefix('csrprobot-pt')
    """从数据库载入策略的持仓情况"""
    myclient = pymongo.MongoClient('mongodb://localhost:27017/')        
    mydb = myclient['VnTrader_Position_Db']   
    collectionList = ['csrProbot_pt']
    future_pnl = pd.DataFrame(columns = ['id', 'ft', 'current_pnl', 'history_pnl', 'long', 'short'])
    for zhanghao in collectionList:
        collection = mydb[zhanghao]
        #instrumentList = ['AP910']
        equity = 0
        for instrument in instrumentList:
            symbol = instrument[:-4]
            print(symbol)               
            try:
                if int(symbol[-1]) >= 0:
                    symbol = symbol[0]
            except ValueError:
                pass                
            
            if symbol in ['AP', 'rb', 'ru', 'MA', 'bu', 'p', 'm']:
                lotSize = 10
            elif symbol in ['i', 'ZC']:
                lotSize = 100
            elif symbol in []:
                pass
            else:
                raise Exception('lot_size unknown')
                
                            
            flt = {'name': 'csrProbot strategy', 'vtSymbol': instrument}
            print(flt)
#                syncData = self.mainEngine.dbQuery(POSITION_DB_NAME, strategy.dataBaseName, flt)  # className has been replaced by dataBaseName, cwh
            try:
                data = list(collection.find(flt))[0]
                current_orders = data['current_orders']
                history_orders = data['history_orders']
                available = data['available']
                equity = equity + data['balance']
                current_pnl = 0
                long_volumes = 0
                short_volumes = 0
                if len(current_orders) > 0:            
                    for order in current_orders:
                        if current_orders[order]['direction'] == 'buy':
                            long_volumes += current_orders[order]['volume']
                            current_pnl += lotSize*current_orders[order]['volume']*(current_orders[order]['preSettle']-current_orders[order]['openPrice'])
                        else:
                            short_volumes += current_orders[order]['volume']
                            current_pnl += lotSize*current_orders[order]['volume']*(current_orders[order]['openPrice']-current_orders[order]['preSettle'])
                
                history_pnl = 0
                if len(history_orders) > 0: 
                    try:
                        for order in history_orders:
                            if history_orders[order]['direction'] == 'buy':
                                history_pnl += lotSize*history_orders[order]['volume']*(history_orders[order]['closePrice']-history_orders[order]['openPrice'])
                            else:
                                history_pnl += lotSize*history_orders[order]['volume']*(history_orders[order]['openPrice']-history_orders[order]['closePrice'])
                    except KeyError, e:
                        print(order, e)
                future_pnl.loc[future_pnl.shape[0]] = [zhanghao.split('_')[1], symbol, current_pnl, history_pnl, long_volumes, short_volumes]
            except:
                print('error in get pnl with ' + symbol)
#                pass
            # pnl['symbol'] = {'current_pnl': current_pnl, 'history_pnl': history_pnl}
        future_pnl.loc[future_pnl.shape[0]] = [u' 合计  ', '', future_pnl[future_pnl['id'] == zhanghao.split('_')[1]]['current_pnl'].sum(), future_pnl[future_pnl['id'] == zhanghao.split('_')[1]]['history_pnl'].sum(), '', '']
        
        if 'pt' in future_pnl['id'].values:
            future_pnl.loc[future_pnl.shape[0]] = [u'初始投资', '', u'各100万', '', '', '']
            future_pnl.loc[future_pnl.shape[0]] = [u'开始时间','', '2020-8-18 9:00','','','']
        else:
            future_pnl.loc[future_pnl.shape[0]] = [u'可用资金', '', available, '', '', '']
        future_pnl.loc[future_pnl.shape[0]] = ['', '', '', '', '', '']
        
    future_pnl.loc[future_pnl['id'] == 'pt', 'id'] = u'模拟交易'
    future_pnl.columns = [u' 帐号 ', u' 品种 ', u'持仓盈亏',u'历史盈亏',u'多(手)',u'空(手)']
    probot_pnl = equity - 4000000.0
    returns = probot_pnl / 4000000.0
#    his_balance = pd.DataFrame(columns=['date', 'equity', 'pnl', 'return'])
    his_balance = pd.read_csv('C:\\autotrade\\pt_csrprobot.csv')
    his_balance.loc[his_balance.shape[0]] = [datetime.today().strftime('%Y-%m-%d'), equity, probot_pnl, returns]
    his_balance.drop_duplicates('date', inplace=True)
    
    his_balance.to_csv('C:\\autotrade\\pt_csrprobot.csv', index=0)
    date = his_balance['date']
    ax_xticklabels = list(map(lambda x:str(x)[0:4]+str(x)[5:7]+str(x)[8:10], date))
    fig1, ax1 = plt.subplots(figsize=(10,7))
    ax1.plot(range(len(his_balance)),his_balance['equity'], label = 'pt_equity')
    ax1.set_xticks([i-1 for i in range(len(his_balance))])
    ax1.set_xticklabels(ax_xticklabels, rotation=45)
    for label in ax1.get_xticklabels():
        label.set_visible(False)
    for label in ax1.get_xticklabels()[::5]:
        label.set_visible(True)
    fig1.savefig('C:\\autotrade\\pt_equity_csr.png')
    fig2, ax2 = plt.subplots(figsize=(10,7))
    ax2.plot(range(len(his_balance)),his_balance['return'], label = 'pt_return')
    ax2.set_xticks([i-1 for i in range(len(his_balance))])
    ax2.set_xticklabels(ax_xticklabels, rotation=45)
    for label in ax2.get_xticklabels():
        label.set_visible(False)
    for label in ax2.get_xticklabels()[::5]:
        label.set_visible(True)
    fig2.savefig('C:\\autotrade\\pt_return_csr.png')
    print(future_pnl)
    email.send("csr_pnl",
               future_pnl.to_html(index=0, justify='left'),
               png=['C:\\autotrade\\pt_equity_csr.png', 
                    'C:\\autotrade\\pt_return_csr.png'],
               mailBox=[cwhEmail, zzhEmail, zmEmail])        
    return future_pnl


# -----------------------------------------------------------------------------
if __name__ == '__main__':
    future_portfolio_pnl_csr(["m2101", "rb2010", "p2101", "bu2012"])  # ["rb2010", "MA005"]
    