# -*- coding: utf-8 -*-
import tushare as ts
import pandas as pd
import talib
import numpy as np
import matplotlib.pyplot as plt
import requests
import time
from keyInfo import token

# --------------------------------------------------------------------------------------
tick = int(time.time() * 1000)
a = time.strftime('%Y.%m.%d', time.localtime(time.time()))
a = a.split('.')[0] + a.split('.')[1] + a.split('.')[2]
ts.set_token(token)
pro = ts.pro_api()
start_date = '20170101'
end_date = a
IH = pro.fut_daily(ts_code='IHL2.CFX', start_date=start_date, end_date=end_date)  # 上证50
IH = IH[['trade_date', 'close']]
IH.columns = ['trade_date', 'IH']
IH = IH.sort_values('trade_date')
IH.reset_index(drop=True, inplace=True)
ih_page = requests.get('https://hq.sinajs.cn/?_=' + str(tick) + '/&list=CFF_RE_IH2003')
ih_close = float(ih_page.text.split(',')[3])
IH.loc[IH.shape[0]] = [a, ih_close]
IC = pro.fut_daily(ts_code='ICL2.CFX', start_date=start_date, end_date=end_date)  # 中证500
IC = IC[['trade_date', 'close']]
IC.columns = ['trade_date', 'IC']
IC = IC.sort_values('trade_date')
IC.reset_index(drop=True, inplace=True)
ic_page = requests.get('https://hq.sinajs.cn/?_=' + str(tick) + '/&list=CFF_RE_IC2003')
ic_close = float(ic_page.text.split(',')[3])
IC.loc[IH.shape[0]] = [a, ic_close]
data = pd.merge(IC, IH, how='inner', left_on='trade_date', right_on='trade_date')
data = data.sort_values('trade_date')
data.reset_index(drop=True, inplace=True)
data['IH1'] = data['IH'] * 300
data['IC1'] = data['IC'] * 200
data['IH'] = data['IH'] / data.loc[0, 'IH']
data['IC'] = data['IC'] / data.loc[0, 'IC']
data['IH1-IC1'] = data['IH'] - data['IC']
data['RSI_11D'] = talib.RSI(np.array(data['IH1-IC1']), timeperiod=11)
data['RSI_21D'] = talib.RSI(np.array(data['IH1-IC1']), timeperiod=21)
data.set_index(pd.to_datetime(data['trade_date']), inplace=True)
data['new'] = data['IH1'] - data['IC1']
data['boll_mid'] = data['IH1-IC1'].rolling(60).mean()
data['std'] = data['IH1-IC1'].rolling(60).std()
data['boll_up'] = data['boll_mid'] + 2 * data['std']
data['boll_down'] = data['boll_mid'] - 2 * data['std']
data['IH1-IC1'].plot(color='k', alpha=0.7, label='IH-IC')


d = data.loc['2019-06-01':, :]
d.to_csv('IH-IC__.csv')
# plt.ylabel('IH-IC')
# data['boll_mid'].plot(color='r', alpha=0.7, label='boll_mid', secondary_y=True)
# data['boll_up'].plot(color='g', alpha=0.7, label='boll_up', secondary_y=True)
# data['boll_down'].plot(color='y', alpha=0.7, label='boll_down', secondary_y=True)
# plt.ylabel("BOLL")
# plt.legend(loc='upper left')
# plt.title('BOLL_60D')
#
#
#
# data['IH1-IC1'].plot(color='k', alpha=0.7)
# plt.ylabel('IH-IC')
# data['RSI_21D'].plot(color='r', alpha=0.7, label='RSI_21D', secondary_y=True)
# data['RSI_11D'].plot(color='g', alpha=0.7, label='RSI_11D', secondary_y=True)
# plt.ylabel('RSI')
# plt.legend(loc='upper left')
# plt.title('RSI_11D_21D')
#
#
# data['IH'].plot(color='r', alpha=0.7)
# data['IC'].plot(color='g', alpha=0.7)
# plt.ylabel('nomalization')
# plt.legend(loc='upper left')
# data['IH1-IC1'].plot(color='k', alpha=0.7, secondary_y=True)
# plt.ylabel('gap')
# plt.title('IH-IC')
# plt.show()





fig, axes = plt.subplots(1, 2)
plt.suptitle('IH50-IC500')
data['RSI_21D'].plot(ax=axes[0], color='r', alpha=0.7)
data['RSI_11D'].plot(ax=axes[0], color='g', alpha=0.7)
data['IH1-IC1'].plot(ax=axes[0], color='k', alpha=0.7, secondary_y=True)
plt.legend(loc='upper left')
plt.title('RSI_11D_21D')
data['IH1-IC1'].plot(ax=axes[1],color='k', alpha=0.7, label='IH-IC')
plt.ylabel('IH-IC')
data['boll_mid'].plot(ax=axes[1],color='r', alpha=0.7, label='boll_mid', secondary_y=True)
data['boll_up'].plot(ax=axes[1],color='g', alpha=0.7, label='boll_up', secondary_y=True)
data['boll_down'].plot(ax=axes[1],color='y', alpha=0.7, label='boll_down', secondary_y=True)
plt.ylabel("BOLL")
plt.legend(loc='upper left')
plt.title('BOLL_60D')
plt.show()


