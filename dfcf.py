# -*- coding: utf-8 -*-
import requests
import pandas as pd
from datetime import datetime
instruments = ['002008', '000333', '000661', '000858', '600036', '600276', '600309', '600887', '603288', '601318', '600009']
for instrument in instruments:
    print(instrument)
    if instrument[0] == '0':
        ad = '2'
    else:
        ad = '1'
    data = requests.get('http://pdfm.eastmoney.com/EM_UBG_PDTI_Fast/api/js?rtntype=4&id=' + instrument + ad + '&type=m60k&_=1546486249481').text
    data1 = eval(data)
    hour_data = pd.DataFrame()
    for x in data1:
        x1 = pd.DataFrame.from_dict(x, orient='index').T
        if len(hour_data) == 0:
            hour_data = x1
        else:
            hour_data = pd.concat([hour_data, x1])
    hour_data = hour_data[['time', 'open', 'high', 'low', 'close']]
    hour_data['open'] = hour_data['open'].apply(lambda x: round(float(x), 2))
    hour_data['high'] = hour_data['high'].apply(lambda x: round(float(x), 2))
    hour_data['low'] = hour_data['low'].apply(lambda x: round(float(x), 2))
    hour_data['close'] = hour_data['close'].apply(lambda x: round(float(x), 2))
    hour_data['time'] = hour_data['time'] +':00'
    hour_data = hour_data[hour_data['time'] > datetime.now().strftime("%Y-%m-%d")]
    hour_data.reset_index(drop=True, inplace=True)
    hour_data.columns = ['datetime', 'open', 'high', 'low', 'close']
    his_hour_data = pd.read_csv("\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\dfcf_data\\" + instrument + '_hour_data.csv')
    new_hour_data = pd.concat([his_hour_data, hour_data])
    new_hour_data.to_csv("\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\dfcf_data\\" + instrument + '_hour_data.csv', index=0)
