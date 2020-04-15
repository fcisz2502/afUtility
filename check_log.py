# -*- coding: utf-8 -*-
import pandas as pd
from datetime import date
from mailing import Email

def check_log(path, instruments):
    email = Email()
    for instrument in instruments:
        data = pd.read_table(path + instrument +'\\' + instrument + '_trading.log', header=None)
        data.columns = ['info']
        data['datetime'] = data['info'].apply(lambda x: x.split(' - ')[0])
        data['message'] = data['info'].apply(lambda x: x.split(' - ')[3])
        data['datetime'] = pd.to_datetime(data['datetime'])
        data = data[data['datetime'] >= date.today()]
        data.reset_index(drop=True, inplace=True)
        data = data[['datetime', 'message']]
        error_data = pd.DataFrame(columns = ['datetime', 'error'])
        for i  in range(len(data)):
            if ('failed' in data.loc[i, 'message']) or ('error' in data.loc[i, 'message']):
                error_data.loc[error_data.shape[0]] = data.loc[i].values
        if len(error_data) > 0:
            email.send("log Error!", error_data.to_html(index=0, justify='left'))


if __name__ == "__main__":
    path =  'C:\\quant\\spike5.0\\realtrading\\realTradingData\\'
    instruments = ['002008', '000333', '600276', '000661', '000858',
                   "601318", "600009", "603288", "600309"]
    check_log(path,instruments)