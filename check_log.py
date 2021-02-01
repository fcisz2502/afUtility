# -*- coding: utf-8 -*-
import pandas as pd
import os
from datetime import datetime, time
from afUtility.mailing import Email
from dateutil import parser


# ------------------------------------------------------------------------------
def check_log(log_dirs_list=None):
    if log_dirs_list is None:
        log_dirs_list = [
            os.path.join('c', os.sep, 'quant', 'spike5.0', 'realTrading', 'realTradingData'),
            os.path.join('c', os.sep, 'quant', 'spike6.0', 'realTrading', 'realTradingData')
            ]

    excluding_errors= [
        "get spike3 orders pnl failed: IndexError('list index out of range',)"
    ]

    email = Email()

    stocks_with_error = set()
    
    for log_dir in log_dirs_list:
        for root, dirs, files in os.walk(log_dir):
            # print(files)
            for file in files:
                if file[-3:] == 'log':
                    
                    data = pd.read_table(
                        os.path.join(root, file), 
                        encoding='unicode_escape',
                        header=None
                        )
                    data.columns = ['info']
                    
                    dt_list = []
                    for dt in [txt[:19] for txt in data.loc[:, 'info']]:
                        try:
                            dt_list.append(parser.parse(dt))
                        except Exception as e:
                            email.send(file[:6]+' ' +dt, repr(e))
                            dt_list.append(datetime.now())
                            
                    data.loc[:, 'datetime'] = dt_list
                    
                    if datetime.now().time() > time(15):
                        begin_datetime = datetime.combine(datetime.now().date(), time(12))
                    else:  # datetime.now().time() > time(11, 30):
                        begin_datetime = datetime.combine(datetime.now().date(), time(0))
                    
                    data = data[data['datetime'] >= begin_datetime]
                    data.reset_index(drop=True, inplace=True)
                    data = data.loc[:, ['datetime', 'info']]
                    for index, row in data.iterrows():
                        if 'error' in str.lower(row['info']) or 'fail' in str.lower(row['info']):
                            excluded = False
                            for error in excluding_errors:
                                if error in row['info']:
                                    excluded = True
                            if not excluded:
                                stocks_with_error.add(file[:6])
    if stocks_with_error:
        email.send('Error or failed infomation in %s trading.log.' % stocks_with_error, '')

    print('Check log is done!')


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    check_log()
