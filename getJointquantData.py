import pandas as pd
import os
from pandas.tseries.offsets import Minute
from datetime import datetime, time, timedelta
import jqdatasdk as jq
from afUtility.keyInfo import jqAccount, jqPassword


# -----------------------------------------------------------------------------
def getJointquantData(stocks, dataSavingPath, fre='60m'):
    # stocks = ['002475', '000333', '000661', '000858', '600036', '600276', '600309', '600585', '603288', '601318', '600009']
    jq.auth(jqAccount, jqPassword)
    start_date = '2020-01-01'
    end_date = datetime.today().strftime("%Y-%m-%d")
    
    if not os.path.exists(dataSavingPath):
        os.makedirs(dataSavingPath)
        
    for stock in stocks:
        security = jq.normalize_code(stock)
        # df = get_bars(security, 100, unit='1m', fields=['date', 'open', 'high', 'low', 'close'],
        #               include_now=False, end_dt=None, fq_ref_date=None)
        df2 = jq.get_price(security, start_date, end_date, frequency=fre, skip_paused=True, fq='pre')
        df2 = df2.loc[:, ['open', 'high', 'low', 'close']]
        df2.loc[:, 'datetime'] = df2.index
        df2.reset_index(drop=True, inplace=True)
        data = df2.loc[:, ['datetime', 'open', 'high', 'low', 'close']]
        data.loc[:, 'ft'] = 0
        data.to_csv(os.path.join(dataSavingPath, stock +".csv"), index=0)
    
# for stock in stocks:
#     data['open'] = data['open'] * 1000
#     data['high'] = data['high'] * 1000
#     data['low'] = data['low'] * 1000
#     data['close'] = data['close'] * 1000
#     data['open'] = data['open'].apply(lambda x: round(x))
#     data['high'] = data['high'].apply(lambda x: round(x))
#     data['low'] = data['low'].apply(lambda x: round(x))
#     data['close'] = data['close'].apply(lambda x: round(x))
#     # data['date'] = data['date'].apply(lambda x: x.split("'")[1])
#     # data['time'] = data['time'].apply(lambda x: x.split("'")[1])
#     data['open'] = data['open'].apply(lambda x: int(x))
#     data['high'] = data['high'].apply(lambda x: int(x))
#     data['low'] = data['low'].apply(lambda x: int(x))
#     data['close'] = data['close'].apply(lambda x: int(x))
#     data['time'] = data['time'].apply(lambda x: str(x))
#     data['time'] = pd.to_timedelta(data['time']) - Minute(1)
#     data.loc[data['time'] < "12:00:00", 'time'] = data.loc[data['time'] < "12:00:00", 'time'] - Minute(30)
#     data['time'] = data['time'].apply(lambda x: str(x).split(" ")[2])
#     data['time'] = data['time'].apply(lambda x: x.split(":")[0] + ":" + x.split(":")[1])
#     # data['date'] = data['date'].apply(lambda x: x.strftime('%Y-%m-%d'))
#     data.to_csv(os.path.join(stocks_path, "mt4_" + stock + ".csv", index=0))


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    stocks = ['002475', '000333', '000661', '000858', '600036', 
              '600276', '600309', '600585', '603288', '601318', '600009']
    dataSavingPath = os.path.join('c:', os.sep, 'cwh',  'spike stocks')
    getJointquantData(stocks, dataSaveingPath)
