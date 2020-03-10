import requests
import pandas as pd
from datetime import datetime
from dateutil.parser import parse
from mailing import Email


# -----------------------------------------------------------------------------
def get_60m_data_from_web(instrument):
    if instrument[0:1] == '6':
        mark = 'sh'
    else:
        mark = 'sz'
    page = requests.get('http://ifzq.gtimg.cn/appstock/app/kline/mkline?param=' + mark + instrument + ',m60,,320&_var=m60_today')
    page_info = page.text
    minute_data = page_info.split("m60")[2].split("[")[2:]
    data = pd.DataFrame()
    data['mesage'] = minute_data
    data['datetime'] = data['mesage'].apply(lambda x: x.split(",")[0][1:-1])
    data['datetime'] = data['datetime'].apply(lambda x: parse(x))
    data['open'] = data['mesage'].apply(lambda x: float(x.split(",")[1][1:6]))
    data['close'] = data['mesage'].apply(lambda x: float(x.split(",")[2][1:6]))
    data['high'] = data['mesage'].apply(lambda x: float(x.split(",")[3][1:6]))
    data['low'] = data['mesage'].apply(lambda x: float(x.split(",")[4][1:6]))
    data = data[['datetime', 'open', 'high', 'low', 'close']]
    data.reset_index(drop=True, inplace=True)
    return data

# -----------------------------------------------------------------------------
def compare_stock_data(instruments, prefix, tradingBarsPath, error_tick=0):
    email = Email()
    email.set_subjectPrefix(prefix)
    error_data = pd.DataFrame()
    for instrument in instruments:
        web_data = get_60m_data_from_web(instrument)
        web_data = web_data[web_data['datetime'] > datetime.today().date()]
        web_data.reset_index(drop=True, inplace=True)        
        if len(web_data) == 0:
            email.send('get ' + instrument + 'web data error! Can not do compare!', str())
        else:
            path = tradingBarsPath + instrument + "\\" + instrument + "_trading_bars.csv"
            local_data = pd.read_csv(path)
            local_data = local_data[pd.to_datetime(local_data['datetime']) > datetime.today().date()]
            local_data.reset_index(drop=True, inplace=True)
            if len(local_data) == 0:
                email.send('get ' + instrument + 'local data error! Can not do compare!', str())
            compare_data = pd.merge(web_data, local_data, how='inner', left_index=True, right_index=True)
            compare_data['o'] = compare_data['open_x'] - compare_data['open_y']
            compare_data['h'] = compare_data['high_x'] - compare_data['high_y']
            compare_data['l'] = compare_data['low_x'] - compare_data['low_y']
            compare_data['c'] = compare_data['close_x'] - compare_data['close_y']
            compare_data['o_ratio'] = compare_data['o'] / compare_data['open_x']
            compare_data['h_ratio'] = compare_data['h'] / compare_data['high_x']
            compare_data['l_ratio'] = compare_data['l'] / compare_data['low_x']
            compare_data['c_ratio'] = compare_data['c'] / compare_data['close_x']
            compare_data['sig'] = instrument            
            if (len(compare_data[abs(compare_data['o_ratio']) > error_tick]) > 0) | (len(compare_data[abs(compare_data['h_ratio']) > error_tick]) > 0) | (len(compare_data[abs(compare_data['l_ratio']) > error_tick]) > 0) | (len(compare_data[abs(compare_data['c_ratio']) > error_tick]) > 0):
                new_local_data =  get_60m_data_from_web(instrument)
                new_local_data['ft'] = 0
                new_local_data.to_csv(path, index=0)
                stock_error = compare_data[['sig', 'datetime_x', 'o', 'h', 'l', 'c']]                
                stock_error.columns = ['stock', 'datetime', 'open', 'high', 'low', 'close']                
                print(stock_error)
                if len(error_data) == 0:
                    error_data = stock_error
                else:
                    error_data = pd.concat([error_data, stock_error])
    if len(error_data) != 0:
        email.send("stock_data_error, web-local", error_data.to_html(index=0, justify='left'))


if __name__ == '__main__':
    instruments = ['600009', '002008', '601318', '600309', '000333']
    ashare50TradingBarsFolder = "C:\\applepy\\projects\\ashare\\docs\\5.0_realTradingData\\"
    prefix = 'applepy-5.0'
    compare_stock_data(instruments, prefix, ashare50TradingBarsFolder, 0.0005)
