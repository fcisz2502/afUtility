# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
from datetime import datetime, time, timedelta


tbSymbol = {"MA": "MA2", "rb": "rb", "bu": "bu"}

# --------------------------------------------------------------------------------------
def getTbInstrumentList(instrumentList):
    tbInstrumentList = list()
    for vtSymbol in instrumentList:
        symbol = vtSymbol[:2]
        try:
            if int(symbol[1]) >= 0:
                symbol = symbol[0]
        except:
            pass

        tbPath = os.path.join(os.sep * 2, "FCIDEBIAN", "FCI_Cloud", "dataProcess",
                              tbSymbol[symbol] + vtSymbol[len(symbol):] + "(1分钟).csv")
        if 3 == sys.version_info[0]:
            import codecs
            tbPath = codecs.open(tbPath, "r", encoding='utf-8')
        elif 2 == sys.version_info[0]:
            tbPath = unicode(tbPath, "utf8")
        else:
            raise Exception("python version error!")

        df1 = pd.read_csv(tbPath, names=['datetime', 'open', 'high', 'low', 'close', 'trade', 'volume'],
                          parse_dates=['datetime'], index_col='datetime')

        if df1.index[-1] > datetime.combine(datetime.now().date(), time(14, 58)):
            tbInstrumentList.append(vtSymbol)

    return tbInstrumentList


# -----------------------------------------------------------------------------
def tb1mTo60m(instrument, lenOfDF=500):
    tbDataPath = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "dataProcess")
    infile = os.path.join(tbDataPath, instrument+'(1分钟).csv')
    if 2 == sys.version_info[0]:
        # python2.x
        unifile = unicode(infile , "utf8")
    elif 3 == sys.version_info[0]:
        # python3.x
        import codecs
        unifile = codecs.open(infile, 'r', encoding='utf-8')
    else:
        raise Exception("python version error, in afUtility.py.")
        
    df = pd.read_csv(unifile, 
                     names=['datetime', 'open', 'high', 'low', 'close', 'trade', 'volume'], 
                     parse_dates=['datetime'], 
                     index_col = 'datetime')
    
    df = df.tail(700*60)
    
    freq = '60min'
    df60 = df[['open']].resample(rule=freq).first()
    df60['high'] = df[['high']].resample(rule=freq).max()
    df60['low'] = df[['low']].resample(rule=freq).min()
    df60['close'] = df[['close']].resample(rule=freq).last()
    df60.dropna(inplace=True)
    
    df60 = df60.tail(lenOfDF)
    
    df60['askPrice1'] = 0
    df60['bidPrice1'] = 0
    df60['ft'] = 0
    df60['symbol'] = instrument
    df60 = df60[['symbol','open','high','low','close','askPrice1','bidPrice1','ft']]
#    df60.to_csv(os.path.join(tbDataPath, '60_for_vnpy' , instrument+'_df_60m.csv'))
    return df60
