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
