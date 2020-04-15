# -*- coding: utf-8 -*-
import sys
import os
import pandas as pd
import threading
from datetime import datetime, time, timedelta
from afUtility.mailing import Email
from afUtility.keyInfo import cwhEmail, zmEmail, zzhEmail


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


# -----------------------------------------------------------------------------
def coverCtaTradingBarsWithTbData(instrumentList, ctaTradingBarFolderDict):
    for instrument in instrumentList:
        symbol = instrument[:2]
        try:
            if int(symbol[1]) >= 0:
                symbol = symbol[0]
        except:
            pass
        
        # get 60m bar data from tb
        tb60m = tb1mTo60m(tbSymbol[symbol]+instrument[len(symbol):])
        
        # get 60m data, both current and history data from cta trading
        ctp60m = pd.read_csv(os.path.join(ctaTradingBarFolderDict[instrument], instrument+"_df_60m.csv"),
                             parse_dates=['datetime'], index_col='datetime')
        try:
            ctp60mHis = pd.read_csv(os.path.join(ctaTradingBarFolderDict[instrument], instrument+"_df_60m_ctp.csv"),
                                    parse_dates=['datetime'], index_col='datetime')
        except:
            ctp60mHis = pd.DataFrame()
        
        nightSectionEndtime = time(23)
        # save ctp 60m bar data before cover it
        if abs(ctp60m.index[-1] - datetime.combine(ctp60m.index[-1].date(), time(11, 30))) < timedelta(minutes=5) or\
            abs(ctp60m.index[-1] - datetime.combine(ctp60m.index[-1].date(), time(15))) < timedelta(minutes=5) or\
                abs(ctp60m.index[-1] - datetime.combine(ctp60m.index[-1].date(), nightSectionEndtime)) < timedelta(minutes=5):
            if len(ctp60mHis):
                dfx = ctp60mHis.append(ctp60m.loc[ctp60mHis.index[-1]+timedelta(minutes=10):, :])
                dfx.to_csv(os.path.join(ctaTradingBarFolderDict[instrument], instrument+"_df_60m_ctp.csv"))
            else:
                ctp60m.to_csv(os.path.join(ctaTradingBarFolderDict[instrument], instrument+"_df_60m_ctp.csv"))
        else:
            email = Email()
            email.receivers = cwhEmail
#            email.receivers.append(zmEmail)
            email.send(instrument + "_df_60m.csv seems to have unusual end time.", "")
        
        # cover ctp 60m data
        if abs(ctp60m.index[-1] - (tb60m.index[-1]+timedelta(hours=1))) < timedelta(minutes=5) and(len(tb60m)>100):
            tb60m.tail(500).to_csv(os.path.join(ctaTradingBarFolderDict[instrument], instrument+"_df_60m.csv"))
        else:
            email = Email()
            email.receivers = cwhEmail
#            email.receivers.append(zmEmail)
            email.send(instrument + "_trading data warning.", instrument+"_df_60m.csv seems to have different end "
                                                                         "time from tb data, or len of tb 60m data "
                                                                         "is less than 100")


def runTbDataBtAndCoverCtaTradingBarData():
    pass

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    ctaTradingBarFolderDict = {
    "rb2010": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                           "paperTrading", "Probot_rb_CtaTrading"),
    "MA005": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                          "paperTrading", "Probot_MA_CtaTrading")
    }
    instrumentList = ["rb2010", "MA005"]
    
    if instrumentList == getTbInstrumentList(instrumentList):
        thread.thread(target=runSpike5OrderReview,
                      args=(deepcopy(spike5TradingInstrumentList),
                            deepcopy(spike5btStartDateDict),
                            deepcopy(spike5TradingBarFolder))
                      ).start()
        coverCtaTradingBarsWithTbData(instrumentList, ctaTradingBarFolderDict)

#    if (time(16) <= datetime.now().time() < time(16, 1)
#            or time(17) <= datetime.now().time() < time(17, 1)
#            or time(20) <= datetime.now().time() < time(20, 1)) \
#            and (not hasRunBtWithTbDataToday):
#        if instrumentList == getTbInstrumentList(instrumentList):
#            # run back testing with tb data
#            # thread to run back testing
#            tbInstrumentList = list()
#            hasRunBtWithTbDataToday = True
#    
#        sleep(60)