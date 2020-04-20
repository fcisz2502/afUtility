import pandas as pd
import os
from datetime import datetime, time, timedelta
from mailing import Email
from keyInfo import cwhEmail
from afUtility.contractInfo import futureTickSize, getSymbol

                      
# -------------------------------------------------------------------------
def checkTradingBars(self, instrumentList, ctpBarFolderDict, strategy):
    webBarsDir = os.path.join(os.sep*2, "FCIDEBIAN",
                           "FCI_Cloud", "dataProcess",
                           "future_daily_data")
    for instrument in instrumentList:
        symbol = getSymbol(instrument)
        
        tickSize = futureTickSize[symbol]
        webBars = pd.read_csv(os.path.join(webBarsDir, instrument+".csv"),
                              parse_dates=['datetime'])
        
        ctpBars = pd.read_csv(os.path.join(ctpBarFolderDict[strategy][symbol],
                                           instrument+"_df_60m.csv"),
                              parse_dates=['datetime'])
        
        if ctpBars.iloc[-1]['datetime'].time() > time(22, 50):
            # night section
            numberOfBars = 2     
        elif ctpBars.iloc[-1]['datetime'].time() > time(14, 50):
            # afternoon section
            numberOfBars = 2
        else:
            # three 60m bars in morning section
            numberOfBars = 3
            webBars.loc[len(webBars)-1, 'datetime'] -= timedelta(hours=0.5)
        
        ctpBars = ctpBars.tail(numberOfBars).reset_index(drop=True)
        ctpBars = ctpBars.loc[:, ['datetime', 'open', 'high', 'low', 'close']]
        ctpBars['mergeKey'] = ctpBars.index
        
        webBars = webBars.tail(numberOfBars).reset_index(drop=True)
        webBars['mergeKey'] = webBars.index
        webBars['datetime'] = webBars['datetime'] + timedelta(hours=1)
        
        compRes = pd.merge(ctpBars, webBars, how='outer', on='mergeKey',
                           suffixes=('_ctp', '_web'))
        
        compRes['dtDiff'] = abs(compRes['datetime_ctp'] - compRes['datetime_web'])
        compRes["dt>5min"] = compRes['dtDiff'] > timedelta(minutes=5)
        compRes['openDiff'] = compRes['open_ctp'] - compRes['open_web']
        compRes['highDiff'] = compRes['high_ctp'] - compRes['high_web']
        compRes['lowDiff'] = compRes['low_ctp'] - compRes['low_web']
        compRes['closeDiff'] = compRes['close_ctp'] - compRes['close_web']
        
        if sum((compRes.loc[:, 'openDiff':'closeDiff'] > tickSize).sum()) |  \
        compRes['dt>5min'].sum():
            email = Email()
            email.set_subjectPrefix("check-trading-bars-with-joinquant")
            email.receivers = cwhEmail
            email.send(instrument, compRes.T.to_html(justify='left'))
        
#            return compRes
    

# -----------------------------------------------------------------------------
if __name__ == "__main__":    
    ctpBarFolderDict = {"probot":{"rb": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", 
                                                     "ctaTrading", "paperTrading", 
                                                     "Probot_rb_CtaTrading"),
                                  "MA": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", 
                                                     "ctaTrading", "paperTrading", 
                                                     "Probot_MA_CtaTrading")
                                  },
                        "csdual":{"rb": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", 
                                                     "ctaTrading", "paperTrading", 
                                                     "csdual_rb"), 
                                  "m": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", 
                                                    "ctaTrading", "paperTrading", 
                                                    "csdual_m"),
                                 }
                        }
    
    instrumentList = ['MA005', 'rb2010']
#    instrumentList = ['MA005']
#    instrumentList = ['rb2010']
    compRes = checkTradingBars(instrumentList, ctpBarFolderDict, "probot")  # 
