import pandas as pd
import os
from datetime import datetime, time, timedelta
from mailing import Email
from keyInfo import cwhEmail
from afUtility.contractInfo import futureTickSize


# -----------------------------------------------------------------------------
class CheckTradingBarsWithOtherSource(object):
    # -------------------------------------------------------------------------
    def __init__(self):
        self.webBars = os.path.join(os.sep*2, "FCIDEBIAN",
                                    "FCI_Cloud", "dataProcess",
                                    "future_daily_data")

        self.ctpBarFolderDict = {"rb": os.path.join("C:", os.sep, "vnpy-1.9.2", 
                                                    "examples", "ctaTrading", 
                                                    "paperTrading"),
                                "MA": os.path.join("C:", os.sep, "vnpy-1.9.2", 
                                                   "examples", "ctaTrading", 
                                                   "paperTrading")}
        self.strategyName = "Probot"
                      
    # -------------------------------------------------------------------------
    def checkTradingBars(self, instrumentList):
        for instrument in instrumentList:
            symbol = instrument[:2]
            print("there"* 10)
            try:
                if int(symbol[1])>=0:
                    symbol = symbol[0]
            except ValueError:
                pass
            
            tickSize = futureTickSize[symbol]
            print("here"* 10)
            self.webBars = pd.read_csv(os.path.join(self.webBars, instrument+".csv"),
                                       parse_dates=['datetime'])
            
            self.ctpBars = pd.read_csv(os.path.join(self.ctpBarFolderDict[symbol],
                                                    self.strategyName+"_"+symbol+"_CtaTrading",
                                                    instrument+"_df_60m.csv"),
                                       parse_dates=['datetime'])
            
            if self.ctpBars.iloc[-1]['datetime'].time() > time(22, 50):
                # night section
                numberOfBars = 2     
            elif self.ctpBars.iloc[-1]['datetime'].time() > time(14, 50):
                # afternoon section
                numberOfBars = 2
                
            else:
                # three 60m bars in morning section
                numberOfBars = 3
    #            self.webBars.reset_index(drop=True, inplace=True)
                self.webBars.loc[len(self.webBars)-1, 'datetime'] -= timedelta(hours=0.5)
            
            self.ctpBars = self.ctpBars.tail(numberOfBars).reset_index(drop=True)
            self.ctpBars = self.ctpBars.loc[:, ['datetime', 'open', 'high', 'low', 'close']]
            self.ctpBars['mergeKey'] = self.ctpBars.index
            
            self.webBars = self.webBars.tail(numberOfBars).reset_index(drop=True)
            self.webBars['mergeKey'] = self.webBars.index
            self.webBars['datetime'] = self.webBars['datetime'] + timedelta(hours=1)
            
            compRes = pd.merge(self.ctpBars, self.webBars, how='outer', on='mergeKey',
                               suffixes=('_ctp', '_web'))
            
            compRes['dtDiff'] = abs(compRes['datetime_ctp'] - compRes['datetime_web'])
            compRes["dt>5min"] = compRes['dtDiff'] > timedelta(minutes=5)
            compRes['openDiff'] = compRes['open_ctp'] - compRes['open_web']
            compRes['highDiff'] = compRes['high_ctp'] - compRes['high_web']
            compRes['lowDiff'] = compRes['low_ctp'] - compRes['low_web']
            compRes['closeDiff'] = compRes['close_ctp'] - compRes['close_web']
            
            if sum((compRes.loc[:, 'openDiff':'closeDiff'] > tickSize).sum()) or  \
            compRes['dt>5min'].sum():
                email = Email()
                email.set_subjectPrefix("check-trading-bars-with-joinquant")
                email.receivers = cwhEmail
                email.send(instrument, compRes.T.to_html(justify='left'))
            
#            return compRes
    

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    instrumentList = ["rb2010"]
    ctb =  CheckTradingBarsWithOtherSource()
    compRes = ctb.checkTradingBars(["MA005", "rb2010"])  # 