import os
from datetime import datetime, time, timedelta
import pandas as pd
from afUtility.mailing import Email
from afUtility.keyInfo import zmEmail, cwhEmail
import copy
from dateutil import parser


# -----------------------------------------------------------------------------
class CtaTradingOrderReviewResultGatherer(object):
    '''
    gatherer will check history orders,
    compare today's orders in real trading with thoes in backtesting. 
    '''
    def __init__(self, strategyName):
        self.strategyName = strategyName
        self.email = Email()
        self.orderReviewTotalResult = None
        self.subjectSuffix = None
        self.fileSaveAndAttached = None
        self.jqOrTb = str()
        self.tradingBarsPathDict = dict()
        self.recentOrders = None
        self.isTbDataUsed = False
        self.checkList = None
        self.reviewResultFolder = None
    
    # -------------------------------------------------------------------------
    def checkJqOrTbDataUsed(self):
        if 'tbdata' in self.reviewResultFolder.lower() \
        or 'tradeblazer' in self.reviewResultFolder.lower():
            self.isTbDataUsed = True
            self.jqOrTb = 'tbData'
        else:
            self.jqOrTb = 'jqData'
            
    # -------------------------------------------------------------------------    
    def getAllOrderReview(self):
        self.checkJqOrTbDataUsed()
            
        checkListCopy = self.checkList[:]
        today = str(datetime.today().date())
        
        orderReviewTotalResult = pd.DataFrame()
        for doc in os.listdir(self.reviewResultFolder):
            # look at last night's order(s)
            if datetime.now().time() < time(9):
                if today in doc.split("_") and (int(doc[11:17])<90000) and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkListCopy.remove(doc[18:-4])
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(self.reviewResultFolder, doc))], sort=False)
            # moring section order(s)
            elif datetime.now().time() < time(15):
                if today in doc.split("_") and (90000<int(doc[11:17])<150000) and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkListCopy.remove(doc[18:-4])
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(self.reviewResultFolder, doc))], sort=False)
            # afternoon section order(s)
            else:
                if today in doc.split("_") and (int(doc[11:17])>150000) and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkListCopy.remove(doc[18:30])
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(self.reviewResultFolder, doc))], sort=False)
        if len(checkListCopy):
            self.subjectSuffix = "Order review result(s) from %s have not been found!" % checkListCopy
        else:
            self.subjectSuffix = "Order review for %s have been done!" % self.checkList
        
        if len(orderReviewTotalResult):
            orderReviewTotalResult['rt_last_enter_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_last_enter_datetime'])
            orderReviewTotalResult['rt_last_depart_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_last_depart_datetime'])
            orderReviewTotalResult['bt_last_enter_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_last_enter_datetime'])
            orderReviewTotalResult['bt_last_depart_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_last_depart_datetime'])
        
        self.fileSaveAndAttached = os.path.join(self.reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_all"+self.strategyName+"OrderReview.csv")
        orderReviewTotalResult.to_csv(self.fileSaveAndAttached, index=0)
        
        self.orderReviewTotalResult = orderReviewTotalResult
        
        return orderReviewTotalResult
    
    # -------------------------------------------------------------------------
    def getRecentOrders(self, startDate=None):
        '''
        Det today's orders in both real trading and backtesting if joinquant data is used.
        If startDate is None, today's date will be used for joinquant data, or the date after 
        last order review date for tradeblazer data will be used.
        Turn isTbDataUsed to False if use joinquant data, to True for Tb data.
        '''        
        if self.orderReviewTotalResult is None:
            self.getAllOrderReview()
        orderReviewTotalResult = self.orderReviewTotalResult.copy()
        
        if startDate is None:
            if self.isTbDataUsed:
                try:
                    instrument = self.tradingBarsPathDict.keys()[-1]
                    lastBacktestingDatetime = pd.read_csv(os.path.join(self.tradingBarsPathDict[instrument], instrument+'_df_60m_ctp.csv'), 
                                              parse_dates=['datetime'], 
                                              index_col='datetime').index[-1]
                    if abs(lastBacktestingDatetime - datetime.combine(lastBacktestingDatetime.date(), time(15))) < timedelta(minutes=2.5):
                        # set time between 1500 to 2100 should be all fine.
                        tradeBeginDatetime = datetime.combine(lastBacktestingDatetime.date(), time(16))
                except Exception, e:
                    raise Exception("cannot fetch last backtesting datetime in getRecentOrders() when using tradeblazer data.")
                    self.email.send("cannot fetch last backtesting datetime in getRecentOrders() when using tradeblazer data.", repr(e))
            else:
                # joinquant data is used
                tradeBeginDatetime = datetime.combine(datetime.now().date(), time(0))
                if 0 == datetime.today().weekday():
                    tradeBeginDatetime -= timedelta(hours=4+48)
                elif datetime.today().weekday() in [1, 2, 3, 4, 5]:
                    tradeBeginDatetime -= timedelta(hours=4)
                else:
                    tradeBeginDatetime -= timedelta(hours=4+24)
        else:
            if isinstance(startDate, str):
                tradeBeginDatetime = datetime.combine(parser.parse(startDate).date(), time(0))
            elif isinstance(startDate, datetime):
                tradeBeginDatetime = datetime.combine(startDate.date(), time(0))
            else:
                raise Exception("please input startDate in str or in datetime format.")
            if 0 == tradeBeginDatetime.weekday():
                tradeBeginDatetime -= timedelta(hours=4+48)
            elif tradeBeginDatetime.weekday() in [1, 2, 3, 4, 5]:
                tradeBeginDatetime -= timedelta(hours=4)
            else:
                tradeBeginDatetime -= timedelta(hours=4+24)
            
        if time(11) < datetime.now().time() < time(15):
            tradeEndDatetime = datetime.combine(datetime.now().date(),time(11, 30))
        elif time(15) < datetime.now().time() < time(23):
            tradeEndDatetime = datetime.combine(datetime.now().date(),time(15))
        else:
            # ??????????????????????????????????????????
            # ??????????????????????????????????????????
            # ??????????????????????????????????????????
            # what to do with night trading'''
            pass
#            tradeEndDatetime = datetime.combine(datetime.now().date(), time(11, 30))
        print("tradeBeginDatetime", tradeBeginDatetime)
        print("tradeEndDatetime", tradeEndDatetime)
        
        if len(orderReviewTotalResult):
            orderReviewTotalResult['rt_last_enter_datetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['rt_last_depart_datetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['bt_last_enter_datetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['bt_last_depart_datetime'].fillna(datetime(2019,1,1), inplace=True)
            
            self.recentOrders = orderReviewTotalResult.loc[(orderReviewTotalResult['rt_last_enter_datetime']>tradeBeginDatetime) | (
                                                    (tradeBeginDatetime < orderReviewTotalResult['rt_last_depart_datetime']) & (
                                                            orderReviewTotalResult['rt_last_depart_datetime'] < tradeEndDatetime)) | (
                                                    orderReviewTotalResult['bt_last_enter_datetime']>tradeBeginDatetime) | (
                                                    (tradeBeginDatetime < orderReviewTotalResult['bt_last_depart_datetime']) & (
                                                            orderReviewTotalResult['bt_last_depart_datetime'] < tradeEndDatetime))]
        else:
            self.recentOrders = pd.DataFrame()
    
    # -------------------------------------------------------------------------    
    def rearangeRecentOrders(self):
        if self.recentOrders is None:
            self.getRecentOrders()
        # rearange today's order(s) before sending them 
        pairComp = pd.DataFrame(columns = ['instrument', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                           'openPrice', 'closePrice', 'volume', 'direction', 'sl', 'tp',
                                           "volFac", "range", "balanceAtOrder"])
        todayOrderPresented = pd.DataFrame(columns=['column', 'realTrading', 'backtesting'])
        for row in self.recentOrders.iterrows():
            pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['rt_index':'rt_balanceAtOrder'])
            pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['bt_index':'bt_balanceAtOrder'])
            tempDF = pairComp.T
            tempDF.reset_index(inplace=True)
            tempDF.rename(columns={"index": "column", 0: "realTrading", 1: "backtesting"}, inplace=True)
            todayOrderPresented = pd.concat([todayOrderPresented, tempDF], sort=False)
            todayOrderPresented.reset_index(drop=True, inplace=True)
            tempDF = None
            todayOrderPresented.loc[todayOrderPresented.shape[0], 'column': "backtesting"]= [str(), str(), str()]
            pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                               'openPrice', 'closePrice', 'volume', 'direction', 'sl', 'tp',
                                               "volFac", "range", "balanceAtOrder"])
    
        todayOrderPresented.set_index('column', inplace=True, drop=True)
        
        todayOrderPresented.loc[todayOrderPresented['realTrading']==datetime(2019,1,1), 'realTrading'] = str()
        todayOrderPresented.loc[todayOrderPresented['backtesting']==datetime(2019,1,1), 'backtesting'] = str()
        
        todayOrderPresented.fillna(str(),inplace=True)
        return todayOrderPresented
    
    # -------------------------------------------------------------------------
    def sendRecentOrders(self, startDate=None):
        if self.recentOrders is None:
            self.getRecentOrders(startDate=startDate)
        recentOrderPresented_ = self.rearangeRecentOrders()
        self.email.set_subjectPrefix(self.jqOrTb)
        if 'tb' in self.jqOrTb:
            subject = "recently"
        else:
            subject = "today"
        self.email.send("Review %s's order(s) %s." % (self.strategyName, subject), 
                        recentOrderPresented_.to_html(justify='left'),
                        files = [self.fileSaveAndAttached])
    
    
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    strategyName = "Probot"
    probotCheckList = ["VM2CF_probot"]
    
    probotTradingBarFolderDict = {
        "rb2010": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                               "paperTrading", "probot_rb"),
        "MA009": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                              "paperTrading", "probot_MA")
    }
    
    # jointquant data bt result
    reviewResultFolder = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", 
                                      "dataProcess", "future_daily_data", 
                                      strategyName.lower()+"OrderReview")
    # tb data bt result
    reviewResultFolderTb = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "dataProcess", 
                                        "future_daily_data", "reviewWithTbData",
                                        strategyName.lower()+"OrderReview")
    
    gatherer = CtaTradingOrderReviewResultGatherer(strategyName)
#    gatherer.email.receivers.append(zmEmail)
    gatherer.email.set_receivers([cwhEmail])
    
    gatherer.checkList= probotCheckList[:]
    
    '''jq data'''
    gatherer.reviewResultFolder = reviewResultFolder
    gatherer.sendRecentOrders(startDate=None)

    ''' Tb data.
    Run this sector on according mathine so that it will 
    get the tradingBarsFolderDict correctly'''
#    gatherer.reviewResultFolder = reviewResultFolderTb
#    gatherer.tradingBarsPathDict = probotTradingBarFolderDict
#    gatherer.sendRecentOrders(startDate=None)
