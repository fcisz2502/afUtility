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
            
        checkListCopy = [item.lower() for item in self.checkList]
        today = str(datetime.today().date())
        
        orderReviewTotalResult = pd.DataFrame()
        for doc in os.listdir(self.reviewResultFolder):
            # look at last night's order(s)
            if datetime.now().time() < time(9):
                if today in doc.split("_") and (parser.parse(' '.join([doc.split('_')[0], doc.split('_')[1]])).time() < time(9)) \
                    and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    # doc[18:-4] =>'machineID_strategy_xx', xx=> 'jq' or 'tb'
                    checkListCopy.remove(doc[18:-4].lower())
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(self.reviewResultFolder, doc))], sort=False)
            # moring section order(s)
            elif datetime.now().time() < time(15):
                if today in doc.split("_") and (time(9) < parser.parse(' '.join([doc.split('_')[0], doc.split('_')[1]])).time() < time(15)) \
                    and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkListCopy.remove(doc[18:-4].lower())
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(self.reviewResultFolder, doc))], sort=False)
            # afternoon section order(s)
            else:
                if today in doc.split("_") and (parser.parse(' '.join([doc.split('_')[0], doc.split('_')[1]])).time() > time(15)) \
                    and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    # doc[18:-7] => 'machineID_strategy'
                    checkListCopy.remove(doc[18:-7].lower())
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(self.reviewResultFolder, doc))], sort=False)

        if len(checkListCopy):
            self.subjectSuffix = "Order review result(s) from %s have not been found!" %checkListCopy
        else:
            self.subjectSuffix = "Order review for %s have been done!" %self.checkList
        
        if len(orderReviewTotalResult):
            orderReviewTotalResult['rt_openDatetime'] = pd.to_datetime(orderReviewTotalResult['rt_openDatetime'])
            orderReviewTotalResult['rt_closeDatetime'] = pd.to_datetime(orderReviewTotalResult['rt_closeDatetime'])
            orderReviewTotalResult['bt_openDatetime'] = pd.to_datetime(orderReviewTotalResult['bt_openDatetime'])
            orderReviewTotalResult['bt_closeDatetime'] = pd.to_datetime(orderReviewTotalResult['bt_closeDatetime'])
        
        self.fileSaveAndAttached = os.path.join(self.reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_all"+self.strategyName+"OrderReview.csv")
        orderReviewTotalResult.to_csv(self.fileSaveAndAttached, index=0)
        
        self.orderReviewTotalResult = orderReviewTotalResult
        
        return orderReviewTotalResult
    
    # -------------------------------------------------------------------------
    def getRecentOrders(self, startDate=None):
        '''
        Get today's orders in both real trading and backtesting if joinquant data is used.
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
                    with open(os.path.join(self.tradingBarsPathDict[instrument], "lastOrderReviewDatetime.txt"), 'r') as f:
                        lastOrderReviewDatetime = parser.parse(f.read())
                    if lastOrderReviewDatetime > datetime(2020, 1, 1) and \
                    abs(lastOrderReviewDatetime - datetime.combine(lastOrderReviewDatetime.date(), time(15))) < timedelta(minutes=2.5):
                        # set time between 1500 to 2100 should be all fine.
                        tradeBeginDatetime = datetime.combine(lastOrderReviewDatetime.date(), time(16))
                    else:
                        self.email.send("lastOrderReview datetime is befort year of 2020 or datetime.time is not near 1500.", str())
                        raise Exception("lastOrderReview datetime is befort year of 2020 or datetime.time is not near 1500.")
                except Exception, e:
                    self.email.send("cannot fetch last backtesting datetime in getRecentOrders() when using tradeblazer data.", repr(e))
                    raise Exception("cannot fetch last backtesting datetime in getRecentOrders() when using tradeblazer data, %s." % repr(e))
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
                self.email.send("please input startDate in str or in datetime format in ctaTradingOrderReviewResultGatherer.", str())
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
        
        if len(orderReviewTotalResult):
            orderReviewTotalResult['rt_openDatetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['rt_closeDatetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['bt_openDatetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['bt_closeDatetime'].fillna(datetime(2019,1,1), inplace=True)
            
            self.recentOrders = orderReviewTotalResult.loc[(orderReviewTotalResult['rt_openDatetime']>tradeBeginDatetime) | (
                                                    (tradeBeginDatetime < orderReviewTotalResult['rt_closeDatetime']) & (
                                                            orderReviewTotalResult['rt_closeDatetime'] < tradeEndDatetime)) | (
                                                    orderReviewTotalResult['bt_openDatetime']>tradeBeginDatetime) | (
                                                    (tradeBeginDatetime < orderReviewTotalResult['bt_closeDatetime']) & (
                                                            orderReviewTotalResult['bt_closeDatetime'] < tradeEndDatetime))]

        else:
            self.recentOrders = pd.DataFrame()
    
    # -------------------------------------------------------------------------    
    def rearangeRecentOrders(self):
        if self.recentOrders is None:
            self.getRecentOrders()
            
        # rearange today's order(s) before sending them 
        columnList = ['instrument', 'orderNumber', 'openDatetime', 'closeDatetime', 
                      'openPrice', 'closePrice', 'strategy', 'volume', 'direction', 
                      'sl', 'tp', 'volFac', 'range', 'balanceAtOrder', 'csrh', 'csrl']
        todayOrderPresented = pd.DataFrame(columns=['column', 'realTrading', 'backtesting'])
        pairComp = pd.DataFrame(columns = columnList)
        for row in self.recentOrders.iterrows():
            pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['rt_index':'rt_csrl'])
            pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['bt_index':'bt_csrl'])
            tempDF = pairComp.T
            tempDF.reset_index(inplace=True)
            tempDF.rename(columns={"index": "column", 0: "realTrading", 1: "backtesting"}, inplace=True)
            todayOrderPresented = pd.concat([todayOrderPresented, tempDF], sort=False)
            todayOrderPresented.reset_index(drop=True, inplace=True)
            tempDF = None
            todayOrderPresented.loc[todayOrderPresented.shape[0], 'column': "backtesting"]= [str(), str(), str()]
            pairComp = pd.DataFrame(columns = columnList)
    
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
    strategyName = "csrProbot"
    strategyCheckList = ["VM2CF_csrProbot"]
    
    gatherer = CtaTradingOrderReviewResultGatherer(strategyName)
#    gatherer.email.receivers.append(zmEmail)
    gatherer.email.set_receivers([cwhEmail])
    
    gatherer.checkList= strategyCheckList[:]
    
    # -------------------------------------------------------------------------
    '''jq data'''
    # jointquant data bt result
#    reviewResultFolder = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", 
#                                      "dataProcess", "future_daily_data", 
#                                      strategyName.lower()+"OrderReview")
#    gatherer.reviewResultFolder = reviewResultFolder
#    gatherer.sendRecentOrders(startDate=None)

    # -------------------------------------------------------------------------
    '''Tb data.
    Run this sector on according mathine so that it will 
    get the tradingBarsFolderDict correctly'''
    # tb data bt result
    reviewResultFolderTb = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "dataProcess", 
                                        "future_daily_data", "reviewWithTbData",
                                        strategyName.lower()+"OrderReview")
    
    csrProbotTradingBarFolderDict = {
        "rb2101": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                               "paperTrading", "csrProbot_rb"),
        "bu2012": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                              "paperTrading", "csrProbot_bu"),
        "m2101": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                              "paperTrading", "csrProbot_m"),
        "p2101": os.path.join("C:", os.sep, "vnpy-1.9.2", "examples", "ctaTrading",
                              "paperTrading", "csrProbot_p")
    }
    gatherer.reviewResultFolder = reviewResultFolderTb
    gatherer.tradingBarsPathDict = csrProbotTradingBarFolderDict
    gatherer.sendRecentOrders(startDate=None)
