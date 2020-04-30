import os
from datetime import datetime, time, timedelta
import pandas as pd
from afUtility.mailing import Email
from afUtility.keyInfo import zmEmail, cwhEmail
import copy


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
    
    # -------------------------------------------------------------------------    
    def getAllOrderReview(self, checkList, reviewResultFolder):      
        checkListCopy = copy.deepcopy(checkList)
        today = str(datetime.today().date())
        
        orderReviewTotalResult = pd.DataFrame()
        for doc in os.listdir(reviewResultFolder):
            # look at last night's order(s)
            if datetime.now().time() < time(9):
                if today in doc.split("_") and (int(doc[11:17])<90000) and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkList.remove(doc[18:-4])
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
            # moring section order(s)
            elif datetime.now().time() < time(15):
                if today in doc.split("_") and (90000<int(doc[11:17])<150000) and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkList.remove(doc[18:-4])
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
            # afternoon section order(s)
            else:
                if today in doc.split("_") and (int(doc[11:17])>150000) and (not "all"+self.strategyName+"OrderReview.csv" in doc.split("_")):
                    checkList.remove(doc[18:-4])
                    orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
        if len(checkList):
            self.subjectSuffix = "Order review result(s) from %s have not been found!" % checkList
        else:
            self.subjectSuffix = "Order review for %s have been done!" % checkListCopy
        
        if len(orderReviewTotalResult):
            orderReviewTotalResult['rt_last_enter_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_last_enter_datetime'])
            orderReviewTotalResult['rt_last_depart_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_last_depart_datetime'])
            orderReviewTotalResult['bt_last_enter_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_last_enter_datetime'])
            orderReviewTotalResult['bt_last_depart_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_last_depart_datetime'])
        
        self.fileSaveAndAttached = os.path.join(reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_all"+self.strategyName+"OrderReview.csv")
        orderReviewTotalResult.to_csv(self.fileSaveAndAttached, index=0)
        
        self.orderReviewTotalResult = orderReviewTotalResult
        return orderReviewTotalResult
    
    # -------------------------------------------------------------------------
    def getTodaysOrders(self):
        # get today's orders in both real trading and backtesting
        if self.orderReviewTotalResult is None:
            self.getAllOrderReview()
        orderReviewTotalResult = self.orderReviewTotalResult.copy()
        
        tradeBeginTime = datetime.combine(datetime.now().date(), time(0))
        if 0 == datetime.today().weekday():
            tradeBeginTime -= timedelta(hours=4+48)
        elif datetime.today().weekday() in [1, 2, 3, 4, 5]:
            tradeBeginTime -= timedelta(hours=4)
        else:
            tradeBeginTime -= timedelta(hours=4+24)
            
        if time(11) < datetime.now().time() < time(15):
            tradeEndTime = datetime.combine(datetime.now().date(),time(11, 30))
        elif time(15) < datetime.now().time() < time(23):
            tradeEndTime = datetime.combine(datetime.now().date(),time(15))
        else:
            # ??????????????????????????????????????????
            # ??????????????????????????????????????????
            # ??????????????????????????????????????????
            # what to do with night trading'''
            tradeEndTime = datetime.combine(datetime.now().date(),time(11, 30))
        print("tradeBeginTime", tradeBeginTime)
        print("tradeEndTime", tradeEndTime)
        
        if len(orderReviewTotalResult):
            orderReviewTotalResult['rt_last_enter_datetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['rt_last_depart_datetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['bt_last_enter_datetime'].fillna(datetime(2019,1,1), inplace=True)
            orderReviewTotalResult['bt_last_depart_datetime'].fillna(datetime(2019,1,1), inplace=True)
            
            self.todaysOrder = orderReviewTotalResult.loc[(orderReviewTotalResult['rt_last_enter_datetime']>tradeBeginTime) | (
                                                    (tradeBeginTime < orderReviewTotalResult['rt_last_depart_datetime']) & (
                                                            orderReviewTotalResult['rt_last_depart_datetime'] < tradeEndTime)) | (
                                                    orderReviewTotalResult['bt_last_enter_datetime']>tradeBeginTime) | (
                                                    (tradeBeginTime < orderReviewTotalResult['bt_last_depart_datetime']) & (
                                                            orderReviewTotalResult['bt_last_depart_datetime'] < tradeEndTime))]
        else:
            self.todaysOrder = pd.DataFrame()
    
    # -------------------------------------------------------------------------    
    def rearangeTodysOrders(self):
        self.getTodaysOrders()
        # rearange today's order(s) before sending them 
        pairComp = pd.DataFrame(columns = ['instrument', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                           'openPrice', 'closePrice', 'volume', 'direction', 'sl', 'tp',
                                           "volFac", "range", "balanceAtOrder"])
        todayOrderPresented = pd.DataFrame(columns=['column', 'realTrading', 'backtesting'])
        for row in self.todaysOrder.iterrows():
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
    def sendTodaysOrders(self):
        todaysOrderPresented_ = self.rearangeTodysOrders()
        self.email.set_subjectPrefix(self.jqOrTb)
        self.email.send("Review "+self.strategyName+"'s order(s) today: ", 
                   todaysOrderPresented_.to_html(justify='left'),
                   files = [self.fileSaveAndAttached])
    
    
# -----------------------------------------------------------------------------
if __name__ == "__main__":
#    pass
    strategyName = "Probot"
    checkList = ["VM2CF_probot"]
    # jointquant data bt result
    reviewResultFolder = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", 
                                      "dataProcess", "future_daily_data", 
                                      strategyName[0].lower()+strategyName[1:]+"OrderReview")
    # tb data bt result
    reviewResultFolderTb = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "dataProcess", 
                                        "future_daily_data", "reviewWithTbData",
                                        strategyName[0].lower()+strategyName[1:]+"OrderReview")
    
    for path in [reviewResultFolder, reviewResultFolderTb]: 
        gatherer = CtaTradingOrderReviewResultGatherer(strategyName)
        if "TbData" in path:
            gatherer.jqOrTb = "tbData"
        else:
            gatherer.jqOrTb = "jqData"
#        gatherer.email.receivers = [cwhEmail]
    #        gatherer.email.receivers.append(zmEmail)
        gatherer.getAllOrderReview(copy.deepcopy(checkList), path)
        gatherer.sendTodaysOrders()
