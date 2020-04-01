import os
from datetime import datetime, time, timedelta
import pandas as pd
from afUtility.mailing import Email
from afUtility.keyInfo import zmEmail, cwhEmail
import copy


# -----------------------------------------------------------------------------
def getAllOrderReview():
    '''
    gatherer will check history orders,
    compare today's orders in real trading with thoes in backtesting. 
    '''
    strategyName = "Probot"
    checkList = ["VM2_probot"]
    reviewResultFolder = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", 
                                      "dataProcess", "future_daily_data", 
                                      strategyName[0].lower()+strategyName[1:]+"OrderReview")
    
    checkListCopy = copy.deepcopy(checkList)
    today = str(datetime.today().date())
    
    orderReviewTotalResult = pd.DataFrame()
    for doc in os.listdir(reviewResultFolder):
        if datetime.now().time() < time(9):
            if today in doc.split("_") and (int(doc[11:17])<90000) and (not "all"+strategyName+"OrderReview.csv" in doc.split("_")):
                checkList.remove(doc[18:-4])
                orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
        elif datetime.now().time() < time(15):
            if today in doc.split("_") and (90000<int(doc[11:17])<150000) and (not "all"+strategyName+"OrderReview.csv" in doc.split("_")):
                checkList.remove(doc[18:-4])
                orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
        else:
            if today in doc.split("_") and (int(doc[11:17])>150000) and (not "all"+strategyName+"OrderReview.csv" in doc.split("_")):
                checkList.remove(doc[18:-4])
                orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
    if len(checkList):
        subjectSuffix = "Order review result(s) from %s have not been found!" % checkList
    else:
        subjectSuffix = "Order review for %s have been done!" % checkListCopy
    
    fileSaveAndAttached = os.path.join(reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_all"+strategyName+"OrderReview.csv")
    orderReviewTotalResult.to_csv(fileSaveAndAttached, index=0)
    
    orderReviewTotalResult['rt_last_enter_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_last_enter_datetime'])
    orderReviewTotalResult['rt_last_depart_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_last_depart_datetime'])
    orderReviewTotalResult['bt_last_enter_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_last_enter_datetime'])
    orderReviewTotalResult['bt_last_depart_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_last_depart_datetime'])
    
    # get today's orders in both real trading and backtesting    
    tradeBeginTime = datetime.combine(datetime.now().date(), time(9))
    if 0 == datetime.today().weekday():
        tradeBeginTime -= timedelta(hours=4+72)
    elif datetime.today().weekday() in [1, 2, 3, 4]:
        tradeBeginTime -= timedelta(hours=4)
    elif 5 == datetime.today().weekday():
        tradeBeginTime -= timedelta(hours=4+24)
    else:
        tradeBeginTime -= timedelta(hours=4+48)
        
    if time(11) < datetime.now().time() < time(15):
        tradeEndTime = datetime.combine(datetime.now().date(),time(11, 30))
    elif time(15) < datetime.now().time() < time(21):
        tradeEndTime = datetime.combine(datetime.now().date(),time(15))
    else:
        pass
        # what to do with night trading
    
    orderReviewTotalResult['rt_last_enter_datetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['rt_last_depart_datetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['bt_last_enter_datetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['bt_last_depart_datetime'].fillna(datetime(2019,1,1), inplace=True)
    
    todaysOrder = orderReviewTotalResult.loc[(orderReviewTotalResult['rt_last_enter_datetime']>tradeBeginTime) | (
                                            (tradeBeginTime < orderReviewTotalResult['rt_last_depart_datetime']) & (
                                                    orderReviewTotalResult['rt_last_depart_datetime'] < tradeEndTime)) | (
                                            orderReviewTotalResult['bt_last_enter_datetime']>tradeBeginTime) | (
                                            (tradeBeginTime < orderReviewTotalResult['bt_last_depart_datetime']) & (
                                                    orderReviewTotalResult['bt_last_depart_datetime'] < tradeEndTime))]
    
    # rearange today's order(s) before sending them 
    pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                       'openPrice', 'closePrice', 'volume', 'direction', 
                                       "volFac", "range"])
    todayOrderPresented = pd.DataFrame(columns=['column', 'realTrading', 'backtesting'])
    for row in todaysOrder.iterrows():
        pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['rt_index':'rt_range'])
        pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['bt_index':'bt_range'])
        tempDF = pairComp.T
        tempDF.reset_index(inplace=True)
        tempDF.rename(columns={"index": "column", 0: "realTrading", 1: "backtesting"}, inplace=True)
        todayOrderPresented = pd.concat([todayOrderPresented, tempDF], sort=False)
        todayOrderPresented.reset_index(drop=True, inplace=True)
        tempDF = None
        todayOrderPresented.loc[todayOrderPresented.shape[0], 'column': "backtesting"]= [str(), str(), str()]
        pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                           'openPrice', 'closePrice', 'volume', 'direction',
                                           "volFac", "range"])

    todayOrderPresented.set_index('column', inplace=True, drop=True)
    
    todayOrderPresented.loc[todayOrderPresented['realTrading']==datetime(2019,1,1), 'realTrading'] = str()
    todayOrderPresented.loc[todayOrderPresented['backtesting']==datetime(2019,1,1), 'backtesting'] = str()
    
    todayOrderPresented.fillna(str(),inplace=True)
    
    email = Email()
    email.set_subjectPrefix(str())
    email.receivers = cwhEmail
#    email.receivers.append(zmEmail)
    email.send("Review "+strategyName+"'s order(s) today: " + subjectSuffix, 
               todayOrderPresented.to_html(justify='left'),
               files = [fileSaveAndAttached])
    
    
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    getAllOrderReview()
