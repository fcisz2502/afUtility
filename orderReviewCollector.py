import os
from datetime import datetime, time
import pandas as pd
from afUtility.mailing import Email
from afUtility.keyInfo import zmEmail,cwhEmail
import copy


# -----------------------------------------------------------------------------
def getAllOrderReview():
    '''
    ordersMonitor will check history orders,
    compare today's orders in real trading with thoes in backtesting. 
    '''
    reviewResultFolder = "\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\spike stocks\\orderReview"
    checkList = ["VM2_spike2", "VM2_spikeOLOS", "VM4_spike2", "VM5_spike2"]
    today = str(datetime.today().date())
    
    orderReviewTotalResult = pd.DataFrame()
    for doc in os.listdir(reviewResultFolder):
        if today in doc.split("_") and (not "allSpikeOrderReview.csv" in doc.split("_")):
            checkList.remove(doc[18:-4])
            orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
#            except TypeError:
#                orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))])
    if len(checkList):
        subjectSuffix = "Order review result(s) from %s have not been found!"%checkList
    else:
        subjectSuffix = "Order review for %s have been done!"%["VM2_spike2", "VM2_spikeOLOS", "VM4_spike2", "VM5_spike2"]
    
    fileSaveAndAttached = os.path.join(reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_allSpikeOrderReview.csv")
    orderReviewTotalResult.to_csv(fileSaveAndAttached, index=0)
    
#    email = Email()
#    email.receivers.append(zmEmail)
#    email.send("spike order review: "+subjectSuffix,
#               str(),
#               files = [fileSaveAndAttached])
    
    # get orderReviewTotalResult from loacl machine
#    reviewResultFolder = "\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\spike stocks\\orderReview"
#    doc = "2020-03-19_161759_allSpikeOrderReview.csv"
#    doc = os.path.join(reviewResultFolder, doc)
#    orderReviewTotalResult = pd.read_csv(doc)
    
    orderReviewTotalResult['rt_openDatetime'] = pd.to_datetime(orderReviewTotalResult['rt_openDatetime'])
    orderReviewTotalResult['rt_closeDatetime'] = pd.to_datetime(orderReviewTotalResult['rt_closeDatetime'])
    orderReviewTotalResult['bt_openDatetime'] = pd.to_datetime(orderReviewTotalResult['bt_openDatetime'])
    orderReviewTotalResult['bt_closeDatetime'] = pd.to_datetime(orderReviewTotalResult['bt_closeDatetime'])
    
    # get today's orders in both real trading and backtesting
    tradeBeginTime = datetime.combine(datetime.now().date(),time(9))
    tradeEndTime = datetime.combine(datetime.now().date(),time(15))
    
    orderReviewTotalResult['rt_openDatetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['rt_closeDatetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['bt_openDatetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['bt_closeDatetime'].fillna(datetime(2019,1,1), inplace=True)
    
    todaysOrder = orderReviewTotalResult.loc[(orderReviewTotalResult['rt_openDatetime']>tradeBeginTime) | (
                                            (tradeBeginTime < orderReviewTotalResult['rt_closeDatetime']) & (
                                                    orderReviewTotalResult['rt_closeDatetime'] < tradeEndTime)) | (
                                            orderReviewTotalResult['bt_openDatetime']>tradeBeginTime) | (
                                            (tradeBeginTime < orderReviewTotalResult['bt_closeDatetime']) & (
                                                    orderReviewTotalResult['bt_closeDatetime'] < tradeEndTime))]
    
    # rearange today's order(s) before sending them 
    pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                       'openPrice', 'closePrice', 'volume', 'direction'])
    todayOrderPresented = pd.DataFrame(columns=['column', 'realTrading', 'backtesting'])
    for row in todaysOrder.iterrows():
        pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['rt_index':'rt_direction'])
        pairComp.loc[len(pairComp), :] = [row[1]['strategyID']] + list(row[1]['bt_index':'bt_direction'])
        tempDF = pairComp.T
        tempDF.reset_index(inplace=True)
        tempDF.rename(columns={"index": "column", 0: "realTrading", 1: "backtesting"}, inplace=True)
        todayOrderPresented = pd.concat([todayOrderPresented, tempDF], sort=False)
        todayOrderPresented.reset_index(drop=True, inplace=True)
        tempDF = None
        todayOrderPresented.loc[todayOrderPresented.shape[0], 'column': "backtesting"]= [str(), str(), str()]
        pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'openDatetime', 'closeDatetime', 
                                           'openPrice', 'closePrice', 'volume', 'direction'])

    todayOrderPresented.set_index('column', inplace=True, drop=True)
    
    todayOrderPresented.loc[todayOrderPresented['realTrading']==datetime(2019,1,1), 'realTrading'] = str()
    todayOrderPresented.loc[todayOrderPresented['backtesting']==datetime(2019,1,1), 'backtesting'] = str()
    
    todayOrderPresented.fillna(str(),inplace=True)
    
    email = Email()
    email.set_subjectPrefix(str())
#    email.receivers = cwhEmail
    email.receivers.append(zmEmail)
    email.send("Review spike's order(s) today: " + subjectSuffix, 
               todayOrderPresented.to_html(justify='left'),
               files = [fileSaveAndAttached])
    
    
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    getAllOrderReview()