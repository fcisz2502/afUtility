import os
from datetime import datetime, time
import pandas as pd
from afUtility.mailing import Email
from afUtility.keyInfo import zmEmail, cwhEmail
from afUtility.keyInfo import machineID


# -----------------------------------------------------------------------------
def getAllOrderReview():
    '''
    ordersMonitor will check history orders,
    compare today's orders in real trading with thoes in backtesting. 
    '''
    email = Email()
    # email.set_subjectPrefix('')
#    reviewResultFolder = "\\\\FCIDEBIAN\\FCI_Cloud\\dataProcess\\spike stocks\\orderReview"
    reviewResultFolder = os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "dataProcess", "spike stocks", "orderReview")
    checkList = [machineID+"_spike61"]
    today = str(datetime.today().date())
    
    orderReviewTotalResult = pd.DataFrame()
    for doc in os.listdir(reviewResultFolder):
        if today in doc.split("_") and (not "allSpikeOrderReview.csv" in doc.split("_")):
            try:
                checkList.remove(doc[18:-4])
                orderReviewTotalResult = pd.concat([orderReviewTotalResult, pd.read_csv(os.path.join(reviewResultFolder, doc))], sort=False)
            except ValueError:
                print("doc is %s, which may not be included in the checkList." % doc)
    if len(checkList):
        subjectSuffix = "Order review result(s) from %s have not been found!" % checkList
    else:
        subjectSuffix = "Order review for %s have been done!" % checkList
    fileSaveAndAttached = os.path.join(reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_allSpikeOrderReview.csv")
    orderReviewTotalResult.to_csv(fileSaveAndAttached, index=0)
    
    orderReviewTotalResult['rt_open_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_open_datetime'])
    orderReviewTotalResult['rt_close_datetime'] = pd.to_datetime(orderReviewTotalResult['rt_close_datetime'])
    orderReviewTotalResult['bt_open_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_open_datetime'])
    orderReviewTotalResult['bt_close_datetime'] = pd.to_datetime(orderReviewTotalResult['bt_close_datetime'])
    
    # get today's orders in both real trading and backtesting
    tradeBeginTime = datetime.combine(datetime.now().date(),time(9))
    tradeEndTime = datetime.combine(datetime.now().date(),time(15))
    
    orderReviewTotalResult['rt_open_datetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['rt_close_datetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['bt_open_datetime'].fillna(datetime(2019,1,1), inplace=True)
    orderReviewTotalResult['bt_close_datetime'].fillna(datetime(2019,1,1), inplace=True)
    
    todaysOrder = orderReviewTotalResult.loc[(orderReviewTotalResult['rt_open_datetime']>tradeBeginTime) | (
                                            (tradeBeginTime < orderReviewTotalResult['rt_close_datetime']) & (
                                                    orderReviewTotalResult['rt_close_datetime'] < tradeEndTime)) | (
                                            orderReviewTotalResult['bt_open_datetime']>tradeBeginTime) | (
                                            (tradeBeginTime < orderReviewTotalResult['bt_close_datetime']) & (
                                                    orderReviewTotalResult['bt_close_datetime'] < tradeEndTime))]

    lot_check = todaysOrder.loc[: ,['strategyID', 'diff_lot']]
    lot_check.fillna(1, inplace=True)
    lot_check['diff_lot_bool'] = lot_check.loc[:, 'diff_lot'].map(lambda x: 0!=int(x))
    if lot_check.loc[:, 'diff_lot_bool'].sum():
        lot_check.set_index("strategyID", drop=True, inplace=True)
#        email.receivers = cwhEmail
        email.send('spike order review volume difference.', lot_check.to_html(justify='left'))
    
    # rearange today's order(s) before sending them 
    pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'open_datetime', 'close_datetime', 
                                       'open_price', 'close_price', 'lot', 'direction'])
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
        pairComp = pd.DataFrame(columns = ['stock', 'orderNumber', 'open_datetime', 'close_datetime', 
                                           'open_price', 'close_price', 'lot', 'direction'])

    todayOrderPresented.set_index('column', inplace=True, drop=True)
    
    todayOrderPresented.loc[todayOrderPresented['realTrading']==datetime(2019,1,1), 'realTrading'] = str()
    todayOrderPresented.loc[todayOrderPresented['backtesting']==datetime(2019,1,1), 'backtesting'] = str()
    
    todayOrderPresented.fillna(str(),inplace=True)
    
    email.receivers.append(zmEmail)
    # email.receivers = [cwhEmail]
    email.send("Review spike6's order(s) today: " + subjectSuffix,
               todayOrderPresented.to_html(justify='left'),
               files = [fileSaveAndAttached])
    
    
# -----------------------------------------------------------------------------
if __name__ == "__main__":
    getAllOrderReview()