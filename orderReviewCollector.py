import os
from datetime import datetime, time
import pandas as pd
from afUtility.mailing import Email
from afUtility.keyInfo import zmEmail


# -----------------------------------------------------------------------------
def getAllOrderReview():
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
        content = "Order review result(s) from %s have not been found!"%checkList
    else:
        content = "Order review for %s have been done!"%["VM2_spike2", "VM2_spikeOLOS", "VM4_spike2", "VM5_spike2"]
    
    allSpikeOrderReview = os.path.join(reviewResultFolder, datetime.now().strftime("%Y-%m-%d_%H%M%S")+"_allSpikeOrderReview.csv")
    orderReviewTotalResult.to_csv(allSpikeOrderReview, index=0)
    
#    email = Email()
#    email.receivers.append(zmEmail)
#    email.send("spike order review", content, files = [allSpikeOrderReview])
    

# -----------------------------------------------------------------------------
if __name__ == "__main__":
    getAllOrderReview()