from applepy.spike61OrderReview.spike61OrderReview import runSpike61OrderReview
from afUtility.spike6OrderReviewResultGatherer import getAllOrderReview
from afUtility.spike6_monitor import spike6_stocks, spike6_order_review_start_date_dict


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    runSpike61OrderReview(spike6_stocks[:], spike6_order_review_start_date_dict)

    # getAllOrderReview()
