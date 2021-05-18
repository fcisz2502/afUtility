from applepy.spike61OrderReview.spike61OrderReview import runSpike61OrderReview
from afUtility.spike6OrderReviewResultGatherer import getAllOrderReview
from datetime import datetime, time, timedelta
import threading
from time import sleep
import redis
from dateutil import parser
from afUtility.mailing import Email


# ----------------------------------------------------------------------------------------------------------------------
def check_spike6_tick_datetime(stocks):
    pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
    red = redis.StrictRedis(connection_pool=pool)
    email = Email()
    for stock in stocks:
        if red.get(stock+'TickTime') is None:
            email.send("Get None for %s while checking it's tick's datetime." % stock, '')
        else:
            if abs(parser.parse(red.get(stock+'TickTime')) - datetime.now()) > timedelta(minutes=2):
                email.send("Spike6 %s tick's datetime has deviated from local time more than 2 minutes." % stock, '')

spike6_stocks = [
        '000333', '000661', '000858', '002475', '600009',
        '600031', '600036', '600276', '600309', '601318', '603288'
    ]

spike6_order_review_start_date_dict = {
    '000333': '20210105',
    '000661': '20210105',
    '000858': '20210105',
    '002475': '20210105',
    '600009': '20210105',
    '600031': '20210105',
    '600036': '20210105',
    '600276': '20201204',
    '600309': '20210105',
    '601318': '20210105',
    '603288': '20210105',
    }


# ----------------------------------------------------------------------------------------------------------------------
if __name__ == '__main__':
    while True:
        if datetime.today().weekday() < 5:
            print('Spike6 trading monitor at work, %s.' % datetime.now().strftime('%Y-%m-%d %H:%M:%S'))
            # ----------------------------------------------------------------------------------------------------------
            # _check tick datetime
            if time(9, 30) <= datetime.now().time() < time(11, 29) or \
                    time(13) <= datetime.now().time() < time(14, 59):
                threading.Thread(
                    target=check_spike6_tick_datetime,
                    args=(spike6_stocks[:],)
                ).start()

                sleep(60)

            # ----------------------------------------------------------------------------------------------------------
            # _order review
            if time(15, 14) <= datetime.now().time() < time(15, 15):
                threading.Thread(
                    target=runSpike61OrderReview,
                    args=(spike6_stocks[:], spike6_order_review_start_date_dict)
                ).start()

                sleep(60)

            # --------------------------------------------------------------------------------------------------------------
            # _collect spike6 order review result
            if time(16, 1) <= datetime.now().time() < time(16, 2):
                threading.Thread(target=getAllOrderReview).start()

            # ----------------------------------------------------------------------------------------------------------
            sleep(60)
        else:
            print('It is weekend! Shake It!')
            sleep(60)
