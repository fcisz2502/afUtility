import os
import threading
from time import sleep
from datetime import datetime, time
from tradingInstrumentMonitor import TradingInstrumentMonitor
from stock_portfolio_pnl import stock_portfolio_pnl
from future_portfolio_pnl import future_portfolio_pnl
from compare_stock_data import get_60m_data_from_web, compare_stock_data

            
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    # xl = Write_xml()
    # xl.set_direct()
    # xl.set_order_number()
    # xl.set_stock_id("190000889431")

    futurePath = os.path.join('c:', os.sep, 'vnpy-1.9.2', 'examples', 'CtaTrading', 'pt_trading_futures')
    futureGivenList = ['MA005', 'rb2005']
    
    ashare50Path = os.path.join('C:', os.sep,  'applepy', 'projects', 'ashare', 'ashare5.0', 'realtrading', 'trading_ashares')
    ashare50List = ['000333', '002008', '601318', '600009', '600309']
    
    ashare40Path = os.path.join('C:', os.sep,  'applepy', 'projects', 'ashare', 'ashare4.0', 'realtrading', 'trading_ashares')
    ashare40List = ['000333', '000858', '002008', '000661', '600036', 
                    '600309', '601318', '600009', '600276', '603288']
    ashare40TradingBarsFolder = "C:\\applepy\\projects\\ashare\\docs\\4.0_realTradingData\\"

    tim = TradingInstrumentMonitor()
    
    while True:
        if datetime.today().weekday() < 5:
            # ----------------------------------------------------------------------------------------------------------
            if time(8, 43) > datetime.now().time() > time(8, 42) or \
                time(13, 13) > datetime.now().time() > time(13, 12)or \
                    time(20, 43) > datetime.now().time() > time(20, 42):
                try:
                    thread = threading.Thread(target=tim.checkTradingBeginning,
                                              args=('Probot-pt',
                                                    futurePath, 
                                                    futureGivenList)
                                              ).start()
                except Exception, e:
                    print('--1: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            # check future trading ending, trading program ends 5 minutes after trading section is closed.
            if time(11, 37) > datetime.now().time() > time(11, 36) or \
                time(15, 7) > datetime.now().time() > time(15, 6)or \
                    time(23, 37) > datetime.now().time() > time(23, 36):
                try:
                    thread = threading.Thread(target=tim.checkTradingEnding,
                                              args=('Probot-pt',
                                                    futurePath, 
                                                    futureGivenList)
                                              ).start()
                except Exception, e:
                    print('--2: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            # check A share trading starting
            if time(9, 23) > datetime.now().time() > time(9, 22):
                try:
                    thread = threading.Thread(target=tim.checkTradingBeginning,
                                              args=('applepy-4.0-pt',
                                                    ashare40Path, 
                                                    ashare40List)
                                              ).start()
                    thread = threading.Thread(target=tim.checkTradingBeginning,
                                              args=('applepy-5.0-pt',
                                                    ashare50Path, 
                                                    ashare50List)
                                              ).start()
                except Exception, e:
                    print('--3: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            # check A share trading ending
            if time(15, 7) > datetime.now().time() > time(15, 6):
                try:
                    thread = threading.Thread(target=tim.checkTradingEnding,
                                              args=('applepy-4.0-pt',
                                                    ashare40Path, 
                                                    ashare40List)
                                              ).start()
                    thread = threading.Thread(target=tim.checkTradingEnding,
                                              args=('applepy-5.0-pt',
                                                    ashare50Path, 
                                                    ashare50List)
                                              ).start()
                except Exception, e:
                    print('--4: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            if time(15, 8) > datetime.now().time() > time(15, 7):
                try:
                    future_portfolio_pnl()
                except Exception, e:
                    print('getting future portfolio pnl failed, error: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            if time(15, 11) > datetime.now().time() > time(15, 10):
                try:
                    stock_portfolio_pnl()
                except Exception, e:
                    print('getting stock portfolio pnl failed, error: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            if time(15, 14) > datetime.now().time() > time(15, 13) or time(11, 40) > datetime.now().time() > time(11, 39):
                try:
                    thread = threading.Thread(target=compare_stock_data,
                                              args=(ashare40List,
                                                    ashare40TradingBarsFolder,
                                                    'applepy-4.0-pt',
                                                    0.002)
                                              ).start()
                except Exception, e:
                    print('compare stock data has failed: ', repr(e))

            # ----------------------------------------------------------------------------------------------------------
            # trade station
            if (time(15, 11) > datetime.now().time() > time(15, 10)) or (time(9, 23) > datetime.now().time() > time(9, 22)):
                order_id = open("C:\\autotrade\\order_number.txt", "w")
                order_id.write("1")
                order_id.close()

            # ----------------------------------------------------------------------------------------------------------
            print('tradingMonitor at work, now is: ' + str(datetime.now().time()))
            sleep(60)
        # weeken
        else:
            print('Weekend. Have some fun!')
            sleep(60)