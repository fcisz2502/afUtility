# encoding: UTF-8
import numpy as np
from pandas.tseries.offsets import Milli
from datetime import datetime, date, time
from time import sleep
import copy
from vnpy.trader.vtObject import VtBarData


# -------------------------------------------------------------------------------------
def move_milli_2300(tick, func, high, low, setUpper, setLower):
    '''process datetime before pass the tick to on60mBar()'''
    if time(20, 59) <= tick.datetime.time() < time(21):
        tick.datetime = datetime.combine(tick.datetime.date(), time(21))
        func(tick)
        setUpper(tick.upperLimit)
        setLower(tick.lowerLimit)

    # the trading day before holiday, night section trading will be closed
    # open price will be generated in the following morning section
    elif time(8, 59) <= tick.datetime.time() < time(9, 0):
        tick.datetime = datetime.combine(tick.datetime.date(), time(9))
        func(tick)
        setUpper(tick.upperLimit)
        setLower(tick.lowerLimit)

    elif time(21) <= tick.datetime.time() <= time(23, 0, 2) \
        or time(9) <= tick.datetime.time() <= time(11, 30, 2) \
        or time(13, 30) <= tick.datetime.time() <= time(15, 0, 2):

        if tick.datetime.time() == time(21):
            tick.datetime = tick.datetime + Milli(20)

        elif tick.datetime.time() == time(23):
            tick.datetime = tick.datetime - Milli(10)

        elif time(23) < tick.datetime.time() <= time(23, 0, 2):
            tick.datetime = datetime.combine(tick.datetime.date(), time(23)) - Milli(5)

        elif tick.datetime.time() == time(9):
            tick.datetime = tick.datetime + Milli(20)

        elif tick.datetime.time() == time(15):
            tick.datetime = tick.datetime - Milli(10)

        elif time(15) < tick.datetime.time() <= time(15, 0, 2):
            tick.datetime = datetime.combine(tick.datetime.date(), time(15)) - Milli(5)

        else:
            pass
        
        # 在价格剧烈变化时，有时收到的tick数据，里面的highPrice大于此前交易时间（同一交易日）
        # 的highPrice，也大于当下tick的lastPrice。这是因为，每秒有两次价格推送，但是一次价格
        # 推送，实际上包含着多笔成交，可能里面的某笔交易的成交价格，已经高过此前的highPrice，
        # 然后又有低成交价，导致推送过来的tick里面的lastPrice小于highPrice
        # 同样的情况也可能发生在lowPrice
        # 如果是这样，我们需要将一个tick推送，拆成三个tick进行推送
        dt = tick.datetime
        tick.lastPrice = round(tick.lastPrice, 2)
        tick.highPrice = round(tick.highPrice, 2)
        tick.lowPrice = round(tick.lowPrice, 2)

        if (tick.highPrice > tick.lastPrice and tick.highPrice > high) \
            and (tick.lowPrice < tick.lastPrice and tick.lowPrice < low):
            print('one tick is going to be splited into 3 ticks')
            print('argument high is: ', high)
            print('argument low is: ', low)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            highTick = copy.deepcopy(tick)
            highTick.lastPrice = tick.highPrice

            lowTick = copy.deepcopy(tick)
            lowTick.lastPrice = tick.lowPrice
            lowTick.datetime = dt + Milli(1)

            tick.datetime = dt + Milli(2)

            tickList = list()
            tickList.append(highTick)
            tickList.append(lowTick)
            tickList.append(tick)
            print('highTick.lastPrice is: ', highTick.lastPrice)
            print('highTick.highPrice is: ', highTick.highPrice)
            print('highTick.lowPrice is: ', highTick.lowPrice)
            print('lowTick.lastPrice is: ', lowTick.lastPrice)
            print('lowTick.highPrice is: ', lowTick.highPrice)
            print('lowTick.lowPrice is: ', lowTick.lowPrice)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            for ti in tickList:
                func(ti)

        elif tick.highPrice > tick.lastPrice and tick.highPrice > high:
            print('one tick is going to be splited into 2 ticks')
            print('argument high is: ', high)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            highTick = copy.deepcopy(tick)
            highTick.lastPrice = tick.highPrice

            tick.datetime = dt + Milli(1)

            tickList = list()
            tickList.append(highTick)
            tickList.append(tick)
            print('highTick.lastPrice is: ', highTick.lastPrice)
            print('highTick.highPrice is: ', highTick.highPrice)
            print('highTick.lowPrice is: ', highTick.lowPrice)

            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            for ti in tickList:
                func(ti)

        elif tick.lowPrice < tick.lastPrice and tick.lowPrice < low:
            print('one tick is going to be splited into 2 ticks')
            print('argument low is: ', low)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            lowTick = copy.deepcopy(tick)
            lowTick.lastPrice = tick.lowPrice

            tick.datetime = dt + Milli(1)

            tickList = list()
            tickList.append(lowTick)
            tickList.append(tick)

            print('lowTick.lastPrice is: ', lowTick.lastPrice)
            print('lowTick.highPrice is: ', lowTick.highPrice)
            print('lowTick.lowPrice is: ',  lowTick.lowPrice)
            print('tick.lastPrice is: ',    tick.lastPrice)
            print('tick.highPrice is: ',    tick.highPrice)
            print('tick.lowPrice is: ',     tick.lowPrice)
            
            for ti in tickList:
                func(ti)
        else:
            func(tick)
    else:
        print tick.datetime, 'out of trading time!'


# -------------------------------------------------------------------------
def move_milli_2330(tick, func, high, low, setUpper, setLower):
    '''process datetime before pass the tick to on60mBar()'''
    if time(20, 59) <= tick.datetime.time() < time(21):
        tick.datetime = datetime.combine(tick.datetime.date(), time(21))
        func(tick)
        setUpper(tick.upperLimit)
        setLower(tick.lowerLimit)

    # the trading day before holiday, night section trading will be closed
    # open price will be generated in the following morning section
    elif time(8, 59) <= tick.datetime.time() < time(9):
        tick.datetime = datetime.combine(tick.datetime.date(), time(9))
        func(tick)
        setUpper(tick.upperLimit)
        setLower(tick.lowerLimit)

    elif time(21) <= tick.datetime.time() <= time(23, 30, 2) \
        or time(9) <= tick.datetime.time() <= time(11, 30, 2) \
        or time(13, 30) <= tick.datetime.time() <= time(15, 0, 2):

        if tick.datetime.time() == time(21):
            tick.datetime = tick.datetime + Milli(20)

        elif tick.datetime.time() == time(23, 30):
            tick.datetime = tick.datetime - Milli(10)

        elif time(23, 30) < tick.datetime.time() <= time(23, 30, 2):
            tick.datetime = datetime.combine(tick.datetime.date(), time(23, 30)) - Milli(5)

        elif tick.datetime.time() == time(9):
            tick.datetime = tick.datetime + Milli(20)

        elif tick.datetime.time() == time(15):
            tick.datetime = tick.datetime - Milli(10)

        elif time(15) < tick.datetime.time() <= time(15, 0, 2):
            tick.datetime = datetime.combine(tick.datetime.date(), time(15)) - Milli(5)

        else:
            pass
        
        # 在价格剧烈变化时，有时收到的tick数据，里面的highPrice大于此前交易时间（同一交易日）
        # 的highPrice，也大于当下tick的lastPrice。这是因为，每秒有两次价格推送，但是一次价格
        # 推送，实际上包含着多笔成交，可能里面的某笔交易的成交价格，已经高过此前的highPrice，
        # 然后又有低成交价，导致推送过来的tick里面的lastPrice小于highPrice
        # 同样的情况也可能发生在lowPrice
        # 如果是这样，我们需要将一个tick推送，拆成三个tick进行推送
        dt = tick.datetime
        tick.lastPrice = round(tick.lastPrice, 2)
        tick.highPrice = round(tick.highPrice, 2)
        tick.lowPrice = round(tick.lowPrice, 2)

        if (tick.highPrice > tick.lastPrice and tick.highPrice > high) and (
                tick.lowPrice < tick.lastPrice and tick.lowPrice < low):
            print('one tick is going to be splited into 3 ticks')
            print('argument high is: ', high)
            print('argument low is: ', low)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            highTick = copy.deepcopy(tick)
            highTick.lastPrice = tick.highPrice

            lowTick = copy.deepcopy(tick)
            lowTick.lastPrice = tick.lowPrice
            lowTick.datetime = dt + Milli(1)

            tick.datetime = dt + Milli(2)

            tickList = list()
            tickList.append(highTick)
            tickList.append(lowTick)
            tickList.append(tick)
            
            print('highTick.lastPrice is: ', highTick.lastPrice)
            print('highTick.highPrice is: ', highTick.highPrice)
            print('highTick.lowPrice is: ', highTick.lowPrice)
            print('lowTick.lastPrice is: ', lowTick.lastPrice)
            print('lowTick.highPrice is: ', lowTick.highPrice)
            print('lowTick.lowPrice is: ', lowTick.lowPrice)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            for ti in tickList:
                func(ti)

        elif tick.highPrice > tick.lastPrice and tick.highPrice > high:
            print('one tick is going to be splited into 2 ticks')
            print('argument high is: ', high)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            highTick = copy.deepcopy(tick)
            highTick.lastPrice = tick.highPrice

            tick.datetime = dt + Milli(1)

            tickList = list()
            tickList.append(highTick)
            tickList.append(tick)
            
            print('highTick.lastPrice is: ', highTick.lastPrice)
            print('highTick.highPrice is: ', highTick.highPrice)
            print('highTick.lowPrice is: ', highTick.lowPrice)

            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            for ti in tickList:
                func(ti)

        elif tick.lowPrice < tick.lastPrice and tick.lowPrice < low:
            print('one tick is going to be splited into 2 ticks')
            print('argument low is: ', low)
            print('tick.lastPrice is: ', tick.lastPrice)
            print('tick.highPrice is: ', tick.highPrice)
            print('tick.lowPrice is: ', tick.lowPrice)
            
            lowTick = copy.deepcopy(tick)
            lowTick.lastPrice = tick.lowPrice

            tick.datetime = dt + Milli(1)

            tickList = list()
            tickList.append(lowTick)
            tickList.append(tick)
            
            print('lowTick.lastPrice is: ', lowTick.lastPrice)
            print('lowTick.highPrice is: ', lowTick.highPrice)
            print('lowTick.lowPrice is: ',  lowTick.lowPrice)
            print('tick.lastPrice is: ',    tick.lastPrice)
            print('tick.highPrice is: ',    tick.highPrice)
            print('tick.lowPrice is: ',     tick.lowPrice)
            
            for ti in tickList:
                func(ti)
        else:
            func(tick)
    else:
        print tick.datetime, 'out of trading time!'


# -------------------------------------------------------------------------
if __name__ == '__main__':
    pass