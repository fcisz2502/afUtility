import redis
from dateutil import parser
from time import sleep


pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
red = redis.StrictRedis(connection_pool=pool)

# -----------------------------------------------------------------------------
def changeStockToTsFormat(stock):
    if 6 == int(stock[0]):
        _suffix = '.sh'
    else:
        _suffix = '.sz'
    return stock + _suffix

# -----------------------------------------------------------------------------
def addStockToTsTickList(stockList):
    for stock in stockList:
        red.sadd('QPs', changeStockToTsFormat(stock))

def removeStockFromTsTickList(stockList):
    for stock in stockList:
        red.srem('QPs', changeStockToTsFormat(stock))

# -----------------------------------------------------------------------------
def getTickFromTS(stockInTsFormat):
    tick_data = red.get(stockInTsFormat)
    ask = float(tick_data.split(',')[1])
    asksize = int(tick_data.split(',')[2])
    bid = float(tick_data.split(',')[4])
    bidsize = int(tick_data.split(',')[5])
    last = float(tick_data.split(',')[7])
    volume = int(tick_data.split(',')[8])
    tickTime = parser.parse(tick_data.split(',')[9])
    return tickTime, ask, asksize, bid, bidsize, last, volume


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    stock = '600276'
    _stockInTsFormat = "TS_" + changeStockToTsFormat(stock)

    while True:
        tickTime, ask, asksize, bid, bidsize, last, volume = getTickFromTS(_stockInTsFormat)
        print("tickTime is %s, last is %s, volume is %s." %(tickTime, last, volume))
        sleep(1.5)
