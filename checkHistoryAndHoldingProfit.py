import pymongo


# -----------------------------------------------------------------------------
def checkHistoryAndHoldingProfit():
    client = pymongo.MongoClient(host='localhost', port=27017)
    db = client.VnTrader_Position_Db
    collection = db["ProbotStrategy_pt"]
    flt = {'name': "Probot strategy", 'vtSymbol': "rb2010"}
    
    historyProfit = 0
    history_orders = collection.find(flt)[0]["history_orders"]
    for order in list(history_orders.keys()):
        if history_orders[order]['direction'] == "short":
            historyProfit += 10*(history_orders[order]['order_volume']*(
                    history_orders[order]['average_enter_price'] - history_orders[order]['average_depart_price']))
        else:
            historyProfit += 10*(history_orders[order]['order_volume']*(
                    history_orders[order]['average_depart_price'] - history_orders[order]['average_enter_price']))
    
    holdingProfit=0
    current_orders = collection.find(flt)[0]["current_orders"]
    for order in list(current_orders.keys()):
        if current_orders[order]['direction'] == "short":
            holdingProfit += 10*(current_orders[order]['order_volume']*(
                    current_orders[order]['average_enter_price'] - current_orders[order]['pre_settle']))
        else:
            holdingProfit += 10*(current_orders[order]['order_volume']*(
                    current_orders[order]['pre_settle'] - current_orders[order]['average_enter_price']))

    print("vtSymbol is %s, hitoryProfit is %s, holdingProfit is %s." % (
            flt['vtSymbol'], historyProfit, holdingProfit))


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    checkHistoryAndHoldingProfit()
