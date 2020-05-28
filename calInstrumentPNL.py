# -*- coding: utf-8 -*-
import pymongo
import pandas  as pd
from afUtility.mailing import Email
from datetime import datetime
import matplotlib.pyplot as plt
#from keyInfo import cwhEmail, zzhEmail, zmEmail


# -----------------------------------------------------------------------------
def calInstrumentPNL(instrument, database, collection, flt):
    # func to cal a single instrument's pnl use data in mongo.
    # it returns history orders' pnl and current orders' pnl.
    commission = 1.1/10000
    lotSize = 10
    myclient = pymongo.MongoClient('mongodb://localhost:27017/')        
    documents = myclient[database][collection] 
    data = documents.find(flt)[0]
    
    current_orders = data['current_orders']
    history_orders = data['history_orders']
    history_pnl = 0
    current_pnl = 0
    for order in list(history_orders.keys()):
        if "buy" == history_orders[order]['direction']:
            history_pnl += (history_orders[order]['average_depart_price'] \
                            - history_orders[order]['average_enter_price']) \
                            * history_orders[order]['total_enter_volume'] * lotSize \
                            - (history_orders[order]['average_depart_price'] \
                            + history_orders[order]['average_enter_price']) \
                            * history_orders[order]['total_enter_volume'] \
                            * lotSize * commission
        else:
            history_pnl += (history_orders[order]['average_enter_price'] \
                          - history_orders[order]['average_depart_price']) \
                          * history_orders[order]['total_enter_volume'] * lotSize \
                          - (history_orders[order]['average_depart_price'] \
                          + history_orders[order]['average_enter_price']) \
                          * history_orders[order]['total_enter_volume'] \
                          * lotSize * commission
    
    for order in list(current_orders.keys()):
        if "buy" == current_orders[order]['direction']:
            current_pnl += (current_orders[order]['pre_settle'] \
                            - current_orders[order]['average_enter_price']) \
                            * current_orders[order]['total_enter_volume'] * lotSize \
                            - (current_orders[order]['pre_settle'] \
                            + current_orders[order]['average_enter_price']) \
                            * current_orders[order]['total_enter_volume'] \
                            * lotSize * commission
        else:
            current_pnl += (current_orders[order]['average_enter_price'] \
                          - current_orders[order]['pre_settle']) \
                          * current_orders[order]['total_enter_volume'] * lotSize \
                          - (current_orders[order]['pre_settle'] \
                          + current_orders[order]['average_enter_price']) \
                          * current_orders[order]['total_enter_volume'] \
                          * lotSize * commission

    return current_pnl, history_pnl


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    instrument = "MA009"
    database='VnTrader_Position_Db'
    collection='ProbotStrategy_pt'
    flt = {'name': 'Probot strategy', 'vtSymbol': instrument}
    cpnl, hpnl = calInstrumentPNL(instrument, database, collection, flt)
    