# -*- coding: utf-8 -*-
import pymongo
import pandas as pd

# -----------------------------------------------------------------------------
databaseName = 'VnTrader_Position_Db'
collectionName = 'ProbotStrategy_pt'
lastClose = 3421
balance = 500000
lot_size = 10
commission = 0.0001
flt = {'name': 'Probot strategy', 'vtSymbol': 'rb2005'}

myclient = pymongo.MongoClient("mongodb://localhost:27017/")
database = myclient[databaseName]
collection = database[collectionName]
document =collection.find(flt)[0]

current = document['current_orders']
history = document['history_orders']

print('get into history orders')
for key in list(history.keys()):
    balance -= (history[key]['average_depart_price'] + history[key]['average_enter_price']) * history[key]['total_enter_volume'] * lot_size * commission
    
    if history[key]['direction'] == 'buy':
        
        profit = (history[key]['average_depart_price'] - history[key]['average_enter_price']) * history[key]['total_enter_volume'] * lot_size  
    else:
        profit = (history[key]['average_enter_price'] - history[key]['average_depart_price']) * history[key]['total_enter_volume'] * lot_size
    balance += profit
    print('%s profit is %f.'%(key, profit))
    
print('get into current orders')
for key in list(current.keys()):
    balance -= current[key]['average_enter_price'] * current[key]['total_enter_volume'] * lot_size * commission
    if current[key]['direction'] == 'buy':
        profit = (lastClose - current[key]['average_enter_price']) * current[key]['total_enter_volume'] * lot_size
    else:
        profit = (current[key]['average_enter_price'] - lastClose) * current[key]['total_enter_volume'] * lot_size
    balance += profit
    print('%s profit is %f.'%(key, profit))

print('final balance is ', balance)