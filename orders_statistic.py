# -*- coding: utf-8 -*-
"""
Created on Sat Oct 24 15:06:12 2020

@author: cwh
"""

# cal win/loss rate
import pymongo
import copy
import pandas as pd


# -----------------------------------------------------------------------------
class OrderStatistic(object):
    def __init__(self):
        self._instrument = ""
        self._strategy = "spike"
        self._history_orders = {}
        self._client = pymongo.MongoClient(host='localhost', port=27017)
        self._db = self._client.aShare_Trading_DB
        self._long_wins = 0
        self._long_losses = 0
        self._long_evens = 0
        self._short_wins = 0 
        self._short_losses = 0
        self._short_evens = 0
        self._order_info_dict = {}
    
    # -------------------------------------------------------------------------
    def set_instrument(self, instrument):
        self._instrument = str(instrument)
        
    def get_order_info_dict(self):
        return copy.deepcopy(self._order_info_dict)
    
    # -------------------------------------------------------------------------
    def get_orders(self, collection, strategy_version):
#        collection = db[_instrument+"_"+_strategy+"_3.x"]
    
        # strategyName or strategy_name
        flt = {'strategyName': self._strategy, 'symbol': self._instrument}
        if not list(collection.find(flt)):
            flt = {'strategy_name': self._strategy, 'symbol': self._instrument}
            
        if list(collection.find(flt)):
            result = list(collection.find(flt))[0]
            his_orders = result["history_orders"]
        else:
            his_orders = {}
    
        for key, value in his_orders.items():
            self._history_orders["_".join(
                    [self._strategy, str(strategy_version), key])] = value
    
    # -------------------------------------------------------------------------
    def get_history_orders(self):
        if self._instrument+"_"+self._strategy+"_3.x" in self._db.list_collection_names():
            collection = self._db[self._instrument+"_"+self._strategy+"_3.x"]
            self.get_orders(collection, 3)
            
        if self._strategy+"_5.0" in self._db.list_collection_names():
            collection = self._db[self._strategy+"_5.0"]
            self.get_orders(collection, 5)
    
    # -------------------------------------------------------------------------        
    def cal_statistic(self):
        self.get_history_orders()
        self.get_win_loss_number()
        self.cal_win_loss_rate()
    
    # -------------------------------------------------------------------------
    def cal_win_loss_rate(self):
        # cal and put result in _order_info_dict
        long_orders = self._long_wins + self._long_evens + self._long_losses
        short_orders = self._short_wins + self._short_evens + self._short_losses
        total_orders = long_orders + short_orders
        
        if total_orders != len(self._history_orders):
            raise ValueError("total_orders is %s, len of history_orders is %s." 
                  %(total_orders, len(self._history_orders)))
        
        total_wins = self._long_wins + self._short_wins
        total_losses = self._long_losses + self._short_losses
        total_evens = self._long_evens + self._short_evens
        
#        print("total wins is: %s, total losses is %s, total evens is %s." 
#              %(total_wins, total_losses, total_evens))
        
        self._order_info_dict['total_orders'] = total_orders
        self._order_info_dict['long_orders'] = long_orders
        self._order_info_dict['short_orders'] = short_orders
        
        self._order_info_dict['total_win_rate'] = total_wins/ total_orders
        self._order_info_dict['total_loss_rate'] = total_losses/ total_orders
        self._order_info_dict['total_even_rate'] = total_evens/ total_orders
        
        self._order_info_dict['long_win_rate'] = self._long_wins/ long_orders
        self._order_info_dict['long_loss_rate'] = self._long_losses/ long_orders
        self._order_info_dict['long_even_rate'] = self._long_evens/ long_orders
        
        self._order_info_dict['short_win_rate'] = self._short_wins/ short_orders
        self._order_info_dict['short_loss_rate'] = self._short_losses/ short_orders
        self._order_info_dict['short_even_rate'] = self._short_evens/ short_orders
    
    # -------------------------------------------------------------------------
    def get_win_loss_number(self):
        for order in self._history_orders.keys():
            if self._history_orders[order]['direction'] in ['long', 'buy']:
                if self._history_orders[order]['close_price'] > self._history_orders[order]['open_price']:
                    self._long_wins += 1
                elif self._history_orders[order]['close_price'] == self._history_orders[order]['open_price']:
                    self._long_evens += 1
                else:
                    self._long_losses += 1
            else:
                if self._history_orders[order]['close_price'] < self._history_orders[order]['open_price']:
                    self._short_wins += 1
                elif self._history_orders[order]['close_price'] == self._history_orders[order]['open_price']:
                    self._short_evens += 1
                else:
                    self._short_losses += 1


# -----------------------------------------------------------------------------        
if __name__ == "__main__":
    orders_sta = {}
#    instruments = ['002008', '600887', 
#                   '000333', '600276', '000661',
#                   '000858', '600036', '601318',
#                   '603288', '600009', '600585',
#                   '002475', '600309']
    instruments = ['000333', '002008']
    for instrument in instruments:
        print("getting to %s." %instrument)
        ot = OrderStatistic()
        ot.set_instrument(instrument)
        ot.cal_statistic()
        orders_sta[instrument] = ot.get_order_info_dict()
    
    orders_sta_df = pd.DataFrame(orders_sta).T
    columns = ['total_orders', 'total_win_rate', 'total_loss_rate', 'total_even_rate',
               'long_orders', 'long_win_rate', 'long_loss_rate', 'long_even_rate', 
               'short_orders', 'short_win_rate', 'short_loss_rate', 'short_even_rate']
    orders_sta_df = orders_sta_df.loc[:, columns]
