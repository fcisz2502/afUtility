import os
from afUtility.mailing import Email
from keyInfo import cwhEmail


# -----------------------------------------------------------------------------
class TradingInstrumentMonitor(object):
    def __init__(self):
        self.email = Email()
    
    # -------------------------------------------------------------------------    
    def _get_trading_instrument_set(self, saving_dir):
        # used in future and stock trading,
        # # so use instrument instead of futures or stocks
        trading_instrument_set = set()
        for root, dirs, files in os.walk(saving_dir):
            for file in files:
                f = open(os.path.join(root, file), 'r+')
                instrument = f.read()
                if future:
                    trading_instrument_set.add(instrument)
                f.seek(0)
                f.truncate()
                f.close()
        return trading_instrument_set
    
    # ------------------------------------------------------------------------- 
    def _compare_given_instrument_to_trading_instrument(self, given_instrument_list, trading_instrument_set):
        print('trading_instrument_set is: ', trading_instrument_set)
        print('given_instrument_list is: ', given_instrument_list)
        gim = given_instrument_list[:]
        if trading_instrument_set:
            try:
                for item in trading_instrument_set:
                    if item != str():
                        gim.remove(item)
            except ValueError:
                self.email.send('ValueError in trading_instrument_list, check it!', repr(e))

        return gim
    
    # -------------------------------------------------------------------------     
    def check_trading_beginning(self, email_subject_prefix, instrument_saving_dir, given_instrument_list):
        tis = self._get_trading_instrument_set(instrument_saving_dir)  # til = tradingInstrumentList
        compare_res = self._compare_given_instrument_to_trading_instrument(given_instrument_list, tis)
        self.email.set_subjectPrefix(email_subject_prefix)
        
        if compare_res:
            self.email.send(str(compare_res) + ' has not started for trading', str())
        else:
            self.email.send('trading has started.', str())
            
    # ------------------------------------------------------------------------- 
    def check_trading_ending(self, email_subject_prefix, instrument_saving_dir, given_instrument_list):
        tis = self._get_trading_instrument_set(instrument_saving_dir)
        compare_res = self._compare_given_instrument_to_trading_instrument(given_instrument_list, tis)
        self.email.set_subjectPrefix(email_subject_prefix)
        
        if compare_res:
            self.email.send(str(compare_res) + ' has not ended normally', str())
        else:
            self.email.send('trading has ended.', str())
            
            
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    tim = TradingInstrumentMonitor()
    tim.email.set_receivers ( [cwhEmail])
    path = os.path.join('c:', os.sep, 'vnpy-1.9.2', 'examples', 'CtaTrading', 'pt_trading_futures')
    tim.checkTradingEnding('Probot-pt', path, ['rb2005', 'MA005'])