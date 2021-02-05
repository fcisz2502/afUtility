import os
from afUtility.mailing import Email
from keyInfo import cwhEmail


# -----------------------------------------------------------------------------
class TradingInstrumentMonitor(object):
    def __init__(self):
        self._email = Email()
    
    # -------------------------------------------------------------------------    
    def _get_trading_instrument_set(self, saving_dir):
        # method used in future and stock trading,
        # so use instrument instead of futures or stocks
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
    def _compare_2_src(self, given_instrument_list, trading_instrument_set):
        print('trading_instrument_set is: ', trading_instrument_set)
        print('given_instrument_list is: ', given_instrument_list)
        
        gil = given_instrument_list[:]

        for instrument in trading_instrument_set:
            if len(instrument) and not instrument.isspace():
                try:
                    gil.remove(instrument)
                except ValueError:
                    self._email.send('ValueError in trading_instrument_list, check it!', repr(e))
        return gil
    
    # -------------------------------------------------------------------------
    def _check_trading(self, instrument_saving_dir, given_instrument_list):
        tis = self._get_trading_instrument_set(instrument_saving_dir)
        res = self._compare_2_src(given_instrument_list, tis)
        return res

    # -------------------------------------------------------------------------     
    def check_trading_beginning(self, email_subject_prefix, instrument_saving_dir, given_instrument_list):
        compare_res = self._check_trading(instrument_saving_dir, given_instrument_list)

        self._email.set_subjectPrefix(email_subject_prefix)
        if compare_res:
            self._email.send('%s has not started for trading.' % compare_res, '')
        else:
            self._email.send('trading has started.', '')
            
    # ------------------------------------------------------------------------- 
    def check_trading_ending(self, email_subject_prefix, instrument_saving_dir, given_instrument_list):
        compare_res = self._check_trading(instrument_saving_dir, given_instrument_list)

        self._email.set_subjectPrefix(email_subject_prefix)
        if compare_res:
            self._email.send('%s has not ended normally.' % compare_res, '')
        else:
            self._email.send('trading has ended.', '')
            
            
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    tim = TradingInstrumentMonitor()
    tim.email.set_receivers ( [cwhEmail])
    path = os.path.join('c:', os.sep, 'vnpy-1.9.2', 'examples', 'CtaTrading', 'pt_trading_futures')
    tim.checkTradingEnding('Probot-pt', path, ['rb2005', 'MA005'])
