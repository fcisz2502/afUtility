import os
import redis
import http.client
from datetime import datetime, time, timedelta
from dateutil import parser
from afUtility.mailing import Email
from afUtility.keyInfo import cwhEmail


# -----------------------------------------------------------------------------
def get_baidu_datetime():
    baidu_dt = None
    try:
        conn = http.client.HTTPConnection('www.baidu.com')
        conn.request('get', '/')
        r = conn.getresponse()

        ts = r.getheader('date')

        try:
            baidu_dt = datetime.strptime(ts, '%a, %d %b %Y %H:%M:%S GMT')
        except ValueError:
            baidu_dt = datetime.strptime(ts, '%a, %d %b %Y %H:%M:%S GMT+0800 (CST)')

        baidu_dt += timedelta(hours=8)
    except:
        # don't use Exception as e or ,e, as don't know py2.7 or py 3.7 call this function
        baidu_dt = None

    return baidu_dt


# -----------------------------------------------------------------------------
class TradingInstrumentMonitor(object):
    '''
    for both stocks and futures
    monitor tick datetime deviate from local time
    monitor beginning and ending of instruments
    '''
    def __init__(self):
        pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        self._rcli = redis.StrictRedis(connection_pool=pool)
        self._email = Email()

    # -------------------------------------------------------------------------
    def set_prefix(self, x):
        self._email.set_subjectPrefix(x)

    # -------------------------------------------------------------------------    
    def _get_trading_instrument_set(self, saving_dir):
        # method used in future and stock trading,
        # so use instrument instead of futures or stocks
        trading_instrument_set = set()
        for root, dirs, files in os.walk(saving_dir):
            for file in files:
                # file: 603288.txt or bu2106.txt, or m2105.txt
                if len(file) <= 10 and 'txt' == file[-3:]:
                    f = open(os.path.join(root, file), 'r+')
                    instrument = f.read()
                    if instrument:
                        trading_instrument_set.add(instrument)
                    f.seek(0)
                    f.truncate()
                    f.close()
        return trading_instrument_set
    
    # ------------------------------------------------------------------------- 
    # def _compare_2_src(self, given_instrument_list, trading_instrument_set):
        # print('trading_instrument_set is: ', trading_instrument_set)
        # print('given_instrument_list is: ', given_instrument_list)
        
        # gil = given_instrument_list[:]

        # for instrument in trading_instrument_set:
        #     if len(instrument) and not instrument.isspace():
        #         try:
        #             gil.remove(instrument)
        #         except Exception, e:
        #             self._email.send('ValueError in trading_instrument_list, check it!', repr(e))

        # return set(given_instrument_list) - trading_instrument_set - set([''])
    
    # -------------------------------------------------------------------------
    # def _check_trading(self, instrument_saving_dir, given_instrument_list):
    #     # tis = self._get_trading_instrument_set(instrument_saving_dir)
    #     # res = self._compare_2_src(given_instrument_list, tis)
    #     # return res
    #
    #     return set(given_instrument_list) - self._get_trading_instrument_set(instrument_saving_dir) - set([''])

    # -------------------------------------------------------------------------     
    def check_trading_beginning(self, instrument_saving_dir, given_instrument_list):
        # compare_res = self._check_trading(instrument_saving_dir, given_instrument_list)
        # self._email.set_subjectPrefix(email_subject_prefix)

        compare_res = set(given_instrument_list) - self._get_trading_instrument_set(instrument_saving_dir) - set([''])
        if compare_res:
            self._email.send('%s has not started for trading.' % compare_res, '')
        else:
            self._email.send('trading has started.', '')

        return True
            
    # ------------------------------------------------------------------------- 
    def check_trading_ending(self, instrument_saving_dir, given_instrument_list):
        # compare_res = self._check_trading(instrument_saving_dir, given_instrument_list)
        # self._email.set_subjectPrefix(email_subject_prefix)

        compare_res = set(given_instrument_list) - self._get_trading_instrument_set(instrument_saving_dir) - set([''])
        if compare_res:
            self._email.send('%s has not ended normally.' % compare_res, '')
        else:
            self._email.send('trading has ended.', '')

        return True

    # -------------------------------------------------------------------------
    def check_tick_datetime(self, instruments):
        for instrument in instruments:
            itt = self._rcli.get('tick_time', instrument)
            if itt is not None:
                if abs(parser.parse(itt) - datetime.now()) > timedelta(minutes=2):
                    b_dt = get_baidu_datetime()
                    if b_dt is not None:
                        if abs(parser.parse(itt) - b_dt) > timedelta(minutes=2):
                            self._email.send(
                                "%s's tick time deviates from " \
                                    "local time > 2 mins." % instrument, 
                                "Deviates from baidu'time > 2 mins too."
                            )
                    else:
                        self._email.send(
                            "%s's tick time deviates from " \
                                "local time > 2 mins." % instrument, 
                            'Get None from baidu.'
                        )
            else:
                self._email.send(
                    "Get None for %s from redis while checking \
                        it's tick time." % instrument, 
                    ''
                )

            
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    pass
