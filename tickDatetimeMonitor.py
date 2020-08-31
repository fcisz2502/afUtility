from afUtility.mailing import Email
from datetime import datetime, timedelta
from dateutil import parser
import redis
import logging
import os
from time import sleep


# -----------------------------------------------------------------------------
class TickDatetimeMonitor(object):
    def __init__(self):
        self.instrumentList = list()
        pool = redis.ConnectionPool(host='127.0.0.1', port=6379, db=0, decode_responses=True)
        self.red = redis.StrictRedis(connection_pool=pool)
        self.email = Email()
        self.deviatesMinutes = 2

        self.logger = logging.getLogger(__name__)
        self.logger.setLevel(level = logging.INFO)

        fileHandler = logging.FileHandler(
            os.path.join(os.path.split(os.path.realpath(__file__))[0], "checkTickTime.log"))
        fileHandler.setLevel(logging.INFO)
        formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        fileHandler.setFormatter(formatter)
        self.logger.addHandler(fileHandler)

        console = logging.StreamHandler()
        console.setLevel(logging.INFO)
        self.logger.addHandler(console)

        self.logger.info("TickDatetimeMonitor initialization done.")

    # -------------------------------------------------------------------------
    def run(self): 
        for instrument in self.instrumentList:
            if abs(parser.parse(self.red.get(instrument+'TickTime')) - datetime.now()) > timedelta(minutes=self.deviatesMinutes):
                self.email.send("%s's tick datetime deviates from local time more than %s minutes." 
                    %(instrument, self.deviatesMinutes), str())
                self.logger.info("%s's tick datetime deviates from local time more than %s minutes." 
                    %(instrument, self.deviatesMinutes))
            else:
                pass


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    tdm = TickDatetimeMonitor()
    tdm.instrumentList = ['600276', 'rb2101', 'bu2012', 'm2101']
    while True:
        tdm.run()
        sleep(1)