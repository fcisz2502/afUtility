import os
from afUtility.mailing import Email
from keyInfo import cwhEmail
from copy import deepcopy


# -----------------------------------------------------------------------------
class TradingInstrumentMonitor(object):
    def __init__(self):
        self.email = Email()
    
    # -------------------------------------------------------------------------    
    def getTradingInstrumentList(self, assetPath):
        tradingInstrumentList = list()
        for doc in os.listdir(assetPath):
            f = open(os.path.join(assetPath, doc), 'r+')
            tradingInstrumentList.append(f.read())
            f.seek(0)
            f.truncate()
            f.close()
        return tradingInstrumentList
    
    # ------------------------------------------------------------------------- 
    def compareGivenInstrumentToTradingInstrument(self, givenInstrumentList, tradingInstrumentList):
        print('tradingInstrumentList is: ', tradingInstrumentList)
        print('givenInstrumentList is: ', givenInstrumentList)
        gim = deepcopy(givenInstrumentList)
        if len(tradingInstrumentList) > 0:
            try:
                for item in tradingInstrumentList:
                    if item != str():
                        gim.remove(item)
            except ValueError, e:
                self.email.send('ValueError in tradingInstrumentList, check it!', repr(e))
                print('ValueError in tradingInstrumentList: ', e)

        return gim
    
    # -------------------------------------------------------------------------     
    def checkTradingBeginning(self, emailSubjectPrefix, assetPath, givenInstrumentList):
        til = self.getTradingInstrumentList(assetPath)  # til = tradingInstrumentList
        compareRes = self.compareGivenInstrumentToTradingInstrument(givenInstrumentList, til)
        self.email.set_subjectPrefix(emailSubjectPrefix)
        
        if len(compareRes) > 0:
            self.email.send(str(compareRes) + ' has not started for trading', str())
        else:
            self.email.send('trading has started.', str())
            
    # ------------------------------------------------------------------------- 
    def checkTradingEnding(self, emailSubjectPrefix, assetPath, givenInstrumentList):    
        til = self. getTradingInstrumentList(assetPath)
        compareRes = self.compareGivenInstrumentToTradingInstrument(givenInstrumentList, til)
        self.email.set_subjectPrefix(emailSubjectPrefix)
        
        if len(compareRes) > 0:
            self.email.send(str(compareRes) + ' has not ended normally', str())
        else:
            self.email.send('trading has ended.', str())
            
            
# -----------------------------------------------------------------------------
if __name__ == '__main__':
    tim = TradingInstrumentMonitor()
    tim.email.set_receivers ( [cwhEmail])
    path = os.path.join('c:', os.sep, 'vnpy-1.9.2', 'examples', 'CtaTrading', 'pt_trading_futures')
    tim.checkTradingEnding('LN1-vnpy-Probot-pt', path, ['rb2005', 'MA005'])