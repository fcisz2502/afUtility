import os
import pandas as pd


# -----------------------------------------------------------------------------
def moveCtaTradingData(instrumentList):
    for instrument in instrumentList:
        symbol = instrument[:2]
        try:
            if int(symbol[-1]) >= 0:
                symbol = symbol[0]
        except ValueError:
            pass
        
        with open (os.path.join("c:", os.sep, "vnpy-1.9.2", "examples", 
                                "CtaTrading_RBT_"+symbol, 
                                instrument+"_positionsData.txt"), "r") as f:
            position = f.read()
        
        with open(os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "cwh", 
                               "ctaTradingData", instrument+"_positionsData.txt"), "w") as f:
            f.write(position)
        
        tradingBars = pd.read_csv(os.path.join("c:", os.sep, "vnpy-1.9.2", "examples",
                                               "CtaTrading_RBT_"+symbol, 
                                               instrument+"_df_60m.csv"))
        
        tradingBars.to_csv(os.path.join(os.sep*2, "FCIDEBIAN", "FCI_Cloud", "cwh", 
                                        "ctaTradingData", instrument+"_df_60m.csv"), index=0)



if __name__ == "__main__":
    instrumentList = ['rb2010', 'bu2006']
    moveCtaTradingData(instrumentList)
