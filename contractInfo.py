futureTickSize = {'bu':2, 'ru':5, 'rb':1, 'p':2, 'i':0.5, 'AP':1, 'm':1, 'RM':1, 'MA':1}
futureLotSize = {'bu':10, 'ru' :10, 'rb':10, 'p':10, 'i':100, 'AP':10, 'm':10, 'RM':10, 'MA':10}

def getSymbol(instrument):
    symbol = instrument[:2]
    try:
        if int(symbol[1])>=0:
            symbol = symbol[0]
    except ValueError:
        pass
    return symbol


# -----------------------------------------------------------------------------
if __name__ == "__main__":
    print(getSymbol("m2005"))