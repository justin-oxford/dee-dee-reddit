def getsymbols():
    symbols = []
    filepath = 'symbols.txt'
    with open(filepath) as fp:
        line = fp.readline()
        cnt = 1
        while line:
            stocksymbol = line.strip().split('|', 1)[0]
            symbols.append(stocksymbol)
            line = fp.readline()
            cnt += 1
    return symbols
