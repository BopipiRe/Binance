def on_open(ws):
    ws.subscribe("futures.tickers", ['BTC_USDT'], False)  # 订阅BTC/USDT永续合约的ticker